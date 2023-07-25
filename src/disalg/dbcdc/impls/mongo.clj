(ns disalg.dbcdc.impls.mongo
  (:require [clojure.walk :as walk]
            [clojure.tools.logging :refer [info warn]]
            [jepsen [util :as util :refer [timeout]]]
            [slingshot.slingshot :refer [try+ throw+]])
  (:import (java.util ArrayList
                      List)
           (java.util.concurrent TimeUnit)
           (com.mongodb Block
                        ConnectionString
                        MongoClientSettings
                        TransactionOptions
                        MongoClientSettings$Builder
                        ServerAddress
                        WriteConcern
                        ReadConcern
                        ReadPreference)
           (com.mongodb.client MongoClient
                               MongoClients
                               MongoCollection
                               MongoDatabase
                               TransactionBody)
           (com.mongodb.client.model Filters
                                     FindOneAndUpdateOptions
                                     ReplaceOptions
                                     ReturnDocument
                                     Sorts
                                     Updates
                                     UpdateOptions
                                     Indexes
                                     IndexOptions)
           (com.mongodb.client.result UpdateResult)
           (com.mongodb.session ClientSession)
           (org.bson Document)))


(defn sharded-cluster?
  [test]
  (= (:deployment test) :sharded-cluster))

(defn replica-set?
  [test]
  (= (:deployment test) :replica-set))

(def mongos-port 27017)
(defn shard-port
  [test]
  (if (replica-set? test)
    27017
    27018))

(defn driver-conn-port
  [test]
  (if (replica-set? test)
    (shard-port test)
    mongos-port))

(def config-port 27019)

;; Basic node manipulation
(defn addr->node
  "Takes a node address like n1:27017 and returns just n1"
  [addr]
  ((re-find #"(.+):\d+" addr) 1))

(defmacro with-block
  "Wrapper for the functional mongo Block interface"
  [x & body]
  `(reify Block
     (apply [_ ~x]
       ~@body)))

;; 获得连接 @used
(defn open
  "Opens a connection to a node.
   {:conn conn
    :session session}"
  [spec]
  (let [node   (:host spec)
        port   (:port spec)
        _      (info "node" node "port" port "spec" spec)
        conn   (MongoClients/create
                (.. (MongoClientSettings/builder)
                    (applyToClusterSettings (with-block builder
                                              (.. builder
                                                  (hosts [(ServerAddress. node port)])
                                                  (serverSelectionTimeout 1 TimeUnit/SECONDS))))
                    (applyToSocketSettings (with-block builder
                                             (.. builder
                                                 (connectTimeout 5 TimeUnit/SECONDS)
                                                 (readTimeout    5 TimeUnit/SECONDS))))
                    (applyToConnectionPoolSettings (with-block builder
                                                     (.. builder
                                                         (minSize 1)
                                                         (maxSize 1)
                                                         (maxWaitTime 1 TimeUnit/SECONDS))))
                    (retryReads false)
                    build))
        session (.startSession conn)
        res     {:conn conn
                 :session session}
        _       (info "get connection to mongodb" res)]
        res))

(defn await-open
  "Blocks until (open node) succeeds. Helpful for initial cluster setup."
  [node port]
  (timeout 120000
           (throw+ {:type ::timed-out-awaiting-connection
                    :node node
                    :port port})
           (loop []
             (or (try
                   (let [conn (open node port)]
                     (try
                       (.first (.listDatabaseNames conn))
                       conn
                       ; Don't leak clients when they fail
                       (catch Throwable t
                         (.close conn)
                         (throw t))))
                   (catch com.mongodb.MongoTimeoutException e
                     (info "Mongo timeout while waiting for conn; retrying."
                           (.getMessage e))
                     nil)
                   (catch com.mongodb.MongoNodeIsRecoveringException e
                     (info "Node is recovering; retrying." (.getMessage e))
                     nil)
                   (catch com.mongodb.MongoSocketReadTimeoutException e
                     (info "Mongo socket read timeout waiting for conn; retrying")
                     nil))
                 ; If we aren't ready, sleep and retry
                 (do (Thread/sleep 1000)
                     (recur))))))

; Basic plumbing
(defprotocol ToDoc
  "Supports coercion to MongoDB BSON Documents."
  (->doc [x]))

(extend-protocol ToDoc
  nil
  (->doc [_] (Document.))

  clojure.lang.Keyword
  (->doc [x] (name x))

  clojure.lang.IPersistentMap
  (->doc [x]
    (->> x
         (map (fn [[k v]] [(name k) (->doc v)]))
         (into {})
         (Document.)))

  clojure.lang.Sequential
  (->doc [x]
    (ArrayList. (map ->doc x)))

  Object
  (->doc [x] x))

(defprotocol FromDoc
  "Supports coercion from MongoDB BSON Documents"
  (parse [x]))

(extend-protocol FromDoc
  nil
  (parse [x] nil)

  Document
  (parse [x]
    (persistent!
     (reduce (fn [m [k v]]
               (assoc! m (keyword k) (parse v)))
             (transient {})
             (.entrySet x))))

  UpdateResult
  (parse [r]
    {:matched-count  (.getMatchedCount r)
     :modified-count (.getModifiedCount r)
     :upserted-id    (.getUpsertedId r)
     :acknowledged?  (.wasAcknowledged r)})

  List
  (parse [x]
    (map parse x))

  Object
  (parse [x]
    x))

;; Write Concerns
(defn write-concern
  "Turns a named (e.g. :majority, \"majority\") into a WriteConcern."
  [wc]
  (when wc
    (case (name wc)
      "acknowledged"    WriteConcern/ACKNOWLEDGED
      "journaled"       WriteConcern/JOURNALED
      "majority"        WriteConcern/MAJORITY
      "unacknowledged"  WriteConcern/UNACKNOWLEDGED)))

(defn read-concern
  "Turns a named (e.g. :majority, \"majority\" into a ReadConcern."
  [rc]
  (when rc
    (case (name rc)
      "available"       ReadConcern/AVAILABLE
      "default"         ReadConcern/DEFAULT
      "linearizable"    ReadConcern/LINEARIZABLE
      "local"           ReadConcern/LOCAL
      "majority"        ReadConcern/MAJORITY
      "snapshot"        ReadConcern/SNAPSHOT)))

(defn transactionless-read-concern
  "Read concern SNAPSHOT isn't supported outside transactions; we weaken it to
  MAJORITY."
  [rc]
  (case rc
    "snapshot" "majority"
    rc))

;; Error handling
(defmacro with-errors
  "Remaps common errors; takes an operation and returns a :fail or :info op
  when a throw occurs in body."
  [op & body]
  `(try ~@body
        (catch com.mongodb.MongoNotPrimaryException e#
          (assoc ~op :type :fail, :error :not-primary))

        (catch com.mongodb.MongoNodeIsRecoveringException e#
          (assoc ~op :type :fail, :error :node-recovering))

        (catch com.mongodb.MongoSocketReadTimeoutException e#
          (assoc ~op :type :info, :error :socket-read-timeout))

        (catch com.mongodb.MongoTimeoutException e#
          (condp re-find (.getMessage e#)
            #"Timed out after \d+ ms while waiting to connect"
            (assoc ~op :type :fail, :error :connect-timeout)

            (assoc ~op :type :info, :error :mongo-timeout)

            (throw e#)))

        (catch com.mongodb.MongoExecutionTimeoutException e#
          (assoc ~op :type :info, :error :mongo-execution-timeout))

        (catch com.mongodb.MongoWriteException e#
          (condp re-find (.getMessage e#)
            #"Not primary so we cannot begin or continue a transaction"
            (assoc ~op :type :fail, :error :not-primary-cannot-txn)

         ; This LOOKS like it ought to be a definite failure, but it's not!
         ; Write transactions can throw this but actually succeed. I'm calling
         ; it info for now.
            #"Could not find host matching read preference"
            (assoc ~op :type :info, :error :no-host-matching-read-preference)

            (throw e#)))

        (catch com.mongodb.MongoCommandException e#
          (condp re-find (.getMessage e#)
         ; Huh, this is NOT, as it turns out, a determinate failure.
            #"TransactionCoordinatorSteppingDown"
            (assoc ~op :type :info, :error :transaction-coordinator-stepping-down)

         ; This can be the underlying cause of issues like "unable to
         ; initialize targeter for write op for collection..."
         ; These are ALSO apparently not... determinate failures?
            #"Connection refused"
            (assoc ~op :type :info, :error :connection-refused)

         ; Likewise
            #"Connection reset by peer"
            (assoc ~op :type :info, :error :connection-reset-by-peer)

            (throw e#)))

        (catch com.mongodb.MongoClientException e#
          (condp re-find (.getMessage e#)
         ; This... seems like a bug too
            #"Sessions are not supported by the MongoDB cluster to which this client is connected"
            (assoc ~op :type :fail, :error :sessions-not-supported-by-cluster)

            (throw e#)))

        (catch com.mongodb.MongoQueryException e#
          (condp re-find (.getMessage e#)
            #"Could not find host matching read preference"
            (assoc ~op :type :fail, :error :no-host-matching-read-preference)

            #"code 251" (assoc ~op :type :fail, :error :transaction-aborted)

         ; Why are there two ways to report this?
            #"code 10107 " (assoc ~op :type :fail, :error :not-primary-2)

            #"code 13436 " (assoc ~op :type :fail, :error :not-primary-or-recovering)
            (throw e#)))))

(defn ^MongoDatabase db
  "Get a DB from a connection."
  ([conn db-name]
   (.getDatabase conn db-name))
  ([conn db-name opts]
   (let [rc (read-concern "majority")
         wc (write-concern "majority")]
     (cond-> (db conn db-name)
       rc (.withReadConcern rc)
       wc (.withWriteConcern wc)))))

(defn ^MongoCollection collection
  "Gets a Mongo collection from a DB."
  [^MongoDatabase db collection-name]
  (.getCollection db collection-name))

(defn drop-database!
  [^MongoDatabase db collection-name]
  (let [coll (.getCollection db collection-name)
        ret  (.drop coll)
        _    (info "Drop collection" collection-name)])
  (.drop db))

(defn create-index!
  [^MongoDatabase db collection-name]
  (let [coll (.getCollection db collection-name)
        index-options (IndexOptions.)
        index-options (.unique index-options true)
        ret  (.createIndex coll (Indexes/ascending ["key"]) index-options)
        _    (info "Create index" collection-name)]))

;; Sessions

(defn start-session
  "Starts a new session"
  [conn]
  (.startSession conn))

(defn get-session-info
  "Gets the transaction information of session"
  [^ClientSession session]
  (let [server-session (.getServerSession session)
        txn-number (.getTransactionNumber server-session)
        uuid (-> server-session
                 .getIdentifier
                 (.get "id")
                 .asBinary
                 .asUuid
                 .toString)]
    {:txn-number txn-number :uuid uuid}))

;; Transactions

(defmacro txn-body
  "Converts body to a TransactionBody function."
  [& body]
  `(reify TransactionBody
     (execute [this]
       ~@body)))

;; Actual commands

(defn command!
  "Runs a command on the given db."
  [^MongoDatabase db cmd]
  (parse (.runCommand db (->doc cmd))))

(defn admin-command!
  "Runs a command on the admin database."
  [conn cmd]
  (command! (db conn "admin") cmd))

(defn find-one
  "Find a document by ID. If a session is provided, will use that session
  for a causally consistent read"
  ([coll id]
   (find-one coll nil id))
  ([^MongoCollection coll ^ClientSession session id]
   (let [filt (Filters/eq "key" id)]
     (-> (if session
           (.find coll session filt)
           (.find coll filt))
         .first
         parse))))

(defn upsert!
  "Ensures the existence of the given document, a map with at minimum an :key
  key."
  ([^MongoCollection coll doc]
   (upsert! nil coll doc))
  ([^ClientSession session ^MongoCollection coll doc]
   (assert (:key doc))
   (parse
    (if session
      (.replaceOne coll
                   session
                   (Filters/eq "key" (:key doc))
                   (->doc doc)
                   (.upsert (ReplaceOptions.) true))
      (.replaceOne coll
                   (Filters/eq "key" (:key doc))
                   (->doc doc)
                   (.upsert (ReplaceOptions.) true))))))


(defn create-table
  [conn spec]
  ; db.createCollection() can be omitted if sh.shardCollection() has been executed
  (let [dbname    (:dbname spec)
        collname  (:collname spec)
        db        (db conn dbname nil)
        _         (info "db" db)
        _         (drop-database! db collname)]
    (info "Collection" dbname "." collname " created")
    (admin-command!
     conn {:shardCollection  (str dbname "." collname)
           :key              {:key :hashed}
           :numInitialChunks 7})
    (info "Collection" dbname "." collname " sharded"))
  (let [dbname    (:dbname spec)
        collname  (:collname spec)
        db        (db conn dbname nil)
        _         (create-index! db collname)]))

(defn txn-options
  "Constructs options for this transaction."
  [test txn]
  ; Transactions retry for well over 100 seconds and I cannot for the life of
  ; me find what switch makes that timeout shorter. MaxCommitTime only affects
  ; a *single* invocation of the transaction, not the retries. We work around
  ; this by timing out in Jepsen as well.
  (cond-> (TransactionOptions/builder)
    true (.maxCommitTime 5 TimeUnit/SECONDS)
    true (.readConcern (read-concern "snapshot"))
    true (.writeConcern (write-concern "majority")) 
    true .build))

(defn apply-mop!
  "Applies a transactional micro-operation to a connection."
  [test db collname session [f k v :as mop]]
  (let [coll (collection db collname)]
    (case f
      :r (let [res (:value (find-one coll session k))
              ;;  _   (info "read res" res)
              ;;  _   (info "(nil? res)" (nil? res))
              ;;  _   (info "read:" mop)
               val (if (nil? res)
                     0
                     (long res))]
           [f k val])
      :w (let [filt (Filters/eq "key" k)
               doc  (->doc {:$set {:value v}})
               opts (.. (UpdateOptions.) (upsert true))
               res  (if session
                      (.updateOne coll session filt doc opts)
                      (.updateOne coll filt doc opts))]
           mop))))

(defn execute-txn
  [conn session txn spec test]
  ;; (info "execute-mongo-txn" 
  ;;       "conn:" conn
  ;;       "session" session
  ;;       "txn" txn
  ;;       "spec" spec)
  (let [dbname (:dbname spec)
        collname (:collname spec) 
        txn'   (let [db   (db conn dbname test)
                     opts (txn-options test txn)
                     body (txn-body (let [session-info (get-session-info session)
                                          res          (mapv (partial apply-mop! test db collname session) txn)]
                                 {:session-info session-info :value res}))]
                 (.withTransaction session body opts))]
    (merge {:type :ok} txn')))
