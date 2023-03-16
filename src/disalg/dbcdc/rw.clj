(ns disalg.dbcdc.rw
  "Test for transactional read-write register write"
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
             [string :as str]]
            [dom-top.core :refer [with-retry]]
            [elle.core :as elle]
            [jepsen [checker :as checker]
             [client :as client]
             [generator :as gen]
             [util :as util]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.tests.cycle.append :as append]
            [jepsen.tests.cycle.wr :as wr]
            [disalg.dbcdc [client :as c]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]
            [disalg.dbcdc.client :as c]))

(def default-table-count 1)

(defn table-name
  "Takes an integer and constructs a table name."
  [i]
  (str "txn" i))

(defn table-for
  "What table should we use for the given key?"
  [table-count k]
  (table-name (mod (hash k) table-count)))

(defn mop!
  "Executes a transactional micro-op on a connection. 
   Returns the completed micro-op."
  [conn test [f k v]]
  (let [table-count (:table-count test default-table-count)
        table (table-for table-count k)]
    (Thread/sleep (rand-int 10))
    [f k (case f
           :r (c/read conn table k test)
           :w (let [_ (c/write conn table k v test)]
                v))]))

; initialized? is an atom which we set when we first use the connection--we set
; up initial isolation levels, logging info, etc. This has to be stateful
; because we don't necessarily know what process is going to use the connection
; at open! time.
(defrecord Client [node conn initialized?]
  client/Client
  (open! [this test node]
    (let [c (c/open test node)]
      (assoc this
             :node          node
             :conn          c
             :initialized?  (atom false))))

  (setup! [_ test] ;; TODO: 这里需要确认一下 setup！函数会被调用几次
    (dotimes [i (:table-count test default-table-count)]
      ; OK, so first worrying thing: why can this throw duplicate key errors if
      ; it's executed with "if not exists"?
      (with-retry [conn  conn
                   tries 10]
        (c/create-table conn (table-name i) test)
        (catch org.postgresql.util.PSQLException e
          (condp re-find (.getMessage e)
            #"duplicate key value violates unique constraint"
            :dup

            #"An I/O error occurred|connection has been closed"
            (do (when (zero? tries)
                  (throw e))
                (info "Retrying IO error")
                (Thread/sleep 1000)
                (c/close! conn)
                (retry (c/await-open node)
                       (dec tries)))

            (throw e))))))

  (invoke! [_ test op]
    ; One-time connection setup
    (when (compare-and-set! initialized? false true)
      (when (= (:database test) :postgresql)
        (j/execute! conn [(str "set application_name = 'jepsen process "
                               (:process op) "'")]))
      (c/set-transaction-isolation! conn (:isolation test)))

    (c/with-errors op
      (let [isolation (c/isolation-mapping (:isolation test) test)
            txn       (:value op)
            txn'      (j/with-transaction [t conn
                                           {:isolation isolation}]
                        (mapv (partial mop! t test) txn))]
        (assoc op :type :ok, :value txn'))))

  (teardown! [_ test])

  (close! [this test]
    (c/close! conn)))

(defn rw-test
  [opts]
  {:generator (wr/gen opts)
   :checker   (wr/checker opts)})

(defn workload
  "A read-write register write workload."
  [opts]
  (-> (rw-test (assoc (select-keys opts [:key-count
                                         :max-txn-length
                                         :max-writes-per-key])
                      :min-txn-length 1
                      :consistency-models [(:expected-consistency-model opts)]))
      (assoc :client (Client. nil nil nil))))
