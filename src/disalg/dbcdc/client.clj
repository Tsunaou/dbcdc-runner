(ns disalg.dbcdc.client
  "Helper functions for interacting with PostgreSQL clients."
  (:require [clojure.tools.logging :refer [info warn]]
            [dom-top.core :refer [with-retry]]
            [jepsen [client :as client]
             [util :as util]]
            [next.jdbc :as j]
            [next.jdbc.result-set :as rs]
            [next.jdbc.sql.builder :as sqlb]
            [slingshot.slingshot :refer [try+ throw+]]
            [wall.hack :as wh]
            [disalg.dbcdc.utils.cfg :refer [read-spec]]
            [disalg.dbcdc.impls
             [postgresql :as pg]
             [mysql :as mysql]
             [oracle :as oracle]
             [dgraph :as dgraph]
             [mongo :as mongo]]
            [jepsen.db :as db]
            [disalg.dbcdc.impls.postgresql :as pg]
            [disalg.dbcdc.impls.dgraph :as dgraph]
            [disalg.dbcdc.impls.mongo :as mongo])
  (:import (java.sql Connection)))

(defn get-spec
  [test]
  (let [spec-map  (read-spec)
        database  (:database test)]
    (spec-map database)))

(def relation-databases
  #{:mysql :postgresql :tidb})

(defn relation-db?
  [database]
  (contains? relation-databases database))

;; For open the connection to database
(defn open-nosql
  [spec database]
  (case database
    :dgraph (dgraph/open-draph-v2 spec) ;; return a url for http operations
    :mongodb (mongo/open spec)))

(defn open-relational
  [spec]
  (let [ds    (j/get-datasource spec)
        conn  (j/get-connection ds)]
    conn))

(defn open
  "Opens a connection to the given node."
  [test node]
  (let [spec-map  (read-spec)
        database  (:database test)
        spec      (spec-map database)]
    (if (relation-db? database)
      (open-relational spec)
      (open-nosql spec database))))

;; For isolation setting
(defn isolation-mapping
  [isolation test]
  (let [db (:database test)]
    (case db
      :postgresql (pg/isolation-mapping isolation)
      :mysql      (mysql/isolation-mapping isolation)
      :tidb       (mysql/isolation-mapping isolation)
      :dgraph     :snapshot-isolation ;; only support SI
      :mongodb    :snapshot-isolation ;; only support SI
      )))

(defn set-transaction-isolation!
  "Sets the transaction isolation level on a connection. Returns conn."
  [^Connection conn level test]
  (let [db (:database test)
        res (case db
              :postgresql (pg/set-transaction-isolation! conn level)
              :mysql      (pg/set-transaction-isolation! conn level)
              :tidb       (mysql/set-transaction-isolation! conn level)
              :dgraph     nil ;; not need for dgraph to set isolation level
              :mongodb    nil ;; not need for mongodb to set isolation level
              )]))

;; For create table to hold testing data and operations
(defn create-table
  [conn table test]
  (let [db (:database test)
        res (case db
              :postgresql (if (:varchar-table test)
                            (pg/create-varchar-table conn table)
                            (pg/create-table conn table))
              :mysql      (mysql/create-table conn table)
              :tidb       (do
                            (when (= :opt (:tidb-mode test))
                              (j/execute-one! conn ["SET GLOBAL tidb_txn_mode = 'optimistic';"])) ;; 开启乐观事务
                            (mysql/create-table conn table))
              :dgraph     (let [spec       (get-spec test)
                                session-id conn]
                            (dgraph/create-table-v2 session-id spec))
              :mongodb    (let [spec       (get-spec test)]
                            (mongo/create-table (:conn conn) spec)))]))

(defn read
  [^Connection conn table key test]
  (let [db (:database test)
        res (case db
              :postgresql (if (:varchar-table test)
                            (pg/read-varchar conn table (str key))
                            (pg/read conn table key))
              :mysql      (mysql/read conn table key)
              :tidb       (mysql/read conn table key))]
    res))

(defn write
  [^Connection conn table key value test]
  (let [db (:database test)
        res (case db
              :postgresql (if (:varchar-table test)
                            (pg/write-varchar-savepoint conn table (str key) (str value))
                            (pg/write conn table key value))
              :mysql      (mysql/write conn table key value)
              :tidb       (mysql/write conn table key value))]
    res))


;; For close the connection to database
(defn close-nosql
  [test conn database]
  (case database
    :dgraph (let [spec       (get-spec test)
                  session-id conn]
              (dgraph/close-session-v2 session-id spec))
    :mongodb nil))

(defn close-relational
  "Closes a connection."
  [^java.sql.Connection conn]
  (.close conn))

(defn close!
  "Closes a connection."
  [test conn]
  (let [database (:database test)]
    (if (relation-db? database)
      (close-relational conn)
      (close-nosql test conn database))))

;; Handle error or other situations
;; TODO:这里目前只有 PG 系列的数据库会调用到这个分支，秉持能用就行的原则，暂时不动
(defn await-open
  "Waits for a connection to node to become available, returning conn. Helpful
  for starting up."
  [test node]
  (with-retry [tries 100]
    (info "Waiting for" node "to come online...")
    (let [conn (open test node)]
      (try (j/execute-one! conn
                           ["create table if not exists jepsen_await ()"])
           conn
           (catch org.postgresql.util.PSQLException e
             (condp re-find (.getMessage e)
               ; Ah, good, someone else already created the table
               #"duplicate key value violates unique constraint \"pg_type_typname_nsp_index\""
               conn

               (throw e)))))
    (catch org.postgresql.util.PSQLException e
      (when (zero? tries)
        (throw e))

      (Thread/sleep 5000)
      (condp re-find (.getMessage e)
        #"connection attempt failed"
        (retry (dec tries))

        #"Connection to .+ refused"
        (retry (dec tries))

        #"An I/O error occurred"
        (retry (dec tries))

        (throw e)))))

(defmacro with-errors
  "Takes an operation and a body; evals body, turning known errors into :fail
  or :info ops."
  [op & body]
  `(try ~@body
        (catch clojure.lang.ExceptionInfo e#
          (warn e# "Caught ex-info")
          (assoc ~op :type :info, :error [:ex-info (.getMessage e#)]))

        (catch org.postgresql.util.PSQLException e#
          (condp re-find (.getMessage e#)
            #"ERROR: could not serialize access"
            (assoc ~op :type :fail, :error [:could-not-serialize (.getMessage e#)])

            #"ERROR: deadlock detected"
            (assoc ~op :type :fail, :error [:deadlock (.getMessage e#)])

            #"An I/O error occurred"
            (assoc ~op :type :fail, :error :io-error)

            #"connection has been closed"
            (assoc ~op :type :fail, :error :connection-has-been-closed)

            #"ERROR: Operation expired"
            (assoc ~op :type :fail, :error :opeartion-expired)

            #"ERROR: Operation failed"
            (assoc ~op :type :fail, :error :opeartion-failed)

            #"ERROR: Query error"
            (assoc ~op :type :fail, :error :query-error)

            #"ERROR: duplicate key value"
            (assoc ~op :type :fail, :error :duplicate-key-value)

            #"expired or aborted by a conflict"
            (assoc ~op :type :fail, :error :txn-conflict)

            #"ERROR: Transaction aborted:"
            (assoc ~op :type :fail, :error :txn-aborted)

            #"ERROR: Unknown transaction, could be recently aborted"
            (assoc ~op :type :fail, :error :unknown-txn)

            #"conflicts with higher priority transaction"
            (assoc ~op :type :fail, :error :conflict-with-higher-priority)

            #"ERROR: Restart read required at:"
            (assoc ~op :type :fail, :error :restart-read-required)

            #"ERROR: Value write after transaction start:"
            (assoc ~op :type :fail, :error :write-after-start)

            #"conflicts with committed transaction"
            (assoc ~op :type :fail, :error :txn-conflict)
            #"错误: 由于同步更新而无法串行访问"
            (assoc ~op :type :fail, :error :no-serial-access)

            #"错误: 检测到死锁"
            (assoc ~op :type :fail, :error :deadlock-detected)

            (throw e#)))

        (catch com.mysql.cj.jdbc.exceptions.MySQLTransactionRollbackException e#
          (condp re-find (.getMessage e#)
            #"Deadlock found when trying to get lock;"
            (assoc ~op :type :fail, :error [:deadlock (.getMessage e#)])

            (throw e#)))

        (catch java.sql.SQLException e#
          (condp re-find (.getMessage e#)
            #"Write conflict, txnStartTS="
            (assoc ~op :type :fail, :error [:write-conflict (.getMessage e#)])

            (throw e#)))

        (catch com.mongodb.MongoCommandException e#
          (condp re-find (.getMessage e#)
            #"WriteConflict"
            (assoc ~op :type :fail, :error [:write-conflict (.getMessage e#)])

            (throw e#)))))
