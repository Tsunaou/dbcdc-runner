(ns disalg.dbcdc.impls.mysql
  "Helper functions for interacting with MySQL clients."
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
             [oracle :as oracle]]
            [jepsen.db :as db])
  (:import (java.sql Connection)))

(defn set-transaction-isolation!
  "Sets the transaction isolation level on a connection. Returns conn."
  [conn level]
  (.setTransactionIsolation
   conn
   (case level
     :serializable        Connection/TRANSACTION_SERIALIZABLE
     :snapshot-isolation  Connection/TRANSACTION_REPEATABLE_READ
     :repeatable-read     Connection/TRANSACTION_REPEATABLE_READ
     :read-committed      Connection/TRANSACTION_READ_COMMITTED
     :read-uncommitted    Connection/TRANSACTION_READ_UNCOMMITTED))
  conn)

(defn isolation-mapping
  [level]
  (case level
    :serializable       :serializable
    :snapshot-isolation :repeatable-read
    :repeatable-read    :repeatable-read
    :read-committed     :read-committed
    :read-uncommitted   :read-uncommitted))

(defn create-table
  [^Connection conn table]
  (let [drop-stmt (str "DROP TABLE IF EXISTS " table)
        create-stmt (str "CREATE TABLE IF NOT EXISTS " table
                         " (k INT UNIQUE NOT NULL PRIMARY KEY, v INT NOT NULL)")]
    (j/execute-one! conn [drop-stmt])
    (j/execute-one! conn [create-stmt])))

(defn read
  [^Connection conn table key]
  (let [res (j/execute-one! conn
                            [(str "SELECT v FROM " table " WHERE k = " key)]
                            {:builder-fn rs/as-unqualified-lower-maps})]
    ;; (info "mysql-read" res)
    (when-let [v (:v res)]
      (if (string? v)
        (long (Long/parseLong v))
        (long v)))))

(defn insert!
  "Performs an initial insert of a key with initial element e. Catches
  duplicate key exceptions, returning true if succeeded. If the insert fails
  due to a duplicate key, it'll break the rest of the transaction, assuming
  we're in a transaction, so we establish a savepoint before inserting and roll
  back to it on failure."
  [conn test table k v]
  (try
    (j/execute! conn ["SAVEPOINT upsert"])
    (info :insert (j/execute! conn
                              [(str "INSERT INTO " table " (k, v)"
                                    " VALUES (?, ?)")
                               k v]))
    (j/execute! conn ["RELEASE SAVEPOINT upsert"])
    true
    (catch Exception e
      (if (re-find #"duplicate key value" (.getMessage e))
        (do (info "insert failed: " (.getMessage e))
            (j/execute! conn ["ROLLBACK TO SAVEPOINT upsert"])
            false)
        (throw e)))))

(defn update!
  "Performs an update of a key k, adding element e. Returns true if the update
  succeeded, false otherwise."
  [conn test table k v]
  (let [res (-> conn
                (j/execute-one! [(str "UPDATE " table " SET v = ?"
                                      " WHERE k = ?") v k]))]
    (info :update res)
    (-> res
        :next.jdbc/update-count
        pos?)))

;; savepoint 机制在 TiDB v6.2 后才支持
(defn write-savepoint
  [^Connection conn table key value]
  (or (update! conn test table key value)
                   ; No dice, fall back to an insert
      (insert! conn test table key value)
                   ; OK if THAT failed then we probably raced with another
                   ; insert; let's try updating again.
      (update! conn test table key value)
                   ; And if THAT failed, all bets are off. This happens even
                   ; under SERIALIZABLE, but I don't think it technically
                   ; VIOLATES serializability.
      (throw+ {:type     ::homebrew-upsert-failed
               :key      key
               :element  value})))

(defn write
  [^Connection conn table key value]
  (let [res (j/execute-one!
             conn
             [(str "INSERT INTO " table
                   " (k, v) VALUES (" key ", " value ")"
                   " ON DUPLICATE KEY UPDATE "
                   " v = " value)])]
    ;; (info "mysql-write" res)
    (when-not (pos? (res :next.jdbc/update-count))
      (throw+ {:type ::write-but-not-take-effect
               :key  key
               :val  value}))))