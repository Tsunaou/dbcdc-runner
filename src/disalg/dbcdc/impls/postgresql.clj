(ns disalg.dbcdc.impls.postgresql
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
    :read-committed     :read-committed
    :read-uncommitted   :read-uncommitted))

(defn create-table
  [^Connection conn table]
  (let [drop-stmt (str "drop table if exists " table)
        create-stmt (str "create table if not exists " table
                         " (key int not null primary key, val int not null)")]
    (j/execute-one! conn [drop-stmt])
    (j/execute-one! conn [create-stmt])))

(defn read
  [^Connection conn table key]
  (let [res (j/execute-one! conn
                            [(str "select val from " table " where key =  ?") key]
                            {:builder-fn rs/as-unqualified-lower-maps})]
    (info "pg-read" res)
    (when-let [v (:val res)]
      (long v))))

(defn write
  [^Connection conn table key value]
  (let [res (j/execute-one!
             conn
             [(str "insert into " table " as t"
                   " (key, val) values (?, ?)"
                   " on conflict (key) do update set"
                   " val = ? where t.key = ?")
              key value value key])]
    (info "pg-write" res)
    (when-not (pos? (res :next.jdbc/update-count))
      (throw+ {:type ::write-but-not-take-effect
               :key  key
               :val  value}))))
