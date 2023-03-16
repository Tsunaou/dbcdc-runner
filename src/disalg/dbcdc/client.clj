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
             [oracle :as oracle]]
            [jepsen.db :as db]
            [disalg.dbcdc.impls.postgresql :as pg])
  (:import (java.sql Connection)))

(defn open
  "Opens a connection to the given node."
  [test node]
  (let [spec-map  (read-spec)
        spec  (spec-map (:database test))
        ds    (j/get-datasource spec)
        conn  (j/get-connection ds)]
    conn))

(defn isolation-mapping
  [isolation test]
  (let [db (:database test)]
    (case db
      :postgresql (pg/isolation-mapping isolation)
      (str "isolation-mapping not be implemented for database " db))))

(defn set-transaction-isolation!
  "Sets the transaction isolation level on a connection. Returns conn."
  [^Connection conn level]
  (let [db (:database test)
        res (case db
              :postgresql (pg/set-transaction-isolation! conn level)
              (str "set-transaction-isolation! not be implemented for database " db))]))

(defn create-table
  [^Connection conn table test]
  (let [db (:database test)
        res (case db
              :postgresql (pg/create-table conn table)
              (str "create-table not be implemented for database " db))]))

(defn read
  [^Connection conn table key test]
  (let [db (:database test)
        res (case db
              :postgresql (pg/read conn table key)
              (str "read not be implemented for database " db))]
    res))

(defn write
  [^Connection conn table key value test]
  (let [db (:database test)
        res (case db
              :postgresql (pg/write conn table key value)
              (str "write not be implemented for database " db))]
    res))


(defn close!
  "Closes a connection."
  [^java.sql.Connection conn]
  (.close conn))

(defn await-open
  "Waits for a connection to node to become available, returning conn. Helpful
  for starting up."
  [node]
  (with-retry [tries 100]
    (info "Waiting for" node "to come online...")
    (let [conn (open node)]
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
            (assoc ~op :type :info, :error :io-error)

            #"connection has been closed"
            (assoc ~op :type :info, :error :connection-has-been-closed)

            (throw e#)))))
