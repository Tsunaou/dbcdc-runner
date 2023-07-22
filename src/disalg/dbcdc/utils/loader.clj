(ns disalg.dbcdc.utils.loader
  (:require 
   [clojure.tools.logging :refer [info warn]]
   [clojure.data.json :as json]))

(defn event->op
  [event]
  (if (:write event)
    [:w (:variable event) (:value event)]
    [:r (:variable event) nil]))

(defn transaction->txn
  [transaction]
  (mapv event->op (:events transaction)))

(defn session->txns
  [session]
  (mapv transaction->txn session))

(defn convert
  [json-map]
  (let [params      (:params json-map)
        variables   (:n_variable params)
        concurrency (:n_node params)
        sessions        (:data json-map)]
    {:concurrency concurrency
     :variables   variables
     :txns        (mapv session->txns sessions)}))

(defn load-testcase
  "{
    :conncurrency: 并发数
    :variables: key 总数
    :txns: 每个 session 上对应的事务
   }"
  [path]
  (try
    (convert (json/read-str (slurp path) :key-fn keyword))
    (catch java.io.FileNotFoundException e
      (warn "can not find hist-*.json in" path)
      (throw e))))