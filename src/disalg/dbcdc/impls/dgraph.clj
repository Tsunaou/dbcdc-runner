(ns disalg.dbcdc.impls.dgraph
  (:require 
   [cheshire.core :as json]
   [clj-http.client :as http]
   [clojure.string :as str]))

(def url "http://175.27.241.31:8080/mutate?commitNow=true")
(def headers {"Content-Type" "application/json"})

(def mock-txn [[:r 8 nil] [:w 5 1] [:w 8 1] [:w 9 1] [:r 9 nil] [:w 9 3]])

(defn send-request [req-data]
  (http/post url {:headers headers :body (json/generate-string req-data)}))

(defn read?
  [[f _ _]]
  (= f :r))

(defn write?
  [[f _ _]]
  (= f :w))

(defn gen-query
  [reads]
  (format "query {%s}"
          (str/join (map-indexed
                     (fn [idx [f k v]]
                       (format "get%d(func: uid(%d)) { value } " (inc idx) k))
                     reads))))

(defn gen-mutations
  [writes]
  (if (empty? writes)
    [{:set {"fake_data" 233}}]
    (mapv (fn [[f k v]]
            {:set {"uid" k "value" v}}) writes)))

(defn gen-txn-req
  [reads writes]
  {:query (gen-query reads)
   :mutations (gen-mutations writes)})

(defn fetch-response
  [response]
  (let [resp-data (json/parse-string (:body response) true)
        code      (-> resp-data :data :code)]
    (when (= code "Success")
      (let [queries (-> resp-data :data :queries)
            values  (mapv (fn [[_ v]] (-> v first :value)) queries)
            ts-info (-> resp-data :extensions :txn)]
        {:values values :ts {:rts (:start_ts ts-info) :cts (:commit_ts ts-info)}}))))

(defn merge-reads
  [reads values]
  (map (fn [[f k _] v]
         [f k v])
       reads
       values))

(defn run-txn
  [txn]
  (let [reads  (filterv read? txn)
        writes (filterv write? txn) 
        response (send-request (gen-txn-req reads writes)) 
        res (fetch-response response)]
    (when res
      (let [values (:values res)
            ts     (:ts res)]
        {:txn (vec (concat (merge-reads reads values) writes))
         :ts ts}))))
 