(ns disalg.dbcdc.impls.dgraph
  (:require
   [clojure.tools.logging :refer [info warn]]
   [cheshire.core :as json]
   [clj-http.client :as http]
   [clojure.string :as str]))


(defn open-draph
  "获得到 Draph 的链接, 由于目前是无状态的 HTTP, 暂时生成 URL 代替"
  [spec]
  (let [host (:host spec)
        port (:port spec)
        url  (format "http://%s:%s/mutate?commitNow=true" host port)
        _    (info "Generate the url for dgraph of" url)]
    url))

(def headers {"Content-Type" "application/json"})

(def url-for-test "http://175.27.241.31:8080/mutate?commitNow=true")
(def txn-for-test [[:r 8 nil] [:w 5 1] [:w 8 1] [:w 9 1] [:r 9 nil] [:w 9 3]])

(defn send-request
  ([url req-data]
   (http/post url {:headers headers :body (json/generate-string req-data)}))
  ([req-data]
   (send-request url-for-test req-data)))

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
    (if (= code "Success")
      (let [queries (-> resp-data :data :queries)
            values  (mapv (fn [[_ v]] (-> v first :value)) queries)
            ts-info (-> resp-data :extensions :txn)]
        {:values values :ts {:rts (:start_ts ts-info) :cts (:commit_ts ts-info)}})
      (warn resp-data))))

(defn merge-reads
  [reads values]
  (map (fn [[f k _] v]
         [f (long k) (if (nil? v) v (long v))])
       reads
       values))

(defn run-txn
  ([url txn]
   (let [reads  (filterv read? txn)
         writes (filterv write? txn)
         response (send-request url (gen-txn-req reads writes))
         res (fetch-response response)
         _   (info "res is " res)]
     (if-not (nil? res)
       (let [values (:values res)
             ts     (:ts res)]
         {:type :ok
          :value (vec (concat (merge-reads reads values) writes))
          :ts ts})
       {:type :fail :error [:unknown res]})))
  ([txn]
   (run-txn url-for-test txn)))

(defn execute-txn
  [url txn]
  (run-txn url txn))