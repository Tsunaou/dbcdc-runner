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

;; -- Test for Expected some name. Got: lex.Item [7] --
(def txn-for-expected-some-name
  [[:w 58 6] [:w 458 1] [:w 1 12] [:w 581 1] [:w 160 3]])

(defn send-request
  ([url req-data]
   (info "send request for" url req-data)
   (http/post url {:headers headers :body (json/generate-string req-data)}))
  ([req-data]
   (send-request url-for-test req-data)))

(defn drop-dgraph-table
  [spec]
  (let [{:keys [host port]} spec
        url  (format "http://%s:%s/alter" host port)
        cmd  {:drop_all true}
        ret  (send-request url cmd)
        {:keys [res stats]} ret]
    (info url cmd ret)))

(defn create-table
  [spec]
  (drop-dgraph-table spec))

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
                       (format "get%d(func: uid(%d)) { value } " (inc idx) (inc k)))
                     reads))))

(defn gen-mutations
  [writes]
  (if (empty? writes)
    [{:set {"fake_data" 233}}]
    (mapv (fn [[f k v]]
            {:set {"uid" (inc k) "value" v}}) writes)))

(defn gen-txn-req
  [reads writes] 
  (cond
    (empty? reads)  {:mutations (gen-mutations writes)}
    (empty? writes) {:query (gen-query reads)}
    :else {:query (gen-query reads)
           :mutations (gen-mutations writes)}))

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
         res (fetch-response response)]
     (if-not (nil? res)
       (let [values (:values res)
             ts     (:ts res)]
         {:type :ok
          :value (vec (concat (merge-reads reads values) writes))
          :ts ts})
       (throw (Exception. (str res))))))
  ([txn]
   (run-txn url-for-test txn)))

(defn execute-txn
  [url txn]
  (run-txn url txn))

;; 下面的暂时用于测试
(defn gen-url-for-create-session
  [host port]
  (format "http://%s:%s/create_session" host port))

(defn create-session-v2
  [host port]
  (let [url        (gen-url-for-create-session host port)
        response   (http/post url {:headers headers})
        body       (json/parse-string (:body response))
        result     (get body "result")]
    (if (= result "Success")
      (let [session-id  (get body "session_id")]
        session-id)
      (throw (Exception. "创建会话失败")))))

(defn open-draph-v2
  "获得到 Draph 的链接, 使用 python driver 的 http 代理，返回"
  [spec]
  (let [host       (:host spec)
        port       (:port spec)
        session-id (create-session-v2 host port)]
    session-id))

(defn gen-url-for-release-connection
  [host port]
  (format "http://%s:%s/release_connection" host port))


(defn gen-data-for-release-connection
  [session-id]
  (json/generate-string
   {:session_id session-id}))


(defn release-connection-v2
  [host port session-id]
  (let [url        (gen-url-for-create-session host port)
        data      (gen-data-for-release-connection session-id)
        response  (http/post url {:headers headers :body data})
        body      (json/parse-string (:body response))
        result (get body "result")]
    (when-not (= result "Success")
      (throw (Exception. "Dgraph release connections failed")))))

(defn close-session-v2
  [session-id spec]
  (let [host (:host spec)
        port (:port spec)
        res  (release-connection-v2 host port session-id)]
    res))

(defn gen-url-for-drop-all
  [host port]
  (format "http://%s:%s/drop_all" host port))


(defn gen-data-for-drop-all
  [session-id]
  (json/generate-string
   {:session_id session-id}))


(defn drop-all-v2
  [host port session-id]
  (let [url        (gen-url-for-drop-all host port)
        data      (gen-data-for-drop-all session-id)
        response  (http/post url {:headers headers :body data})
        body      (json/parse-string (:body response))
        result (get body "result")]
    (when-not (= result "Success")
      (throw (Exception. "Dgraph drop all tables failed")))))

(defn create-table-v2
  [session-id spec]
  (let [host (:host spec)
        port (:port spec)
        res  (drop-all-v2 host port session-id)]
   res))

(defn gen-url-for-commit-transaction
  [host port]
  (format "http://%s:%s/commit_transaction" host port))

(defn gen-data-for-commit-transaction
  [session-id txn]
  (json/generate-string
   {:session_id session-id
    :ops (mapv
          (fn [[f k v]]
            (case f
              :w [k v]
              :r [k]))
          txn)}))

(defn commit-transaction-v2
  [host port session-id txn]
  (let [url       (gen-url-for-commit-transaction host port)
        data      (gen-data-for-commit-transaction session-id txn)
        _         (info "submit data" data)
        response  (http/post url {:headers headers :body data})
        body      (json/parse-string (:body response))
        result    (get body "result")]
    (if (= result "Success")
      (let [start-ts  (get body "start_ts")
            commit-ts (get body "commit_ts")
            retvalues (get body "values")
            values    (filterv (fn [x] (not (nil? x))) retvalues)
            reads     (filterv read? txn)
            writes    (filterv write? txn)]
        {:type :ok
         :values (vec (concat (merge-reads reads values) writes))
         :ts {:rts start-ts :cts commit-ts}})
      (throw (Exception. "失败的理由")))))

(defn execute-txn-v2
  [session-id spec txn]
  (let [host (:host spec)
        port (:port spec)
        res  (commit-transaction-v2 host port session-id txn)]
    res))
