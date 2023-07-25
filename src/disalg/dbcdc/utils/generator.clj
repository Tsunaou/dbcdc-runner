(ns disalg.dbcdc.utils.generator
  (:require
   [clojure.tools.logging :refer [info warn]]
   [jepsen.generator :as gen]
   [jepsen.util :as util]))

(defrecord DbcopGenerator [sessions]
  gen/Generator
  (update [this test ctx event]
    (if-not (= :ok (:type event))
      this
      (let [concurrency (count sessions)
            process (:process event) 
            thread  (mod process concurrency)
            session (nth sessions thread)
            next-gen (DbcopGenerator. (assoc sessions thread (next session)))]
        next-gen)))

  (op [this test ctx] 
    (if (every? nil? sessions)
      nil
      (let [p (gen/some-free-process ctx)] ;; 注意这里返回的是 worker，对应到 thread 要 mode
        (if (nil? p)
          [:pending this]
          (let [concurrency (count sessions)
                thread  (mod p concurrency)
                session (nth sessions thread)]
            (if (nil? session)
              [:pending this]
              (let [session (nth sessions thread)
                    txn     (first session)
                    ;; _       (info "Process" p ", session" session)
                    ]
                [(gen/fill-in-op {:f :txn :process p :value txn} ctx) this]))))))))

(defn dbcop-generator
  [opts sessions]
  (DbcopGenerator. sessions))