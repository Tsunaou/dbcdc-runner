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
      (let [process (:process event)
            session (nth sessions process)
            ;; _       (info "Update for event" event)
            next-gen (DbcopGenerator. (assoc sessions process (next session)))]
        next-gen)))

  (op [this test ctx] 
    (if (every? nil? sessions)
      nil
      (let [p (gen/some-free-process ctx)]
        (if (nil? p)
          [:pending this]
          (let [session (nth sessions p)]
            (if (nil? session)
              [:pending this]
              (let [session (nth sessions p)
                    txn     (first session)
                    ;; _       (info "Process" p ", session" session)
                    ]
                [(gen/fill-in-op {:f :txn :process p :value txn} ctx) this]))))))))

(defn dbcop-generator
  [opts sessions]
  (DbcopGenerator. sessions))