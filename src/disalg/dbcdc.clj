(ns disalg.dbcdc
  "Constructs tests, handles CLI arguments, etc."
  (:require [clojure.tools.logging :refer [info warn]]
            [clojure [pprint :refer [pprint]]
             [string :as str]]
            [jepsen [cli :as cli]
             [checker :as checker]
             [db :as jdb]
             [generator :as gen]
             [os :as os]
             [tests :as tests]
             [control :as ctrl]]
            [disalg.dbcdc
             [rw :as rw]
             [nemesis :as nemesis]
             [gen :as cdc-gen]]
            [jepsen.generator :as gen]
            [disalg.dbcdc.gen :as cdc-gen]))

(def workloads
  {:rw      rw/workload
   :none        (fn [_] tests/noop-test)})

(def all-workloads
  "A collection of workloads we run by default."
  (remove #{:none} (keys workloads)))

(def workloads-expected-to-pass
  "A collection of workload names which we expect should actually pass."
  (remove #{} all-workloads))

(def all-nemeses
  "Combinations of nemeses for tests"
  [[]
   [:pause :kill :partition :clock]])

(def special-nemeses
  "A map of special nemesis names to collections of faults"
  {:none []
   :all  [:pause :kill :partition :clock]})

(defn parse-nemesis-spec
  "Takes a comma-separated nemesis string and returns a collection of keyword
  faults."
  [spec]
  (->> (str/split spec #",")
       (map keyword)
       (mapcat #(get special-nemeses % [%]))))

(def short-isolation
  {:strict-serializable "Strict-1SR"
   :serializable        "S"
   :strong-snapshot-isolation "Strong-SI"
   :snapshot-isolation  "SI"
   :repeatable-read     "RR"
   :read-committed      "RC"
   :read-uncommitted    "RU"})

(defn dbcdc-test
  "Given an options map from the command line runner (e.g. :nodes, :ssh,
  :concurrency, ...), constructs a test map."
  [opts]
  (let [workload-name (:workload opts)
        workload      ((workloads workload-name) opts)
        db            jdb/noop
        os            os/noop
        nemesis       (nemesis/nemesis-package nil)]
    (merge tests/noop-test
           opts
           {:name (str "dbcdc " (name workload-name)
                       " " (name (:database opts))
                       (when (= (:database opts) :tidb)
                         (str " " (name (:tidb-mode opts))))
                       " num " (:txn-num opts)
                       " con " (:concurrency opts)
                       " len " (:max-txn-length opts)
                       " " (short-isolation (:isolation opts)) " ("
                       (short-isolation (:expected-consistency-model opts)) ")"
                       " " (str/join "," (map name (:nemesis opts))))
            :pure-generators true
            :os   os
            :db   db
            :checker (checker/compose
                      {:perf       (checker/perf
                                    {:nemeses (:perf nemesis)})
                       :clock      (checker/clock-plot)
                       :stats      (checker/stats)
                       :exceptions (checker/unhandled-exceptions)
                       :workload   (:checker workload)})
            :client    (:client workload)
            :nemesis   (:nemesis nemesis)
            :generator (gen/phases
                        (->> (:generator workload)
                             (gen/stagger (/ (:rate opts)))
                             (gen/nemesis (:generator nemesis))
                             (cdc-gen/limit-ok (:txn-num opts))
                             (gen/time-limit (:time-limit opts))))})))
(def cli-opts
  "Additional CLI options"
  [["-i" "--isolation LEVEL" "What level of isolation we should set: serializable, repeatable-read, etc."
    :default :snapshot-isolation
    :parse-fn keyword
    :validate [#{:read-uncommitted
                 :read-committed
                 :repeatable-read
                 :snapshot-isolation
                 :serializable}
               "Should be one of read-uncommitted, read-committed, repeatable-read, snapshot-isolation, or serializable"]]

   [nil "--existing-postgres" "If set, assumes nodes already have a running Postgres instance, skipping any OS and DB setup and teardown. Suitable for debugging issues against a local instance of Postgres (or some sort of pre-built cluster) when you don't want to set up a whole-ass Jepsen environment."
    :default false]

   [nil "--expected-consistency-model MODEL" "What level of isolation do we *expect* to observe? Defaults to the same as --isolation."
    :default nil
    :parse-fn keyword]

   [nil "--just-postgres" "Don't bother with replication or anything, just set up a plain old single-node postgres install."
    :default false]

   [nil "--key-count NUM" "Number of keys in active rotation."
    :default  10
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--nemesis FAULTS" "A comma-separated list of nemesis faults to enable"
    :parse-fn parse-nemesis-spec
    :validate [(partial every? #{:pause :kill :partition :clock :member})
               "Faults must be pause, kill, partition, clock, or member, or the special faults all or none."]]

   [nil "--max-txn-length NUM" "Maximum number of operations in a transaction."
    :default  12
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer"]]

   [nil "--max-writes-per-key NUM" "Maximum number of writes to any given key."
    :default  128
    :parse-fn parse-long
    :validate [pos? "Must be a positive integer."]]

   [nil "--nemesis-interval SECS" "Roughly how long between nemesis operations."
    :default 5
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   [nil "--txn-num NUM" "The number of transactions invoked when testing."
    :default 3000
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-r" "--rate HZ" "Approximate request rate, in hz"
    :default 100
    :parse-fn read-string
    :validate [pos? "Must be a positive number."]]

   ["-w" "--workload NAME" "What workload should we run?"
    :parse-fn keyword
    :validate [workloads (cli/one-of workloads)]]

   [nil "--database DATABASE" "Which database should we test?"
    :parse-fn keyword
    :default :postgresql
    :validate [#{:postgresql, :mysql, :tidb, :dgraph}
               "Should be one of postgresql, mysql, tidb, dgraph"]]

   [nil "--varchar-table" "If set, the fields in the tested table will be varchar(16)."
    :default false]

   [nil "--tidb-mode MODE" "Optimistic transactions or pessimistic transactions"
    :parse-fn keyword
    :default :pess
    :validate [#{:pess, :opt}
               "Should be pess or opt"]]])

(defn all-test-options
  "Takes base cli options, a collection of nemeses, workloads, and a test count,
  and constructs a sequence of test options."
  [cli nemeses workloads]
  (for [n nemeses, w workloads, i (range (:test-count cli))]
    (assoc cli
           :nemesis   n
           :workload  w)))

(defn all-tests
  "Turns CLI options into a sequence of tests."
  [test-fn cli]
  (let [nemeses   (if-let [n (:nemesis cli)] [n]  all-nemeses)
        workloads (if-let [w (:workload cli)] [w]
                          (if (:only-workloads-expected-to-pass cli)
                            workloads-expected-to-pass
                            all-workloads))]
    (->> (all-test-options cli nemeses workloads)
         (map test-fn))))

(defn opt-fn
  "Transforms CLI options before execution."
  [parsed]
  (update-in parsed [:options :expected-consistency-model]
             #(or % (get-in parsed [:options :isolation]))))

(defn -main
  "Handles command line arguments. Can either run a test, or a web server for
  browsing results."
  [& args]
  (cli/run! (merge (cli/single-test-cmd {:test-fn  dbcdc-test
                                         :opt-spec cli-opts
                                         :opt-fn   opt-fn})
                   (cli/test-all-cmd {:tests-fn (partial all-tests dbcdc-test)
                                      :opt-spec cli-opts
                                      :opt-fn   opt-fn})
                   (cli/serve-cmd))
            args))
