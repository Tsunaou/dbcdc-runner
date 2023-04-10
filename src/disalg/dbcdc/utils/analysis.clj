(ns disalg.dbcdc.utils.analysis
  (:require [disalg.dbcdc.utils.cfg :refer [read-history]]
            [elle.rw-register :as rw]
            [clojure.java.io :as io]))

(def filepath "/data/home/tsunaouyang/github-projects/dbcdc-runner/store/dbcdc rw tidb pess num 5000 con 9 len 12 SI (SI) /latest/history.edn")
(def h (read-history filepath))

(defn check-history
  "使用 Elle 检验事务历史 h 是否满足快照隔离所需的时间"
  [h]
  (rw/check {:consistency-models [:snapshot-isolation]} h))

(defn list-directories [dir]
  (let [files (map #(.getCanonicalFile %) (.listFiles (io/file dir)))]
    (concat
      (filter #(.isDirectory %) files)
      (apply concat (map #(list-directories %) (filter #(.isDirectory %) files))))))

(def store-path "/data/home/tsunaouyang/github-projects/dbcdc-runner/store")

(defn list-files [dir]
  (let [files (map #(.getCanonicalFile %) (.listFiles (io/file dir)))]
    (concat
      (filter #(.isFile %) files)
      (apply concat (map #(list-files %) (filter #(.isDirectory %) files))))))

(defn check-instance-file
  "返回检验实例目录 instance-file 下 history.edn 事务历史时间"
  [instance-file]
  (let [history-file (io/file instance-file "history.edn")
        history-path (.getAbsolutePath history-file)]
    (let [start-time (System/currentTimeMillis)
          execute    (check-history (read-history history-path))
          end-time   (System/currentTimeMillis)]
      (- end-time start-time))))

(defn average [coll] 
  (/ (reduce + coll) (count coll)))

(defn check-param-file 
  "返回检验参数目录 param-file 下所有事务历史时间所需的时间"
  [param-file]
  (let [instances (.listFiles param-file) 
        instances (filter #(not (re-find #"latest" (.getName %))) instances)]
    {:param (.getName param-file) :time (average (map check-instance-file instances))}))

(defn calculate-time
  [store-path]
  (let [files (.listFiles (io/file store-path))
        files (filter #(re-find #"dbcdc" (.getName %)) files)]
    (map check-param-file files)))
