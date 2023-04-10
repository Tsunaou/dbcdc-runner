(ns disalg.dbcdc.utils.cfg
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn load-edn
  "Load edn from an io/reader source (filename or io/resource)."
  [source]
  (try
    (with-open [r (io/reader source)]
      (edn/read (java.io.PushbackReader. r)))

    (catch java.io.IOException e
      (printf "Couldn't open '%s': %s\n" source (.getMessage e)))
    (catch RuntimeException e
      (printf "Error parsing edn file '%s': %s\n" source (.getMessage e)))))

;; cite from jepsen-io/elle
(defn read-history
  "Reads a history of op maps from a file."
  [filename]
  (with-open [r (java.io.PushbackReader. (io/reader filename))]
    (->> (repeatedly #(edn/read {:eof nil} r))
         (take-while identity)
         vec)))

(defn read-spec
  []
  (let [config (load-edn (io/resource "db-config.edn"))]
    config))

