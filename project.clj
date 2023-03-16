(defproject disalg.dbcdc "0.1.0-SNAPSHOT"
  :description "运行基于 CDC 的 Jepsen 测试的执行框架"
  :url "https://github.com/Tsunaou/dbcdc-runner"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.11.1"] 
                 [jepsen "0.2.0"]
                 [jepsen.etcd "0.2.1"]
                 [seancorfield/next.jdbc "1.0.445"]
                 [org.postgresql/postgresql "42.2.12"]
                 [cheshire "5.10.0"]
                 [clj-wallhack "1.0.1"]]
  :main disalg.dbcdc
  :repl-options {:init-ns disalg.dbcdc})
