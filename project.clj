(defproject loopme/db "0.1.5"
  :description "Loopme postgresql connection logic."
  :url "http://loopme.biz"
  :license {:name "MIT license"
            :url "http://opensource.org/licenses/MIT"}
  :min-lein-version "2.0.0"
  :plugins [[s3-wagon-private "1.1.2"]]
  :repositories [["releases" {:url        "s3p://lm-artifacts/releases/"
                              :username   :env
                              :passphrase :env}]
                 ["snapshots" {:url        "s3p://lm-artifacts/snapshots/"
                               :username   :env
                               :passphrase :env}]]
  :deploy-repositories [["releases" {:url        "s3p://lm-artifacts/releases/"
                                     :username   :env
                                     :passphrase :env}]
                        ["snapshots" {:url        "s3p://lm-artifacts/snapshots/"
                                      :username   :env
                                      :passphrase :env}]]
  :exclusions [org.clojure/clojure]
  :dependencies [[loopme/cache "0.1.2"]
                 [loopme/log "0.1.8"]
                 [postgresql/postgresql "9.4-1200.jdbc4"]
                 [org.clojure/java.jdbc "0.3.5"]]
  :profiles {:dev {:resource-paths ["test-resources"]
                   :dependencies   [[org.clojure/clojure "1.6.0"]
                                    [org.clojure/tools.trace "0.7.8"]
                                    [junit/junit "4.11"]]}})
