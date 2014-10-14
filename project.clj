(defproject loopme/db "0.1.1"
  :description "Loopme postgresql connection logic."
  :url "http://loopme.biz"
  :license {:name "MIT license"
            :url "http://opensource.org/licenses/MIT"}
  :min-lein-version "2.0.0"
  :plugins [[s3-wagon-private "1.1.2"]]
  :repositories [["loopme" {:url           "s3p://lm-artifacts/releases/"
                            :username :env :passphrase :env }]]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [loopme/cache "0.1.2"]
                 [postgresql/postgresql "8.4-702.jdbc4"]
                 [com.mchange/c3p0 "0.9.5-pre8"]])
