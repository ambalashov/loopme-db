(ns loopme-db.db
  (:import [com.mchange.v2.c3p0 ComboPooledDataSource])
  (:require [loopme-db.config :as conf]))

(defn- parse-conn-string [conn-str]
  (let [ms (re-matches #"(postgresql)://(.*):(.*)@(.*)"
                       conn-str)]
    {:subprotocol (second ms)
     :subname     (str "//" (last ms))
     :user        (nth ms 2)
     :password    (nth ms 3)}))

;; ## Definition of c3p0's pool
;; You can change `setMinPoolSize` and `setMaxPoolSize` if server has
;; not 8 CPU cores.
(defn- pool
  [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               (.setMinPoolSize 5)
               (.setMaxPoolSize 7)
               (.setMaxStatements 360)
               (.setMaxIdleTimeExcessConnections 30)
               (.setMaxIdleTime (* 3 60)))]
    {:datasource cpds}))

(def pooled-db (delay (pool (parse-conn-string conf/postgresql-url))))

(defn db-connection [] @pooled-db)