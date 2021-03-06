(ns loopme.db.core
  (:import [org.postgresql.jdbc4 Jdbc4Array]
           [java.util Date]
           [com.mchange.v2.c3p0 ComboPooledDataSource])
  (:require [clojure.java.jdbc :as j]
            [loopme.log :refer :all]
            [loopme.db.config :as config]
            [clojure.core.cache :as cache]))

(def ^:private logger (get-log *ns*))

(def C (atom (cache/ttl-cache-factory {} :ttl 600000))) ; 10 minutes

(defn row-fn [row]
  (into {} (for [e (filter #(not (nil? (val %))) row)]
             (cond
               (instance? Jdbc4Array (val e)) [(key e) (into #{} (vec (.getArray (val e))))]
               (instance? Date (val e)) [(key e) (.getTime (val e))]
               :else [(key e) (val e)]))))

(def db-spec (atom nil))

(defn init [url]
  (swap! db-spec (fn [_] (config/parse-conn-string url))))

(defn pool [spec]
  (let [cpds (doto (ComboPooledDataSource.)
               (.setDriverClass (:classname spec))
               (.setJdbcUrl (str "jdbc:" (:subprotocol spec) ":" (:subname spec)))
               (.setUser (:user spec))
               (.setPassword (:password spec))
               ;; expire excess connections after 30 minutes of inactivity:
               (.setMaxIdleTimeExcessConnections (* 30 60))
               ;; expire connections after 3 hours of inactivity:
               (.setMaxIdleTime (* 3 60 60))
               (.setMaxPoolSize 1))]
    {:datasource cpds}))

(def pooled-db (delay (pool @db-spec)))

(defn db-connection [] @pooled-db)

(defn get-from-db
  ([table value] (get-from-db table "id" value))
  ([table field value]
   (first
     (j/query
       (db-connection)
       [(str "select * from " (name table) " where " field " = ?") value]
       :row-fn row-fn))))

(defn get-cacheable
  ([table value] (get-cacheable table "id" value))
  ([table field value]
   (try
     (when (and table field value)
       (let [key [table field value]]
         (if (cache/has? @C key)
           (clojure.core/get @C key)
           (let [v (get-from-db table field value)]
             (swap! C (fn [c] (assoc c key v)))
             v))))
     (catch Exception e
       (ERROR logger "Db get error" e)))))

;; === DOMAIN HELPERS ===

(defn get-app [id]
  (get-cacheable :apps id))

(defn get-app-by-key [key]
  (get-cacheable :apps "key" key))

(defn get-creative [id]
  (get-cacheable :creatives id))

(defn get-ad-format
  ([id] (get-cacheable :ad_formats id))
  ([table id] (get-cacheable :ad_formats id)
  (case table
    :creatives (->> id (get-cacheable :creatives)
                    :ad_format_id (get-cacheable :ad_formats))
    nil)))

(defn get-line-item
  ([id] (get-cacheable :line_items id))
  ([table id]
   (case table
     :creatives (->> id (get-cacheable :creatives)
                     :ad_format_id (get-cacheable :ad_formats)
                     :line_item_id (get-cacheable :line_items))
     :ad_format (->> id (get-cacheable :ad_formats)
                     :line_item_id (get-cacheable :line_items))
     nil)))

(defn get-campaign
  ([id] (get-cacheable :campaigns id))
  ([table id]
   (case table
     :creatives (->> id (get-cacheable :creatives)
                     :ad_format_id (get-cacheable :ad_formats)
                     :line_item_id (get-cacheable :line_items)
                     :campaign_id (get-cacheable :campaigns))
     :ad_format (->> id (get-cacheable :ad_formats)
                     :line_item_id (get-cacheable :line_items)
                     :campaign_id (get-cacheable :campaigns))
     :line_items (->> id (get-cacheable :line_items)
                      :campaign_id (get-cacheable :campaigns))
     nil)))

(defn get-exchange [name]
  (get-cacheable :exchanges "name" name))

;; === TEST HELPERS ===

(defn clear-cache []
  (swap! C (fn [_] (cache/ttl-cache-factory {} :ttl 600000))))

(defn put-in-cache [table id value]
  (swap! C (fn [c] (assoc c [table id] value))))

(defn evict-from-cache [table id]
  (swap! C (fn [c] (dissoc c [table id]))))

(comment
  (init "postgresql://postgres:@localhost:5432/dashboard_local")
  (init "postgresql://postgres:@localhost:5432/fake_base")

  (get-cacheable :apps 336)
  (get-cacheable :creatives 23402)
  (get-cacheable :creatives 21709)

  (get-cacheable :line_items 3607)
  (get-line-item 3607)

  (clojure.pprint/pprint
    (get-line-item :creatives 21709))
  (clojure.pprint/pprint
    (get-campaign :creatives 21709))

  (-> (get-app 336) :key)
  (get-app-by-key "387e601986")

  (get-exchange "inneractive"))

