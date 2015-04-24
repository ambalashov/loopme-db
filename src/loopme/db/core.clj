(ns loopme.db.core
  (:import [org.postgresql.jdbc4 Jdbc4Array]
           [java.util Date])
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

(defn get-from-db [table id]
  (first
    (j/query
      @db-spec
      [(str "select * from " (name table) " where id = ?") id]
      :row-fn row-fn)))

(defn get-cacheable [table id]
  (try
    (when (and table id)
      (let [key [table id]]
        (if (cache/has? @C key)
          (clojure.core/get @C key)
          (let [v (get-from-db table id)]
            (swap! C (fn [c] (assoc c key v)))
            v))))
    (catch Exception e
      (ERROR logger "Db get error" e))))

(defn get-app [id]
  (get-cacheable :apps id))

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
    (get-campaign :creatives 21709)))

