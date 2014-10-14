(ns loopme-db.repo.creatives
  (:require [loopme-db.db :refer :all]
            [clojure.java.jdbc :as sql]
            [loopme.cache.processor :as cache]))

(def traceable-ad-delivery-log-sql
  "SELECT * FROM creatives
   INNER JOIN ad_formats ON ad_formats.id = creatives.ad_format_id
   INNER JOIN line_items ON line_items.id = ad_formats.line_item_id
   INNER JOIN users ON users.id = line_items.user_id
   WHERE creatives.id = ? AND
         line_items.state = 1 AND
         users.state = 1 AND
         line_items.starts_at < CURRENT_TIMESTAMP AND
         line_items.finishes_at > CURRENT_TIMESTAMP - INTERVAL '7 days'")

(def active-ad-delivery-log-sql
  "line_items.limited_by_daily_clicks = ?")

(defn find-by-id
  [id]
  (cache/hit-or-miss-function
    (keyword (str "creative-by-id-" id))
    :long
    #(first (sql/query @pooled-db ["SELECT * FROM creatives WHERE id = ?" id]))))

(defn find-traceable
  "Search ad delivery log row with traceable line item"
  [creative-id]
  (cache/hit-or-miss-function
    (keyword (str "traceable-ad-delivery-logs-by-creative-id-" creative-id))
    :long
    #(first (sql/query (db-connection) [traceable-ad-delivery-log-sql creative-id]))))

(defn find-active-and-traceable
  "Search ad delivery log row with active and traceable line item"
  [creative-id]
  (cache/hit-or-miss-function
    (keyword (str "active-and-traceable-ad-delivery-logs-by-creative-id-" creative-id))
    :long
    #(first (sql/query
              (db-connection)
              [(str
                 traceable-ad-delivery-log-sql
                 " AND "
                 active-ad-delivery-log-sql) creative-id false]))))

(defn is-traceable?
  "Check is traceable current line item"
  [creative-id]
  (not (nil? (find-traceable creative-id))))

(defn is-active-and-traceable?
  "Check is active and traceable current line item"
  [creative-id]
  (not (nil? (find-active-and-traceable creative-id))))