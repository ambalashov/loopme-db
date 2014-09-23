(ns loopme-db.repo.line-items
  (:require [loopme-db.db :refer :all]
            [clojure.java.jdbc :as sql]
            [loopme.cache.processor :as cache]))

(defn find-by-id
  [id]
  (cache/hit-or-miss-function
    (keyword (str "line-item-by-id-" id))
    :long
    #(first (sql/query @pooled-db ["SELECT * FROM line_items
                                         WHERE id = ?" id]))))

(defn find-by-creative-id
  [creative-id]
  (cache/hit-or-miss-function
    (keyword (str "line-item-by-creative-id-" creative-id))
    :long
    #(first (sql/query @pooled-db ["SELECT * FROM line_items
                                         INNER JOIN ad_formats ON ad_formats.line_item_id = line_items.id
                                         INNER JOIN creatives ON creatives.ad_format_id = ad_formats.id
                                         WHERE creatives.id = ?" creative-id]))))
