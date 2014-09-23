(ns loopme-db.repo.apps
  (:require [loopme-db.db :refer :all]
            [clojure.java.jdbc :as sql]
            [loopme.cache.processor :as cache]))

(defn find-by-id
  [id]
  (cache/hit-or-miss-function
    (keyword (str "apps-by-id-" id))
    :long
    #(first (sql/query @pooled-db ["select * from apps where id = ?" id]))))

(defn find-by-key
  "search apps by key"
  [app-key]
  (cache/hit-or-miss-function
    (keyword (str "apps-by-key-" app-key))
    :long
    #(first (sql/query @pooled-db ["select * from apps where key = ?" app-key]))))
