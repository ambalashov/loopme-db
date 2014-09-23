(ns loopme-db.repo-tests.apps
  (:require [loopme-db.db :refer :all]
            [clojure.java.jdbc :as sql]))

(defn find-by-name
  "search apps by name"
  [app-name]
  (first (sql/query @pooled-db ["select * from apps where name = ?" app-name])))