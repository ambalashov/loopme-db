(ns loopme-db.repo-tests.common
  (:require [loopme-db.db :refer :all]
            [clojure.java.jdbc :as sql]))

(defn delete-all-rows
  "TEST ONLY: Delete specific test data from db"
  [data-type]
  (sql/delete! @pooled-db data-type []))

(defn insert-row
  "TEST ONLY: Insert data to db to specific table"
  [data-type data]
  (sql/insert! @pooled-db data-type data :transaction? false))