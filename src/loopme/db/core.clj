(ns loopme.db.core
  (:import [org.postgresql.jdbc4 Jdbc4Array]
           [java.util Date]
           [java.sql Timestamp]
           [java.util.concurrent Executors ScheduledExecutorService TimeUnit])
  (:require [clojure.java.jdbc :as j]
            [loopme.log :refer :all]))

(def ^:private logger (get-log *ns*))

(def update-db-executor (Executors/newSingleThreadScheduledExecutor))

(defn init
  "Initialize update executors with update functions and intervals in seconds/"
  [executor full-reload-interval full-reload-function update-interval update-fn]
  (.scheduleAtFixedRate
    ^ScheduledExecutorService executor
    full-reload-function 0 full-reload-interval TimeUnit/SECONDS)
  (.scheduleAtFixedRate
    ^ScheduledExecutorService executor
    update-fn update-interval update-interval TimeUnit/SECONDS))

;; ==============================================

(defn create-map
  "Create map collection in db. Id field as key"
  [db map-name]
  (if-not (contains? @db map-name)
    (swap! db assoc map-name {})
    (throw (Exception. "map already exists."))))

(defn remove-map
  "Remove collection from db"
  [db name]
  (if (contains? @db name)
    (swap! db dissoc name)))

(defn update-value [db coll-name id data]
  "Update value in db collection"
  (swap! db (fn [db coll-name id data]
              ;double assoc work faster than assoc-in
              (assoc db coll-name (assoc (coll-name db) id data))) coll-name id data))

(defn get-value
  "Get value from db"
  [db coll-name id]
  (get-in @db [coll-name id]))

(defn get-nested-val
  "Get nested value from db.
   fields - sequence of :link_id :linked_table"
  [db coll-name id field & fields]
  (loop [db- @db
         cn coll-name
         i id
         fld field
         flds fields]
    (if (seq? flds)
      (recur db- (first flds) (get-in db- [cn i fld]) (second flds) (nnext flds))
      (get-in db- [cn i fld]))))

(comment
  (core/get-nested-val db :child_table 1 :parent_id_in_child_tbl :parent_tbl :parent_tbl_field))

(defn remove-value [db coll-name id]
  (swap! db (fn [db coll-name id]
              (let [m (get db coll-name)]
                (assoc-in db [coll-name] (dissoc m id)))) coll-name id))

;; ==============================================

(defn row-fn [row]
  (into {} (for [e (filter #(not (nil? (val %))) row)]
             (cond
               (instance? Jdbc4Array (val e)) [(key e) (vec (.getArray (val e)))]
               (instance? Date (val e)) [(key e) (.getTime (val e))]
               :else [(key e) (val e)]))))

(defn update-db
  [sql-db db table-names]
  (try
    (let [time-now (System/currentTimeMillis)
          last-update-time (or (get-value db :system :last_update_time) 0)]
      (DEBUG logger (str "Update tables in cache. Last update time " last-update-time))
      (doseq [tn table-names
              r (j/query
                  sql-db
                  [(str "select * from " tn " where updated_at >= ?") (Timestamp. last-update-time)]
                  :row-fn row-fn)]
        (DEBUG logger (str "start update table: " tn))
        (update-value db (keyword tn) (:id r) r))
      (update-value db :system :last_update_time time-now))
    (catch Exception e
      (ERROR logger "db lib error" e)
      (throw (RuntimeException. "db lib error" e)))))

(defn load-db
  "Load all data from tables"
  [sql-db db table-names]
  (try
    (DEBUG logger (str "Load tables in cache"))
    (doseq [tn table-names
            r (j/query
                sql-db
                [(str "select * from " tn)]
                :row-fn row-fn)]
      (DEBUG logger (str "start load table: " tn))
      (update-value db (keyword tn) (:id r) r))
    (catch Exception e
      (ERROR logger "db lib error" e)
      (throw (RuntimeException. "db lib error" e)))))
