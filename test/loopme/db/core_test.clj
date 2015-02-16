(ns loopme.db.core-test
  (:import [clojure.lang IPersistentMap]
           [java.sql Timestamp]
           [java.util.concurrent Executors])
  (:require [clojure.test :refer :all]
            [loopme.db.core :as core]
            [loopme.db.config :as conf]
            [clojure.java.jdbc :as j]
            [clojure.edn :as edn]))

(def conn-str "postgresql://postgres:passw0rd@127.0.0.1:5432/test_db?pool=10&reaping_frequency=10")

(def expected-conf
  {:subprotocol "postgresql"
   :subname "//127.0.0.1:5432/test_db?pool=10&reaping_frequency=10"
   :user "postgres"
   :password "passw0rd"})

(deftest parse-postgre-db-url
  (is (= expected-conf (conf/parse-conn-string conn-str))))

(deftest create-collection-test
  (testing "Create map collection"
    (let [db (atom {})]
      (core/create-map db :test-map)
      (is (contains? @db :test-map))
      (is (instance? IPersistentMap (:test-map @db)))))
  (testing "Delete map collection"
    (let [db (atom {})]
      (core/create-map db :test-map)
      (core/remove-map db :apps)
      (is (empty? (:test-map @db))))))

(deftest data-crud-test
  (testing "Read value"
    (let [db (atom {:test-map {1 {:name "test"}}})]
      (is (= {:name "test"} (core/get-value db :test-map 1)))))
  (testing "Add value"
    (let [db (atom {:test {}})]
      (core/update-value db :test 1 {:name "name"})
      (is (= {:name "name"} (core/get-value db :test 1)))))
  (testing "Add value"
    (let [db (atom {:test {1 {:name "name"}}})]
      (core/update-value db :test 1 {:name "name_new"})
      (is (= {:name "name_new"} (core/get-value db :test 1)))))
  (testing "Remove value"
    (let [db (atom {:test {1 {:name "name"}}})]
      (core/remove-value db :test 1)
      (is (nil? (core/get-value db :test 1))))))

(deftest get-nested-val
  (let [db (atom {:table1 {1 {:id 1 :name "row1" :table1_id 1}
                     2 {:id 2 :name "row2" :table1_id 2}}
            :table2 {1 {:id 1 :name "row1-tb2" :table3_id 1}
                     2 {:id 2 :name "row2-tb2" :table3_id 3}}
            :table3 {1 {:id 1 :name "row1-tb3" :table4_id 1}
                     2 {:id 2 :name "row2-tb3" :table4_id 5}
                     3 {:id 3 :name "row3-tb3"}}
            :table4 {1 {:id 1 :name "row1-tb4"}}})]
    (testing "Should get value in collaction"
      (is (= "row1" (core/get-nested-val db :table1 1 :name)))
      (is (= 1 (core/get-nested-val db :table1 1 :table1_id )))
      (is (= "row1-tb2" (core/get-nested-val db :table1 1 :table1_id :table2 :name)))
      (is (= "row1-tb3" (core/get-nested-val db :table1 1 :table1_id
                                             :table2 :table3_id
                                             :table3 :name)))
      (is (nil? (core/get-nested-val db :table1 2 :table1_id
                                             :table2 :table3_id
                                             :table3 :table4_id))))))

(def test-source-db {:subprotocol "postgresql"
                     :subname "//127.0.0.1:5432/loopme_test"
                     :user "postgres"
                     :password " "})

(defn clean-source-db [db-spec]
  (j/delete! db-spec :apps nil))

(deftest load-from-source-test
  (let [db (atom {})]
    (testing "Should contain all data from db"
      (let [cur-time (System/currentTimeMillis)
            row {:id 1
                 :key "app-key"
                 :created_at (Timestamp. cur-time)
                 :updated_at (Timestamp. cur-time)}]
        (clean-source-db test-source-db)
        (j/insert! test-source-db :apps row)
        (core/update-db test-source-db db ["apps"])
        (is (= 1 (count (:apps @db))))
        (let [caches-row (core/get-value db :apps 1)]
          (is (= 1 (:id caches-row)))
          (is (= "app-key" (:key caches-row)))
          (is (= cur-time (:created_at caches-row)))
          (is (= cur-time (:updated_at caches-row))))))
    (testing "Should load added row"
      (let [cur-time (System/currentTimeMillis)
            row {:id 2
                 :key "app-key2"
                 :created_at (Timestamp. cur-time)
                 :updated_at (Timestamp. cur-time)}]
        (j/insert! test-source-db :apps row)
        (core/update-db test-source-db db ["apps"])
        (is (= 2 (count (:apps @db))))
        (let [caches-row (core/get-value db :apps 2)]
          (is (= 2 (:id caches-row)))
          (is (= "app-key2" (:key caches-row)))
          (is (= cur-time (:created_at caches-row)))
          (is (= cur-time (:updated_at caches-row))))))
    (testing "Should load updated row"
      (let [cur-time (System/currentTimeMillis)
            row {:id 2
                 :key "updated-app-key2"
                 :updated_at (Timestamp. cur-time)}]
        (j/update! test-source-db :apps row ["id = ?" 2])
        (core/update-db test-source-db db ["apps"])
        (is (= 2 (count (:apps @db))))
        (let [caches-row (core/get-value db :apps 2)]
          (is (= 2 (:id caches-row)))
          (is (= "updated-app-key2" (:key caches-row)))
          (is (= cur-time (:updated_at caches-row))))))))

(deftest schedule-executor-test
  (let [db (atom {})
        executor (Executors/newSingleThreadScheduledExecutor)]
    (clean-source-db test-source-db)
    (j/insert! test-source-db :apps {:id         1
                                     :key        "app-key"
                                     :created_at (Timestamp. (System/currentTimeMillis))
                                     :updated_at (Timestamp. (System/currentTimeMillis))})
    (testing "Should full load db on start"
      (is (= {} @db))
      ;start executor and init load
      (core/init
        executor
        10
        #(do

          (core/load-db test-source-db db ["apps"]))
        7
        #(do

          (core/update-db test-source-db db ["apps"])))
      (Thread/sleep 4000)
      (is (= 1 (count (:apps @db))))
      (j/insert! test-source-db :apps {:id         2
                                       :key        "app-key2"
                                       :created_at (Timestamp. (System/currentTimeMillis))
                                       :updated_at (Timestamp. (System/currentTimeMillis))})
      (Thread/sleep 4000)
      ;should start update load
      (is (= 2 (count (:apps @db))))
      (j/insert! test-source-db :apps {:id         3
                                       :key        "app-key3"
                                       :created_at (Timestamp. (System/currentTimeMillis))
                                       :updated_at (Timestamp. (System/currentTimeMillis))})
      (Thread/sleep 4000)
      (is (= 3 (count (:apps @db)))))
    (.shutdown executor)))

(defn profile
  [times f]
  (let [ct (System/currentTimeMillis)]
    (dotimes [_ times]
      (f))
    (println "Times: " times " Time:" (- (System/currentTimeMillis) ct) "ms")))

(comment

  (def last-update (atom 0))

  (def test-source-db {:subprotocol "postgresql"
                       :subname "//127.0.0.1:5432/loopme_test"
                       :user "postgres"
                       :password " "})


  (def db (atom {}))                                        ;:row-fn :cost

  (j/query postgresql-db
           ["select * from apps"] :row-fn row-fn)

  (create-map db :apps)
  (remove-map db :apps)

  (time
    (dotimes [i 20000]
      (add-value db :apps i {:name (str "apps-" i)})))


  (def table-names [
                     "ad_format_ranks"
                     "advertisers"
                     "app_categories"
                     "app_categories_genres"
                     ;"brand_verticals"
                     ;"brand_verticals_genres"
                     ;"billing_logs"
                     "apps"
                     "control_groups"
                     ;"countries"
                     "control_groups"
                     "control_group_tests"
                     "creative_ranks"
                     "genres"
                     "handsets"
                     "leads"
                     "notifications"
                     "orders"
                     "payment_details"
                     ; "schema_migrations"
                     ; "statistics_app_campaigns"
                     ; "statistics_apps"
                     ; "statistics_campaigns"
                     ;"statistics_events"
                     "platforms"
                     "statistics_monthly_apps"
                     "statistics_monthly_campaigns"
                     ;"video_meta_infos"
                     ;"worker_logs"
                     "users"
                     "ad_formats"
                     "line_items"
                     ;"alternative_billing_logs"
                     "creatives"
                     "content_categories"
                     "campaigns"
                     ])

  (def db-conf (edn/read (PushbackReader. (FileReader. "postgres-conf.edn"))))

  (time (core/load-db (:server2 db-conf) db ["campaigns"
                                          "line_items"
                                          "ad_formats"
                                          "creatives"
                                          "apps"
                                          "ad_format_ranks"
                                          "creative_ranks"
                                          "users"
                                          "control_groups"
                                          "control_group_tests"]))

  (swap! db (fn [_] {}))
  (clojure.pprint/pprint @db)

  @last-update

  (time
    (dotimes [_ 200]
      (load-db postgresql-db db ["apps"] @last-update)
      ;(let [m {:key {:key "val"}}]
      ;  (let [m2 (assoc m :key (assoc (:key m) :key "val2"))]
      ;  ;(assert (= m2 {:key {:key "val2"}}))
      ;  ))
      ;(assoc-in {} [:key :key] "val")
      ;(assoc (assoc-in {} [:key :key] "val") :key nil)
      ))

  (time
    (dotimes [_ 1000]
      (filter (fn [e] (= 3 (:state (val e))))
              (:apps @db))
      ;(count )
      ))

  (count (:apps @db)) ; => 2407
  (count  (filter (fn [e] (= 3 (:state (val e)))) (:apps @db))) ; =>2215

  (into #{} (map (fn [e] (:state (val e))) (:apps @db)))
  (get (group-by (fn [e] (:state (val e))) (:apps @db)) 0)
  (get (:apps @db) 1344)

  (profile 200000 #(get (:apps @db) 1344))

  (:state (val (first (:apps @db))))

  (clojure.pprint/pprint (first (:creative_ranks @db)))
  (clojure.pprint/pprint (first (:ad_formats @db)))
  (clojure.pprint/pprint (last (:line_items @db)))
  (clojure.pprint/pprint (last (:control_group_tests @db)))

  (count (:ad_formats @db))
  (into #{} (map (fn [e] (:app_ids (val e))) (:control_group_tests @db)))
  (into #{} (map (fn [e] (:country_ids (val e))) (:control_group_tests @db)))
  (into #{} (map (fn [e] (:orientation (val e))) (:ad_formats @db)))
  (into #{} (map (fn [e] (:app_promo (val e))) (:users @db)))
  (into #{} (map (fn [e] (:days_of_week (val e))) (:line_items @db)))
  (into #{} (map (fn [e] (:publishers (val e))) (:line_items @db)))
  (into #{} (map (fn [e] (:limited_by_daily_clicks (val e))) (:line_items @db)))
  (into #{} (map (fn [e] (:segments (val e))) (:line_items @db)))

  (profile 100 #(count  (filter (fn [e] (some #{25} (:segments (val e)))) (:line_items @db)))) ; =>2215
  (count  (filter (fn [e] (some #{32 34} (:segments (val e)))) (:line_items @db)))

  (defn array-restriction
    "val - set of seeking values"
    [arr val]
    (if (and arr val)
      (or
        (some val arr)
        (empty? arr)
        )
      true)
    )

                                                            ;;chain -> id (line_item user) (users state)

                                                            ;; filters

                                                            ;app_id 1267
                                                            ;user_id 781
  (def day_of_week #{"wed"})
  (def country_id #{"US"})
  (def app_id nil)
  (def handset_os #{"ios"})
  (def handset_os_version #{"4.4"})
  (def handset_model nil)
  (def isp nil)
  (def segments nil)
  (def publisher nil)

                                                            ;; select ad_formats
  (profile 100000
           #(filter
             (fn [e]
               (let [line_intem_id (:line_item_id (val e))
                     line_item (core/get-value db :line_items line_intem_id)
                     user_id (core/field db :line_items :user_id line_intem_id)
                     user  (core/get-value db :users user_id)]
                 (and
                   ;(:line_item_id (val e))
                   (and
                     (= 1 (:state user)) ; active user
                     (or
                       (and
                         (= 1 (:role_id user))
                         (> (:balance_cents user) 0))
                       (and
                         (= 2 (:role_id user))
                         (:app_promo user)))
                     ) ; users.app_promo = ?
                   (or
                     (= 781 (:user_id line_item)) ; line_items.user_id = ?
                     (= 1 (:role_id user))) ;users.role_id = 1
                   (= 3 (:state line_item))
                   (not (:limited_by_daily_clicks line_item))
                   (not (:limited_by_daily_views line_item))
                   (array-restriction (:days_of_week line_item) day_of_week) ;"mon" "tue" "wed" "sat" "sun"
                   (array-restriction (:country_ids line_item) country_id)
                   (array-restriction (:app_ids line_item) app_id)
                   (array-restriction (:handset_os line_item) handset_os)
                   (array-restriction (:handset_os_versions line_item) handset_os_version)
                   (array-restriction (:handset_models line_item) handset_model)
                   (array-restriction (:isps line_item) isp)
                   (array-restriction (:segments line_item) segments) ;need segments array as set
                   (array-restriction (:publishers line_item) publisher)
                   )
                 )
               )
             (:ad_formats @db)))

  )
