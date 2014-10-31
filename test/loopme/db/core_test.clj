(ns loopme.db.core-test
  (:import [clojure.lang IPersistentMap]
           [java.util Date]
           [java.sql Timestamp]
           [org.postgresql.jdbc4 Jdbc4Array]
           [java.util.concurrent Executors TimeUnit Executor ScheduledExecutorService])
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
    (testing "Should full load db after delay"
      (is (= {} @db))
      ;start executor and init load
      (core/init
        executor
        10
        #(do
          (println "run full reload")
          (core/remove-value db :system :last_update_time)
          (core/update-db test-source-db db ["apps"]))
        7
        #(do
          (println "run only new reload. " "last update: " (core/get-value db :system :last_update_time))
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

;(active-user           (-> params :application :user_id))
;(active-line-item)
;
;(for-day               (current-day))
;(for-country           (-> params :targeting-params :country-code))
;(for-age-group         (-> params :application :age_groups))
;(for-platform          (-> params :application :platform_id))
;(for-genre             (-> params :application :genre_id))
;(for-age-rating        (-> params :application :age_rating))
;(for-category          (-> params :application :app_category_id))
;(for-app               (-> params :application :id))
;(for-gender            (-> params :application :gendre_id))
;(for-os-named          (-> params :targeting-params :handset :os))
;(for-os-version        (-> params :targeting-params :handset :os-version))
;(for-device-model      (-> params :targeting-params :handset :model))
;(for-isp               (-> params :targeting-params :isp :name))
;(for-segments          (-> params :targeting-params :segments))
;(for-publishers        (-> params :application :company))
;(except-publishers     (-> params :application :company))
;(for-quality           (-> params :application :quality))
;(except-country        (-> params :targeting-params :country-code))
;(except-hour           (current-hour))
;(except-app            (-> params :application :id))
;(except-genre          (-> params :application :genre_id))
;(except-category       (-> params :application :app_category_id))
;(except-mobile-carrier (-> params :targeting-params :is-mobile))
;(except-isps           (-> params :targeting-params :isp :name))
;(excluded-by-daily     (-> params :targeting-params :excluded))
;(excluded-by-exchange  ids-for-exchange)
;(excluded-from-ron     (-> params :application :is_excluded_from_ron))

;--SELECT * FROM ad_formats
;INNER JOIN line_items ON line_items.id = ad_formats.line_item_id
;LEFT OUTER JOIN campaigns ON campaigns.id = line_items.campaign_id
;LEFT OUTER JOIN users     ON users.id     = line_items.user_id
;LEFT OUTER JOIN ad_format_ranks ON ad_formats.id = ad_format_ranks.ad_format_id AND ad_format_ranks.app_id = ?
;--WHERE users.state = 1
;--AND ((users.role_id = 1 AND users.balance_cents > 0)
;--     OR  (users.role_id = 2 AND users.app_promo = ?))
;-- AND (line_items.user_id = ? OR users.role_id = 1)
;--AND line_items.state = ?
;--AND line_items.limited_by_daily_clicks = ?
;--AND line_items.limited_by_daily_views = ?
;AND (? = ANY ( line_items.days_of_week ))
;
;-- AND (? = ANY (line_items.country_ids)
;--       OR line_items.country_ids = '{}'
;--      OR line_items.country_ids = '{""}')
;
; --     AND (? = ANY(line_items.app_ids)
; --             OR line_items.app_ids = '{}')
;
;-- AND (line_items.handset_os = ?
;--      OR line_items.handset_os = ''
;--      OR line_items.handset_os IS NULL)
;
;-- AND (? = ANY (line_items.handset_os_versions)
;--      OR line_items.handset_os_versions = '{}')
;
;-- AND (? = ANY (line_items.handset_models)
;--       OR line_items.handset_models = '{}')
;
;-- AND (? = ANY (line_items.isps)
;--      OR line_items.isps = '{}')
;
;-- AND (line_items.segments && ARRAY[-1]::integer[]
;--       OR line_items.segments = '{}')
;
;-- AND (line_items.publishers && ARRAY[$$$$]::varchar[]
;--      OR line_items.publishers = '{}')
;           AND ( NOT (line_items.except_publishers && ARRAY[$$$$]::varchar[]) OR line_items.except_publishers = '{}')
;                     AND (NOT ( ? = ANY( line_items.except_country_ids )))
;                     AND (NOT ( ? = ANY( line_items.except_utc_hours )))
;                     AND (NOT ( ? = ANY( line_items.except_app_ids )))
;                     AND ( NOT (? = ANY (line_items.except_isps))
;                               OR line_items.except_isps = '{}' )
;                               AND campaigns.state = 1
;                               AND ad_formats.active_creative_count > 0
;                               ORDER BY delivery_priority asc, is_new desc, rank DESC, random() LIMIT 1 OFFSET 0