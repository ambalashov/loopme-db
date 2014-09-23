(ns loopme-db.db-test
  (:require [clojure.test :refer :all]
            [loopme-db.db :as db]))

(def conn-str "postgresql://postgres:passw0rd@127.0.0.1:5432/test_db?pool=10&reaping_frequency=10")

(def expected-conf
  {:subprotocol "postgresql"
   :subname "//127.0.0.1:5432/test_db?pool=10&reaping_frequency=10"
   :user "postgres"
   :password "passw0rd"})


(deftest parse-postgre-db-url
  (is (= expected-conf (db/parse-conn-string conn-str))))
