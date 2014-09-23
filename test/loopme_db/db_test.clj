(ns loopme-db.db-test
  (:require [clojure.test :refer :all]))

(def conn-str "postgresql://postgres:passw0rd@127.0.0.1:5432/test_db?pool=10&reaping_frequency=10")

(def expected-conf
  {:subprotocol "postgresql"
   :subname "//127.0.0.1:5432/test_db?pool=10&reaping_frequency=10"
   :user "postgres"
   :password "passw0rd"})

(defn parse-conn-string [conn-str]
  (let [ms (re-matches #"(postgresql)://(.*):(.*)@(.*)"
                       conn-str)]
    {:subprotocol (second ms)
     :subname     (str "//" (last ms))
     :user        (nth ms 2)
     :password    (nth ms 3)}))

(deftest parse-postgre-db-url
  (is (= expected-conf (parse-conn-string conn-str))))
