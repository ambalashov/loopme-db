(ns loopme.db.config)

;update interval in sec
(def update-interval 60)

;full db update sec
(def full-update-interval 300)

(def postgresql-url
  "Url for PostgreSQL database with login, password, port and table."
  (or
    (System/getenv "DATABASE_URL")
    "postgresql://postgres: @localhost:5432/loopme_test"))

(defn parse-conn-string [conn-str]
  (let [ms (re-matches #"(postgresql)://(.*):(.*)@(.*)"
                       conn-str)]
    {:subprotocol (second ms)
     :subname     (str "//" (last ms))
     :user        (nth ms 2)
     :password    (nth ms 3)}))