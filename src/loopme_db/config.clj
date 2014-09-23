(ns loopme-db.config)

(def postgresql-url
  "Url for PostgreSQL database with login, password, port and table."
  (or
    (System/getenv "DATABASE_URL")
    "postgresql://postgres: @localhost:5432/loopme_test"))