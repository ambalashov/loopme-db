(ns loopme.db.helpers
  (:require [loopme.db.core :as core]))

;TODO test

(defn is-creative-traceable?
  [db id]
  (let [line-item (->> id
                       (core/field db :creatives :line_item_id)
                       (core/get-value db :line_items))
        user (->> (:user_id line-item)
                  (core/get-value db :users))
        cur-time (System/currentTimeMillis)
        cur-time-7-day (- cur-time (* 1000 60 60 24 7))]
    (and
      (= 1 (:state line-item))
      (= 1 (:state user))
      (< (:starts_at line-item) cur-time)
      (> (:finishes_at line-item) cur-time-7-day))))

(defn is-creative-active?
  [db id]
  (let [line-item (->> id
                       (core/field db :creatives :line_item_id)
                       (core/get-value db :line_items))]
    (false? (:limited_by_daily_clicks line-item))))

(defn is-creative-active-and-traceable?
  [db id]
  (and
    (is-creative-traceable? db id)
    (is-creative-active? db id)))