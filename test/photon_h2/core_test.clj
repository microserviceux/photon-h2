(ns photon-h2.core-test
  (:require [clojure.test :refer :all]
            [photon.db.h2 :refer :all]
            [photon.db :as db]
            [midje.sweet :refer :all]))

(def db (->DBH2 {:h2.path "/tmp/photon.h2"}))

(db/delete-all! db)
(let [start (System/currentTimeMillis)
      n 5000]
  (dorun (take n
               (map #(db/store db {:photon-timestamp (System/currentTimeMillis)
                                   :order-id %
                                   :stream-name "hello" :payload {:hello (* 2 %)}})
                    (range))))
  (fact "One distinct value"
        (db/distinct-values db :stream-name) => #{"hello"})
  (fact "5000 events in"
        (count (db/lazy-events db "__all__" nil)) => 5000)
  (facts "Correct first event"
         (fact (-> (db/lazy-events db "hello" nil) first :payload :hello)
               => 0)
         (fact (-> (db/fetch db "__all__" 0) :payload :hello) => 0))
  (fact "Correct last event"
        (fact (-> (db/lazy-events db "__all__" nil) last :payload :hello)
              => (* 2 (dec n))))
  (facts "Search function"
         (let [s (db/search db 88)]
           (fact (count s) => 1)
           (fact (-> s second :payload :hello) => nil)
           (fact (-> s first :payload :hello) => 176))))
