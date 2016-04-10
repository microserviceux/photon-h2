(ns photon-h2.core-test
  (:require [clojure.test :refer :all]
            [photon.db.h2 :refer :all]
            [photon.db :as db]
            [com.stuartsierra.component :as component]
            [midje.sweet :refer :all]))

(def db (component/start (->DBH2 {:h2.path "/tmp/photon.h2"})))

(db/delete-all! db)
(let [start (System/currentTimeMillis)
      n 5000]
  (dorun (take n
               (map #(db/store db {:photon-timestamp (System/currentTimeMillis)
                                   :order-id %
                                   :stream-name "hello" :payload {:hello true}})
                    (range))))) 

(fact "One distinct value"
      (db/distinct-values db :stream-name) => #{"hello"})
(fact "5000 events in"
      (count (db/lazy-events db "__all__" nil)) => 5000)

(component/stop db)
