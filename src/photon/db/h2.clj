(ns photon.db.h2
  (:require [korma.db :refer :all]
            [korma.core :refer :all]
            [taoensso.nippy :as nippy]
            [photon.db :as db])
  (:import (java.io DataInputStream)))

(defn global-inits! [driver]
  (declare events)
  (defentity events
    (pk :ORDERID)
    (table :EVENTS)
    (entity-fields :STREAMNAME :DATA))
  (defentity streams
    (table :EVENTS)
    (entity-fields :STREAMNAME))
  (with-db driver
    (exec-raw [(str "SET LOG 0;")])
    (exec-raw [(str "SET DB_CLOSE_DELAY 10;")])
    (exec-raw [(str "SET CACHE_SIZE 131072;")])
    (exec-raw [(str "SET MULTI_THREADED 1;")])
    (exec-raw [(str "SET UNDO_LOG 0;")])
    (exec-raw [(str "SET TRACE_LEVEL_FILE 0;")])
    (exec-raw [(str "SET TRACE_LEVEL_SYSTEM_OUT 0;")])
    (exec-raw [(str "CREATE TABLE IF NOT EXISTS EVENTS "
                    "(ORDERID BIGINT, STREAMNAME VARCHAR(255),"
                    " DATA BINARY, PRIMARY KEY (ORDERID));")])
    (exec-raw [(str "CREATE INDEX IF NOT EXISTS IDX_STREAMNAME "
                    "ON EVENTS(STREAMNAME);")])))

(def page-size 500)

(defn k->k [k]
  (let [s (name k)
        s (.replaceAll s "-" "")]
    (keyword (clojure.string/upper-case s))))

(def h2-instances (ref {}))

(defmacro wrap-driver [dbh2 body]
  `(with-db
     (if-let [driver# (get @h2-instances ~dbh2)]
       driver#
       (let [driver# (h2 {:db (:h2.path (:conf ~dbh2)) :make-pool? true})]
         (global-inits! driver#)
         (dosync (alter h2-instances assoc ~dbh2 driver#))
         driver#))
     ~body))

;; TODO: Implement data encryption
(defn decrypt [data]
  (nippy/thaw data))

(defn encrypt [data]
  (nippy/freeze data))

(defrecord DBH2 [conf]
  db/DB
  (db/driver-name [this] "h2")
  (db/fetch [this stream-name id]
    (-> this
        (wrap-driver
         (if (or (= :__all__ stream-name)
                 (= "__all__" stream-name))
           (select events (where {:ORDERID [= id]}) (limit 1))
           (select events (where {:ORDERID [= id]
                                  :STREAMNAME stream-name})
                   (limit 1))))
        first :DATA decrypt))
  (db/delete! [this id]
    (wrap-driver this (delete events (where {:ORDERID id}))))
  (db/delete-all! [this]
    (wrap-driver this (delete events)))
  (db/put [this data] (db/store this data))
  (db/search [this id]
    (map (comp decrypt :DATA)
         (wrap-driver this (select events (where {:ORDERID id})))))
  (db/store [this payload]
    (wrap-driver this
                 (insert events (values {:ORDERID (:order-id payload)
                                         :STREAMNAME (:stream-name payload)
                                         :DATA (encrypt payload)}))))
  (db/distinct-values [this k]
    (into #{}
          (map #(get % (k->k k))
               (wrap-driver this
                            (select streams (modifier "DISTINCT"))))))
  (db/lazy-events [this stream-name date]
    (map (comp decrypt :DATA)
         (db/lazy-events-page this stream-name date 0)))
  (db/lazy-events-page [this stream-name date page]
    (let [oid (if (nil? date) 0 (* 1000 date))
          statement
          (wrap-driver
           this
           (if (or (= :__all__ stream-name)
                   (= "__all__" stream-name))
             (select events (where {:ORDERID [>= oid]})
                     (limit page-size) (offset page))
             (select events (where {:ORDERID [>= oid]
                                    :STREAMNAME stream-name})
                     (limit page-size) (offset page))))]
      (when-let [r (seq statement)]
        (lazy-cat r
                  (db/lazy-events-page this stream-name date
                                       (+ page page-size)))))))
