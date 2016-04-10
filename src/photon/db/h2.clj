(ns photon.db.h2
  (:require [korma.db :refer :all]
            [korma.core :refer :all]
            [photon.db :as db]
            [com.stuartsierra.component :as component]))

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
                    " DATA CLOB, PRIMARY KEY (ORDERID));")])
    (exec-raw [(str "CREATE INDEX IF NOT EXISTS IDX_STREAMNAME "
                    "ON EVENTS(STREAMNAME);")])))

(def page-size 500)

(defn k->k [k]
  (let [s (name k)
        s (.replaceAll s "-" "")]
    (keyword (clojure.string/upper-case s))))

(defrecord DBH2 [conf]
  component/Lifecycle
  (start [component]
    (if (nil? (:driver component))
      (let [driver (h2 {:db (:h2.path conf)
                        :make-pool? true})]
        (global-inits! driver)
        (assoc component :driver driver))
      component))
  (stop [component]
    (if (nil? (:driver component))
      component
      (do
        (assoc component :driver nil))))
  db/DB
  (db/driver-name [this] "h2")
  (db/fetch [this stream-name id]
    (with-db (:driver this)
      (select events (where {:STREAMNAME stream-name :ORDERID id}))))
  (db/delete! [this id]
    (with-db (:driver this) (delete events (where {:ORDERID id}))))
  (db/delete-all! [this]
    (with-db (:driver this) (delete events)))
  (db/put [this data] (db/store this data))
  (db/search [this id]
    (with-db (:driver this (select events (where {:ORDERID id})))))
  (db/store [this payload]
    (with-db (:driver this)
      (insert events (values {:ORDERID (:order-id payload)
                              :STREAMNAME (:stream-name payload)
                              :DATA (pr-str payload)}))))
  (db/distinct-values [this k]
    (into #{}
          (map #(get % (k->k k))
               (with-db (:driver this)
                 (select streams (modifier "DISTINCT"))))))
  (db/lazy-events [this stream-name date]
    (db/lazy-events-page this stream-name date 0))
  (db/lazy-events-page [this stream-name date page]
    (let [oid (if (nil? date) 0 (* 1000 date))
          statement (with-db (:driver this)
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
