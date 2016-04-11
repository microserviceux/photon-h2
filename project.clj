(defproject tranchis/photon-h2 "0.9.45"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [korma "0.4.0"]
                 [tranchis/photon-db "0.9.31"]
                 [com.h2database/h2 "1.4.191"]
                 [com.taoensso/nippy "2.11.1"]
                 [midje "1.8.3"]]
  :plugins [[lein-midje "3.2"]])
