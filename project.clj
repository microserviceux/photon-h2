(defproject tranchis/photon-h2 "0.10.1"
  :description "H2 plugin for the Photon eventstore"
  :url "https://github.com/microserviceux/photon-h2"
  :license {:name "GNU Affero General Public License Version 3"
            :url "https://www.gnu.org/licenses/agpl-3.0.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [korma "0.4.3"]
                 [tranchis/photon-db "0.10.1"]
                 [com.h2database/h2 "1.4.193"]
                 [com.taoensso/nippy "2.13.0"]
                 [midje "1.8.3"]]
  :plugins [[lein-midje "3.2"]])
