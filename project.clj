(defproject kafka-filter "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [compojure "1.6.2"]
                 [http-kit "2.5.1"]
                 [ring/ring-defaults "0.3.2"]
                 [ring/ring-json "0.5.0"]
                 [fundingcircle/jackdaw "0.6.4"]]
  :main ^:skip-aot kafka-filter.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
