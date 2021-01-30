(ns kafka-filter.core
  (:require [org.httpkit.server :as server]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.defaults :refer :all]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [ring.middleware.json :refer [wrap-json-body wrap-json-response]]
            [ring.util.response :refer [response bad-request not-found created]])
  (:gen-class))

(def filters (atom []))

(defn parse-int [number-string]
  (try (Integer/parseInt number-string)
       (catch Exception e nil)))

(defn get-filter [req]
  (let [params (:params req)]
    (cond
      (empty? params) (response (remove nil? @filters))
      (:id params) (if-let [id (parse-int (:id params))]
                     (response {:msg (str "TODO: Print filter " id " content")})
                     (bad-request {:msg "Filter id must be integer"}))
      :else (bad-request {:msg (str "Unsupported query: " (:query-string req))}))))

(defn add-filter [req]
  (let [body (:body req)]
    (if (and (map? body) (string? (:topic body)) (string? (:q body)))
      (created (:uri req) (last (swap! filters conj (assoc body :id (count @filters)))))
      (bad-request {:msg "Unexpected body"}))))

(defn remove-filter [req]
  (let [body (:body req)]
    (if-let [id (and (map? body) (integer? (:id body)) (:id body))]
      (if-let [removed (get @filters id)]
        (do (swap! filters assoc id nil)
            (response removed))
        (bad-request {:msg (str "Filter with id = " id " not found")}))
      (bad-request {:msg "Unexpected body"}))))

(defroutes app-routes
  (GET "/filter" [] get-filter)
  (POST "/filter" [] add-filter)
  (DELETE "/filter" [] remove-filter)
  (route/not-found (fn [_] (not-found {:msg "Page not found"}))))

(defn -main
  [& args]
  (let [port (Integer/parseInt (or (System/getenv "PORT") "3000"))]
    (server/run-server
     (-> #'app-routes
         (wrap-json-response)
         (wrap-json-body {:keywords? true :bigdecimals? true})
         (wrap-defaults (assoc-in site-defaults [:security :anti-forgery] false)))
     {:port port})
    (println (str "Running webserver at http:/127.0.0.1:" port "/"))))
