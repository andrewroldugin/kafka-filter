(ns kafka-filter.core
  (:require [org.httpkit.server :as server]
            [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.defaults :refer :all]
            [ring.middleware.json :refer [wrap-json-body wrap-json-response]]
            [ring.util.response :refer [response bad-request not-found created]]
            [jackdaw.client :as jc]
            [jackdaw.client.log :as jcl]
            [jackdaw.admin :as ja]
            [jackdaw.serdes.edn :refer [serde]])
  (:gen-class))

;; atom to store filters
(def filters (atom []))

;; The config for our Kafka app
(def kafka-config
  {"application.id" "kafka-filter-app"
   "bootstrap.servers" "localhost:9092"
   "default.key.serde" "jackdaw.serdes.EdnSerde"
   "default.value.serde" "jackdaw.serdes.EdnSerde"
   "cache.max.bytes.buffering" "0"})

;; Serdes tell Kafka how to serialize/deserialize messages
;; We'll just keep them as EDN
(def serdes
  {:key-serde (serde)
   :value-serde (serde)})

;; Each topic needs a config. The important part to note is the :topic-name key.
(def books-topic
  (merge {:topic-name "books"
          :partition-count 1
          :replication-factor 1
          :topic-config {}}
         serdes))

;; Kafka topics in our service
(def topics {"books" books-topic})

(defn publish-message! [topic msg]
  "Publish a message to the topic. Message must have :id key."
  (with-open [producer (jc/producer kafka-config serdes)]
    @(jc/produce! producer topic (:id msg) msg)))

(defn view-messages [topic offset]
  "View the messages on the given topic starting at specified offset."
  (with-open [consumer (jc/subscribed-consumer (assoc kafka-config "group.id" (str (java.util.UUID/randomUUID)))
                                               [topic])]
    (jc/poll consumer 0) ;; load assignments
    (jc/seek consumer (merge {:partition 0} topic) offset)
    (->> (jcl/log-until-inactivity consumer 100)
         (map :value)
         doall)))

(defn filter-message [filter-string msg]
  "Case insensitive filter for string representation of message."
  (re-find (re-pattern (str "(?i)" filter-string)) (pr-str msg)))

;; (filter-message "sicp" {:id 42644 :name "SiCP book"})
;; (filter-message "some" {:id 42464 :name "SiCP book"})
;; (filter-message "sicp" {:id 94637 :name "Harry Potter"})

(defn messages-count [topic]
  "Return messages count in the topic in partition 0."
  (with-open [consumer (jc/subscribed-consumer (assoc kafka-config "group.id" (str (java.util.UUID/randomUUID)))
                                               [topic])]
    (jc/seek-to-end-eager consumer)
    (jc/position consumer (merge {:partition 0} topic))))

(defn parse-int [number-string]
  "Parse integer from string. Return nil if failed."
  (try (Integer/parseInt number-string)
       (catch Exception e nil)))

(defn wrap-filter-response [x]
  "Select necessary keys from filter data for response."
  (select-keys x [:id :topic :q]))

;; (wrap-filter-response {:topic "books" :q "sicp" :id 4 :offset 6})

(defn get-filter [req]
  "Return all filters if params are empty.
Otherwise print filtered kafka messages for specified filter id."
  (let [params (:params req)]
    (cond
      (empty? params) (response (->> @filters
                                     (remove nil?)
                                     (map (partial wrap-filter-response))))
      (:id params) (if-let [id (parse-int (:id params))]
                     (if-let [f (get @filters id)]
                       (response (filter
                                  (partial filter-message (:q f))
                                  (view-messages (topics (:topic f))
                                                 (:offset f))))
                       (bad-request {:msg (str "Filter with id = " id " not found")}))
                     (bad-request {:msg "Filter id must be integer"}))
      :else (bad-request {:msg (str "Unsupported query: " (:query-string req))}))))

(defn add-filter [req]
  "Add filter."
  (let [body (:body req)]
    (if (and (map? body) (string? (:topic body)) (string? (:q body)))
      (if-let [topic (topics (:topic body))]
        (created (:uri req)
                 (wrap-filter-response
                  (last (swap! filters
                               conj (assoc body
                                           :id (count @filters)
                                           :offset (messages-count topic))))))
        (bad-request {:available-topics (keys topics)
                      :msg (str "Unknown topic '" (:topic body) "'. See available topics.")}))
      (bad-request {:msg "Unexpected body"}))))

(defn remove-filter [req]
  "Remove filter."
  (let [body (:body req)]
    (if-let [id (and (map? body) (integer? (:id body)) (:id body))]
      (if-let [removed (get @filters id)]
        (do (swap! filters assoc id nil)
            (response (wrap-filter-response removed)))
        (bad-request {:msg (str "Filter with id = " id " not found")}))
      (bad-request {:msg "Unexpected body"}))))

(defroutes app-routes
  (GET "/filter" [] get-filter)
  (POST "/filter" [] add-filter)
  (DELETE "/filter" [] remove-filter)
  (route/not-found (fn [_] (not-found {:msg "Page not found"}))))

(defn -main
  [& args]
  (let [port (or (parse-int (System/getenv "PORT")) 3000)]
    (server/run-server
     (-> #'app-routes
         (wrap-json-response)
         (wrap-json-body {:keywords? true :bigdecimals? true})
         (wrap-defaults (assoc-in site-defaults [:security :anti-forgery] false)))
     {:port port})
    (println (str "Running webserver at http:/127.0.0.1:" port "/"))))

(comment
  ;; An admin client is needed to do things like create and delete topics
  (def admin-client (ja/->AdminClient kafka-config))

  ;; Create books topic
  (ja/create-topics! admin-client [books-topic])

  ;; Publish messages to books topic
  (publish-message! books-topic {:id (rand-int 100000) :name "SICP book"})
  (publish-message! books-topic {:id (rand-int 100000) :name "book SiCP"})
  (publish-message! books-topic {:id (rand-int 100000) :name "the sicp book"})
  (publish-message! books-topic {:id (rand-int 100000) :name "Harry Potter"})

  ;; Total messages in books topic
  (messages-count books-topic)

  ;; View all messages in books topic
  (view-messages books-topic 0)

  ;; View filtered messages
  (filter (partial filter-message "sicp") [{:id 94637 :name "Harry Potter"}
                                           {:id 42464 :name "SiCP book"}
                                           {:id 42644 :name "SiCP book"}])

  ;; View filtered messages from books topic
  (filter (partial filter-message "sicp") (view-messages books-topic 0))
  (filter (partial filter-message "sicp") (view-messages books-topic 1))
  )
