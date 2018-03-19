(ns fmine.core
  (:require [clj-http.client :as http])
  (:require [hickory.core :as h])
  (:require [hickory.select :as s])
  (:require [clojure.string :as str])
  (:require [clojure.core.async :as async])
  (:require [clojure.java.io :as io])
  (:gen-class))

(use 'clojure.pprint)

; (defmacro sleep-go
;   (let [num-secs (inc (rand-int 20))]
;     (do
;       (Thread/sleep (* num-secs 1000)))))


(defn- fetch-doc
  "Fetches a website body based on a url"
  [url]
  (let [num-secs (inc (rand-int 20))]
    (do
      (Thread/sleep (* num-secs 1000))
      (http/get url))))

(def parsed-doc
  (-> "<html>
          <head></head>
          <body>
          One <a href=\"http://www.google.com\">Google</a>
          Two <a href=\"http://www.bing.com\">Bing</a>
          </body>
        </html>"
        (str/replace #"\n" "")
        h/parse
        h/as-hickory))


(def work-chan (async/chan 30))
(def results-chan (async/chan))


(defn valid-url [url]
   (try (clojure.java.io/as-url url)
      url
      (catch Exception e false)))

(defn- parse-doc [doc]
  (-> doc
    h/parse
    h/as-hickory))

(defn- invalid-doc? [doc]
  (let
    [content-type (get-in doc [:headers "Content-Type"])]
    (nil? (re-find #"html" content-type))))

(defn- mine-urls [doc]
  ; Only mine the doc if it's a valid html document
  (if (invalid-doc? doc)
    []
    (let
      [alinks
        (mapv #(:href (:attrs %))
             (s/select (s/child (s/tag :a))
               (parse-doc (:body doc))))]
      (filterv valid-url alinks))))

(defn- valid-ftype? [url]
  (some #(str/ends-with? url %) [".gif" ".jpg" "jpeg"]))

(defn- filename [url]
  (last (str/split url #"/")))

(defn- download [url]
  (let [num-secs (inc (rand-int 20))]
    (do
      (prn (str "[Downloading] " url "..."))
      (with-open [in (io/input-stream url)
                  out (io/output-stream (filename url))]
        (io/copy in out))
      (Thread/sleep (* num-secs 1000))
      (prn (str "[Complete!] " url))
      [])))

(defn- start-workers
  [num-workers]
  (dotimes [idx num-workers]
    (prn (str "Starting worker with idx=" idx))
    (async/thread
      (while true
        (let [url (async/<!! work-chan)]
          (do
            (prn (str "[WORKCHAN] " url))
            (if (valid-ftype? url)
              (download url)
              (let [new-urls (mine-urls (fetch-doc url))]
                (do
                  (prn "Feeding new-urls into work-chan")
                  (map #(async/>!! work-chan %) new-urls))))))))))

(defn -main
  [& args]
  (do
    (prn "Ok")
    (start-workers 5)
    (let [seeding-url "http://www.google.com"]
      (async/>!! work-chan seeding-url))))
