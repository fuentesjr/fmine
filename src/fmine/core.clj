(ns fmine.core
  (:require [clj-http.client :as http])
  (:require [hickory.core :as h])
  (:require [hickory.select :as s])
  (:require [clojure.string :as str])
  (:require [clojure.core.async :as async])
  (:require [clojure.java.io :as io])
  (:gen-class))

(use 'clojure.pprint)

(def work-chan (async/chan))
(def filter-chan (async/chan))
(def results-chan (async/chan))

(def print-chan (async/chan 40))
(defn- prnt [text]
  (async/>!! print-chan text))
(defn- start-printer []
  (async/thread
    (while true
      (let [print-job (async/<!! print-chan)]
        (print (str print-job "\n"))))))

; (defmacro rnd-sleep
;   (let [num-secs (inc (rand-int 20))]
;     (do
;       (Thread/sleep (* num-secs 1000)))))


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

(defn- fetch-doc
  "Fetches a website body based on a url"
  [url]
  (let [num-secs (inc (rand-int 5))]
    (do
      (Thread/sleep (* num-secs 1000))
      (prnt (str "[fetch-doc] url=" url " num-secs=" num-secs " milliseconds=" (* num-secs 1000)))
      (http/get url))))

(defn valid-url [url]
   (try (clojure.java.io/as-url url)
      url
      (catch Exception e false)))

(defn- parse-doc [doc]
  (-> doc
    h/parse
    h/as-hickory))

(defn- hyperlinks [doc]
  (mapv #(:href (:attrs %)) (s/select (s/child (s/tag :a)) (parse-doc (:body doc)))))

(defn- invalid-doc? [doc]
  (let
    [content-type (get-in doc [:headers "Content-Type"])]
    (nil? (re-find #"html" content-type))))

(defn- mine-urls [doc]
  ; Only mine the doc if it's a valid html document
  (if (invalid-doc? doc)
    (do
      (prnt "[mine-urls] Invalid document")
      [])
    (let [alinks (hyperlinks doc)
          urls (filterv valid-url alinks)]
      (do
        (prnt (str "[mine-urls] links=" urls "\n\n"))
        urls))))


(defn- valid-ftype? [url]
  (some #(str/ends-with? url %) [".gif" ".jpg" "jpeg"]))

(defn- filename [url]
  (last (str/split url #"/")))

(defn- download [url]
  (let [num-secs (inc (rand-int 20))]
    (do
      (prnt (str "[Downloading] " url "..."))
      (with-open [in (io/input-stream url)
                  out (io/output-stream (filename url))]
        (io/copy in out))
      (Thread/sleep (* num-secs 1000))
      (prnt (str "[Complete!] " url))
      [])))




(defn- start-supervisor []
  (async/thread
    (let [visited {}]
      (while true
        (let [url (async/<!! filter-chan)]
          (when-not (contains? visited (keyword url))
            (do
              (assoc visited (keyword url) nil)
              (prnt (str "[start-supervisor] visited size=" (count visited)))
              (async/>!! work-chan url))))))))

(defn- start-workers
  [num-workers]
  (dotimes [tid num-workers]
    (prnt (str "Starting worker with tid=" tid))
    (async/thread
      (while true
        (let [url (async/<!! work-chan)]
          (do
            (prnt (str "[start-workers][WORKCHAN DEQUEUE] " url))
            (if (valid-ftype? url)
              (do
                (prnt "[start-workers] (download url)")
                (download url))
              (let [new-urls (mine-urls (fetch-doc url))]
                (do
                  (prnt (str "[start-workers][WORKCHAN ENQUEUE] Feeding new-urls into filter-chan tid=" tid " new-urls=" new-urls "\n\n"))
                  (doseq [new-url new-urls]
                    (async/>!! filter-chan new-url)))))))))))

(defn -main
  [& args]
  (do
    (start-printer)
    (start-supervisor)
    (prnt "Spawning worker threads...")
    (start-workers 3)
    (let [seeding-url "http://www.google.com"]
      (async/>!! work-chan seeding-url)
      (async/<!! results-chan))))
