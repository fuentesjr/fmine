(ns fmine.core
  (:require [clj-http.client :as http])
  (:require [hickory.core :as h])
  (:require [hickory.select :as s])
  (:require [clojure.string :as str])
  (:require [clojure.core.async :as async])
  (:require [clojure.java.io :as io])
  (:gen-class))

(use 'clojure.pprint)

(def work-chan (async/chan 100))
(def filter-chan (async/chan 100))
(def results-chan (async/chan))

(def print-chan (async/chan 40))
(defn prnt [text]
  (async/>!! print-chan text))
(defn start-printer []
  (async/thread
    (while true
      (let [print-job (async/<!! print-chan)]
        (print (str print-job "\n"))))))

(defmacro rnd-sleep [form]
  (let [sleep-secs (inc (rand-int 10))]
    (do
      '(print (str "[rnd-sleep] sleep-secs=" sleep-secs " milliseconds=" (* sleep-secs 1000)))
      '(Thread/sleep (* sleep-secs 1000))
      form)))


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

(defn fetch-doc
  "Fetches a website body based on a url"
  [url]
  (let [num-secs (inc (rand-int 10))]
    (do
      (Thread/sleep (* num-secs 1000))
      (prnt (str "[fetch-doc] url=" url " num-secs=" num-secs " milliseconds=" (* num-secs 1000)))
      (http/get url))))

(defn fetch-doc2
  "Fetches a website body based on a url"
  [url]
  (rnd-sleep
    (do
      (prnt (str "[fetch-doc] url=" url))
      (http/get url))))

(defn valid-url [url]
   (try (clojure.java.io/as-url url)
      url
      (catch Exception e false)))

(defn parse-doc [doc]
  (-> doc
    h/parse
    h/as-hickory))

(defn hyperlinks [doc]
  (mapv #(:href (:attrs %)) (s/select (s/child (s/tag :a)) (parse-doc (:body doc)))))

(defn invalid-doc? [doc]
  (let
    [content-type (get-in doc [:headers "Content-Type"])]
    (nil? (re-find #"html" content-type))))

(defn mine-urls [doc]
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


(defn valid-ftype? [url]
  (some #(str/ends-with? url %) [".gif" ".jpg" "jpeg"]))

(defn filename [url]
  (last (str/split url #"/")))

(defn download [url]
  (let [num-secs (inc (rand-int 20))]
    (do
      (prnt (str "[Downloading] " url "..."))
      (with-open [in (io/input-stream url)
                  out (io/output-stream (filename url))]
        (io/copy in out))
      (Thread/sleep (* num-secs 1000))
      (prnt (str "[Complete!] " url))
      [])))



(defn start-supervisor []
  (async/thread
    (let [visited (atom {})]
      (while true
        (let [batch (async/<!! filter-chan)
              visits @visited
              filtered-batch (filterv #(not (contains? visits (keyword %))) batch)]
          (do
            (doseq [url filtered-batch]
              (swap! visited update-in [(keyword url)] (constantly nil)))
            (prnt (str "[start-supervisor][FILTERCHAN enqueue] visited size=" (count @visited)))
            (async/>!! work-chan filtered-batch)))))))

(defn start-workers
  [num-workers]
  (dotimes [tid num-workers]
    (prnt (str "Starting worker with tid=" tid))
    (async/thread
      (while true
        (let [workbatch (async/<!! work-chan)]
          (doseq [url workbatch]
            (do
              (prnt (str "[start-workers tid=" tid "][WORKCHAN dequeue] " url))
              (if (valid-ftype? url)
                (do
                  (prnt "[start-workers tid=" tid "] (download url=" url ")")
                  (download url))
                (let [new-urls (mine-urls (fetch-doc url))
                      url-batches (mapv vec (partition-all 20 new-urls))]
                  (do
                    (prnt (str "[start-workers tid=" tid "][FILTERCHAN enqueue] Feeding url-batches into filter-chan \nnew-urls=" new-urls "\nurl-batches=" url-batches "\n\n"))
                    (doseq [batch url-batches]
                      (async/>!! filter-chan batch))))))))))))

(defn -main
  [& args]
  (do
    (start-printer)
    (start-supervisor)
    (prnt "Spawning worker threads...")
    (start-workers 3)
    (let [seeding-urls ["http://www.google.com"]]
      (async/>!! work-chan seeding-urls)
      (async/<!! results-chan))))
