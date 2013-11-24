(ns NaNoGenMo.core
  (require [clojure.data.json :as json]
           [clojure.java.io :as io])
  (import [java.io File PrintWriter BufferedReader]
          [java.util.concurrent LinkedBlockingQueue BlockingQueue TimeUnit]
          [java.util.zip ZipFile GZIPInputStream GZIPOutputStream]))

(defn top-k
  [s scorefn k]
  "Retruns the items with the top k scores as determined by scorefn.
   Eager."
  (let [top (java.util.PriorityQueue. k
              (fn [a b]
                (- (scorefn a) (scorefn b))))] 
    (doseq [e s]
      (.add top e))
    (seq (.toArray top))))

(defmacro nil-errors
  [& exprs]
  `(try
    ~@exprs
    (catch Exception e# nil)))

(defmacro seq-with-open
  "Like with-open, but for expressions that return lazy seqs.
  Closes the file when the seq has been completely consumed."
  [[fname fexp] body]
  `(let [~fname ~fexp
         f# (fn f# [cur#]
              (lazy-seq 
                (if (empty? cur#)
                  (do
                    (.close ~fname)
                    nil)
                  (try
                    (cons (first cur#) (f# (rest cur#)))
                    (catch Exception e#
                      (do
                        (.close ~fname)
                        (throw e#)))))))]
      (f# ~body)))

(defn read-zipfile
  [#^File file]
  (nil-errors
    (let [zipfile (ZipFile. file)]
      (map
        (fn [entry]
          {
            :directory (.isDirectory entry)
            :name (.getName entry)
            :size (.getSize entry)
            :body (if (.isDirectory entry)
                    nil
                    (.getInputStream zipfile entry))})
        (enumeration-seq (.entries zipfile))))))

(defmacro timeout [ms & body]
  `(let [f# (future ~@body)]
     (.get f# ~ms java.util.concurrent.TimeUnit/MILLISECONDS)))

(defn read-zipfile-tree
  [dirname]
  (mapcat read-zipfile (filter #(.endsWith (.getName %) ".zip")
                               (file-seq (File. dirname)))))
(defn date-to-json
  [#^java.util.Date date #^PrintWriter out]
  (.print out (long (/ (.getTime date) 1000))))

(extend java.util.Date json/JSONWriter {:-write date-to-json})


(defn html-files
  [dirname]
  (map :body (filter (fn [f]
                      (and
                        (:body f)
                        (or
                          (.endsWith (:name f) ".html")
                          (.endsWith (:name f) ".htm"))))
                     (read-zipfile-tree dirname))))

(defn json-lines
  "Takes a gzipped file of newline-delimited json, returns a seq of parsed lines"
  [#^File f]
  (let [res (fn res [#^BufferedReader r]
              (lazy-seq
                (let [line (.readLine r)]
                  (if-not (nil? line)
                    (cons (json/read-str line) (res r))
                    (do
                      (.close r)
                      nil)))))]
    (res (io/reader (GZIPInputStream. (io/input-stream f))))))

(defn gzip-writer
  [f]
  (io/writer (GZIPOutputStream. (io/output-stream f))))

(defn write-json-gz-lines
  [outfile inp]
  (with-open [out (gzip-writer outfile)]
    (doseq [row inp]
      (json/write row out)
      (.write out "\n"))))

(def done 'done)

(defn consume
  [#^BlockingQueue q thunk]
  (loop []
    (let [v (.take q)]
      (if (not (= v done))
        (do
          (thunk v)
          (recur))))))

(defn run-thread
  [thunk]
  (let [worker (fn worker []
                (try
                  (thunk)
                  (catch Throwable e
                    (do
                      (println "error: " e)
                      (worker)))))
        res (Thread. worker)]
    (.start res)
    res))

(defn queue-map
  [#^BlockingQueue in thunk #^BlockingQueue out]
  (run-thread
    (fn []
      (let [v (.take in)]
        (if (= v done)
          (do
            (.put out v)
            (.put in v))
          (do
            (.put out (thunk v))
            (recur)))))))

(defn unordered-pmap
  ([thunk inp queue-size n-threads]
    (let [inq (LinkedBlockingQueue. queue-size)
          outq (LinkedBlockingQueue. queue-size)
          threads (doseq [i (range n-threads)]
                    (queue-map inq thunk outq))
          res (fn res []
                (lazy-seq
                  (let [v (.take outq)]
                    (if (= v done)
                      nil
                      (cons v (res))))))]
      (res)))
  ([thunk inp]
    (unordered-pmap thunk inp 100 8)))

(defn json-pmap
  [outfile thunk inp]
  (let [out (io/writer outfile)
        inq (LinkedBlockingQueue. 100)
        outq (LinkedBlockingQueue. 100)
        threads (doseq [i (range 8)]
                  (queue-map inq (comp json/write-str thunk) outq))
        consumer (run-thread
                  (fn []
                    (consume outq
                      (fn [row]
                        (do
                          (.write out row)
                          (.write out "\n"))))))]
     
    (doseq [in-line inp]
      (.put inq in-line))
    (println "consumed input")
    (.put inq done)
    (.join consumer)))
