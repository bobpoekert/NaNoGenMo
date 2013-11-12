(ns NaNoGenMo.core
  (require [clojure.data.json :as json])
  (import [MurmurHash3]
          [java.io File PrintWriter]
          [java.util.zip ZipFile]))

(def murmur-seed (int (* 100000 (Math/random)))) 

(defn murmurhash
  ([#^CharSequence s offset length]
    (MurmurHash3/murmurhash3_x86_32
      s offset length murmur-seed))
  ([#^CharSequence s]
    (murmurhash s 0 (count s))))

(defn cmp-k
  ([k f cmp s]
    (loop [res [] s s]
      (if (empty? s) (map second res)
        (let [cur (first s)
              fc (f cur)]
          (cond
            (< (count res) k)
              (recur
                (conj res [fc cur])
                (rest s))
            (cmp (first (first res)) fc)
              (recur
                (take k (reverse (sort-by
                  (fn [l r] (cmp (first l) (first r)))
                  (conj res [fc cur]))))
                (rest s))
            :else
              (recur res (rest s)))))))
  ([k f s]
    (cmp-k k f > s))
  ([k s]
    (cmp-k k identity > s))
  ([s]
    (cmp-k 1 identity > s)))

(defn min-k
  ([k f s]
    (cmp-k f < s))
  ([k s]
    (cmp-k identity < s)))

(def max-k cmp-k)

(defn distinct-with
  "Returns a lazy sequence of the elements of coll with duplicates removed"
  {:added "1.0"
   :static true}
  [thunk coll]
    (let [step (fn step [xs seen]
                   (lazy-seq
                    ((fn [[f :as xs] seen]
                      (when-let [s (seq xs)]
                        (let [k (thunk f)]
                          (if (contains? seen k) 
                            (recur (rest s) seen)
                            (cons f (step (rest s) (conj seen k)))))))
                     xs seen)))]
      (step coll #{})))

(defn simhash 
  ([sq restrictiveness]
    "Returns the simhash of the strings in the given sequence"
    (let [st (hash-set sq)]
      (reduce bit-xor (min-k restrictiveness (map murmurhash st)))))
  ([sq] (simhash sq 5)))

(defn dedupe
  [token-groups restrictiveness]
  (distinct-with #(simhash % restrictiveness) token-groups))

(defmacro seq-with-open
  "Like with-open, but for expressions that return lazy seq's.
  Closes the file when the seq has been completely consumed."
  [[fname fexp] body]
  `(let [~fname ~fexp
         f# (fn f# [cur#]
              (lazy-seq 
                (if (empty? cur#)
                  (do
                    (.close ~fname)
                    nil)
                  (cons (first cur#) (f# (rest cur#))))))]
      (f# ~body)))

(defn read-zipfile
  [#^File file]
    (seq-with-open [zipfile (ZipFile. file)]
      (map
        (fn [entry]
          {
            :directory (.isDirectory entry)
            :name (.getName entry)
            :size (.getSize entry)
            :body (if (.isDirectory entry)
                    nil
                    (slurp (.getInputStream zipfile entry)))})
        (enumeration-seq (.entries zipfile)))))

(defn read-zipfile-tree
  [dirname]
    (mapcat read-zipfile (filter #(.endsWith (.getName %) ".zip")
                                 (file-seq (File. dirname)))))
(defn date-to-json
  [#^java.util.Date date #^PrintWriter out]
  (.print out (long (/ (.getTime date) 1000))))

(extend java.util.Date json/JSONWriter {:-write date-to-json})

(defmacro nil-errors
  [& exprs]
  `(try
    ~exprs
    (catch Exception e# nil)))

(defn html-files
  [dirname]
  (map :body (filter (fn [f]
                      (and
                        (:body f)
                        (or
                          (.endsWith (:name f) ".html")
                          (.endsWith (:name f) ".htm"))))
                     (read-zipfile-tree dirname))))

(defn -main
  "I don't do a whole lot."
  [& args]
  (println "Hello, World!"))
