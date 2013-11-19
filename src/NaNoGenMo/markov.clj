(ns NaNoGenMo.markov
  (use NaNoGenMo.core)
  (import [java.nio ByteBuffer])
  (require [clj-leveldb :as level]))

(defn binify
  [v]
  (case v
    :start "\1"
    :end "\2"
    (.replace v "\0" "")))

(defn unbin
  [v]
  (case v
    "\1" :start
    "\2" :end
    v))

(defn encode-key
  [k]
  (.getBytes
    (apply str (flatten (interpose "\0" (map binify (flatten k)))))
    "UTF-8"))

(defn decode-key
  [s]
  (partition 2 (map unbin (.split (String. s "UTF-8") "\0"))))

(defn long-to-bytes
  [n]
  (let [res (ByteBuffer/allocate 8)]
    (.putLong res n)
    (.array res)))

(defn bytes-to-long
  [b]
  (if (nil? b)
    0
    (.getLong (ByteBuffer/wrap b))))

'(seq (long-to-bytes 1204))
'(filter (fn [[a b]] (not (= a b)))
  (for [i (range 100000)]
    [i (bytes-to-long (long-to-bytes i))]))

(defn create-db
  [fname]
  (level/create-db
    fname
    {
      :key-encoder encode-key
      :key-decoder decode-key
      :val-encoder long-to-bytes
      :val-decoder bytes-to-long}))


(defn update-counts
  ([db batch-size inp]
    (let [counter (atom 0)]
      (doseq [[k v] (mapcat (comp seq frequencies) (partition batch-size inp))]
        (let [prev (or (level/get db k) 0)]
          (level/put db k (+ prev v))
          (swap! counter inc)
          (if (zero? (mod @counter 100))
            (println @counter))))))
  ([db inp]
    (update-counts db 100 inp)))

(defn bigrams
  ([beginning tokens]
    (lazy-seq
      (cond
        (empty? tokens) nil
        beginning (cons [:start (first tokens)] (bigrams false (rest tokens)))
        (empty? (rest tokens)) [(first tokens) :end]
        :else (cons
                [(first tokens) (second tokens)]
                (bigrams false (rest tokens))))))
  ([tokens]
    (bigrams true tokens)))

(defn pairs
  [s]
  (filter #(= (count %) 2)
    (partition 2 s)))

(defn paragraphs
  [fname]
  (mapcat #(get % "paragraphs") 
    (get (first (json-lines fname)) "content")))

(defn ngram-pairs
  [paragraphs]
  (mapcat
    (fn [sentence]
      (pairs (bigrams sentence)))
    paragraphs))

(defn count-keys
  [fname]
  (mapcat ngram-pairs (paragraphs fname)))

(defn read-data
  [infname outfname]
  (update-counts
    (create-db outfname)
    (count-keys infname)))

(defn printall
  [s]
  (doseq [e s]
    (println e)))

(defn -main
  [& args]
  ;(printall (count-keys "tokenized.jsons.gz")))
  (read-data "tokenized.jsons.gz" "bigrams.level"))
