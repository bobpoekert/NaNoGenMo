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
    (update-counts db 1000 inp)))

(defn pairs
  [s]
  (filter #(= (count %) 2)
    (partition 2 1 s)))

(defn bigrams
  [tokens]
  (filter #(= (count %) 2)
    (partition 2
      (concat
        [:start]
        tokens
        [:end]))))

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

(defn get-transition-counts
  [db bigram]
  (take-while
    (fn [[[left right] v]]
      (= left bigram))
    (level/iterator db bigram)))

'(let [db (create-db "bigrams.level")]
  (get-transition-counts db [:start "The"]))

(defn get-transition-probs
  [db bigram]
  (let [counts (get-transition-counts db bigram)
        total (reduce + (map second counts))]
    (for [[[l r] c] counts]
      [r (/ total c)])))

(defn chain
  [db start-key]
  (lazy-seq
    (let [counts (get-transition-counts db start-key)]
      (if (empty? counts)
        nil
        (let [res (second (first (apply max-key second counts)))]
          (cons
            res
            (chain db res)))))))

(defn single-chain
  [db start-key]
  (cons
    start-key
    (take-while
      (fn [[l r]]
        (not (= r :end)))
      (chain db start-key))))

'(cons [:start "The"] (take-while (fn [[l r]] (not (= r :end))) (chain (create-db "bigrams.level") [:start "The"])))

(defn -main
  [& args]
  ;(printall (count-keys "tokenized.jsons.gz")))
  (read-data "tokenized.jsons.gz" "bigrams.level"))
