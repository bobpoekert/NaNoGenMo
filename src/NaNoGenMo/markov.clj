(ns NaNoGenMo.markov
  (use NaNoGenMo.core)
  (import [java.nio ByteBuffer])
  (require [clj-leveldb :as level]))

(defn binify
  [v]
  (case v
    :start "\1"
    :end "\2"
    (let [^String s v]
      (.replace s "\0" ""))))

(defn unbin
  [v]
  (case v
    "\1" :start
    "\2" :end
    v))

(defn encode-key
  [k]
  (let [^String res (apply str (flatten (interpose "\0" (map binify (flatten k)))))]
    (.getBytes res "UTF-8")))

(defn decode-key
  [^bytes s]
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
    (doseq [[k v] (mapcat (comp seq frequencies) (partition batch-size inp))]
      (let [prev (or (level/get db k) 0)]
        (level/put db k (+ prev v)))))
  ([db inp]
    (update-counts db 100000 inp)))

(defn pairs
  ([prev s]
    (lazy-seq
      (if (empty? s)
        nil
        (let [nxt (first s)]
          (cons [prev nxt]
            (pairs nxt (rest s)))))))
  ([s]
    (pairs (first s) (rest s))))

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
  (mapcat
    (fn [book]
      (let [content (get book "content")]
        (println (get book "title"))
        (mapcat #(get % "paragraphs") content)))
    (json-lines fname)))

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

(defn get-transition-counts
  [db bigram]
  (let [iter (level/iterator db bigram)]
    {
      :total (second (first iter))
      :counts 
        (take-while
          (fn [[[left right] v]]
            (= left bigram))
          (rest iter))}))

(defn get-transition-probs
  [db bigram]
  (let [results (get-transition-counts db bigram)
        total (:total results)]
    (for [[[l r] c] (:counts results)]
      [r (/ total c)])))

(defn pick-bigram
  [bigrams target-set]
  (let [ceil (:total bigrams)
        tokens (:counts bigrams)]
    (apply max-key
      (fn [[k c]]
        (let [score (/ (+ c (* (Math/random) ceil)) 2)]
          (if (some (partial contains? target-set) k)
            (+ score ceil)
            score)))
      tokens)))

(def punct #{"." "," ";" ":" "?" "!"})

(defn punct?
  [token]
  (or
    (contains? punct token)
    (re-find #"^['\"«»‘’‚‛“”„‹›]" token)))

(defn re-quote
  [s]
  (java.util.regex.Pattern/quote s))

(defmacro quattern
  [s]
  (re-pattern (re-quote s)))

(def bracket-pairs
  {
    "“" #"”"
    "\"" #"\""
    "'" #"'"
    "«" #"»"
    "‘" #"’"
    "[" (quattern "]")
    "(" (quattern ")")})

(def open-bracket-re
  (re-pattern (str "([" (apply str
                          (map
                            re-quote
                            (keys bracket-pairs)))
                    "])")))


(defprotocol PatternTest
  (test-pattern [pattern target]))

(extend-type String
  PatternTest
  (test-pattern [pattern target]
    (not (= (.indexOf pattern target) -1))))

(extend-type java.util.regex.Pattern
  PatternTest
  (test-pattern [pattern target]
    (boolean (re-find pattern target))))

(defn check-target-set
  [target-set token]
  (let [res (some #(test-pattern % token) target-set)]
    (if res
      [true (disj target-set res)]
      [false target-set])))

(defn closing-pair
  [token]
  (let [matches (re-matches open-bracket-re)]
    (if matches
      (get bracket-pairs (first matches))
      nil)))

(defn pick-bigram
  [counts target-set]
  (let [ceil (:total counts)
        bigrams (:counts counts)]
    (loop [bigrams bigrams
           target-set target-set
           score 0
           res nil]
      (if (empty? bigrams)
        [res target-set]
        (let [[k c] (first bigrams)
              [in-set target-set] (check-target-set target-set k)
              new-score (+
                          (/ (+ c (* (Math/random) ceil)))
                          (if (contains? k :end) (* ceil 5) 0)
                          (if in-set ceil 0))]
          (if (or (nil? res) (> new-score score))
            (recur
              (rest bigrams)
              target-set
              new-score
              k)
            (recur
              (rest bigrams)
              target-set
              score
              res)))))))

(defn chain
  ([db start-key target-set]
    (lazy-seq
      (let [counts (get-transition-counts db start-key)]
        (if (empty? counts)
          nil
          (let [[[old res] target-set] (pick-bigram counts target-set)]
            (cons
              res
              (chain db res target-set)))))))
  ([db start-key]
    (chain db start-key #{})))

(defn single-chain
  [db start-key]
  (cons
    start-key
    (take-while
      (fn [[l r]]
        (not (= r :end)))
      (chain db start-key))))

(defn add-total-counts
  [db]
  (loop [iter (level/iterator db)
         cur nil
         cnt 0]
    (if (not (empty? iter))
      (let [[pair c] (first iter)]
        (if (= (count pair) 2)
          (let [[l r] pair]
            (if (= l cur)
              (recur (rest iter) cur (+ cnt c))
              (do
                (if (not (nil? cur))
                  (level/put db cur cnt))
                (recur (rest iter) l 1))))
          (recur (rest iter) cur cnt))))))

(defn join-bigrams
  [bigrams]
  (let [b (StringBuilder.)]
    (doseq [[l r] bigrams]
      (when (string? l)
        (if-not (contains? punct l) (.append b " "))
        (.append b l))
      (when (string? r)
        (if-not (contains? punct r) (.append b " "))
        (.append b r)))
    (.trim (.toString b))))

(defn random-chain
  [db]
  (with-open [db (level/snapshot db)]
    (join-bigrams (take 20 (single-chain db [:start "The"])))))

(defn -main
  [& args]
  ;(printall (count-keys "tokenized.jsons.gz")))
  ;(read-data "tokenized.jsons.gz" "bigrams.level"))
  (add-total-counts (create-db "bigrams.level")))
