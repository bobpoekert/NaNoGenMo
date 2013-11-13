(ns NaNoGenMo.nlp
  (use NaNoGenMo.core)
  (import [java.io File])
  (require [opennlp.nlp :as onlp]))

(defn gutenberg-data
  []
  (json-lines (java.io.File. "/Volumes/Untitled 1/gutenberg/clean.jsons.gz")))

(def get-sentences (onlp/make-sentence-detector "data/en-sent.bin"))
(def get-tokens (onlp/make-tokenizer "data/en-token.bin"))

(defn tokenize-text
  [text]
  (map get-tokens (get-sentences text)))

(defn tokenize-book
  [book]
  (try
    (assoc book "content"
      (for [section (get book "content")]
        {
          :title (tokenize-text (get section "title"))
          :paragraphs (map tokenize-text (get section "paragraphs"))}))
    (catch Exception e
      (do
        (println e)
        book))))

(defn tokenize-data
  []
  (json-pmap (File. "/Volumes/Untitled 1/gutenberg/tokenized.jsons.gz")
    (fn [book]
      (do
        (println (get book "title"))
        (tokenize-book book)))
    (gutenberg-data)))

'(tokenize-book (first (gutenberg-data)))

(defn -main
  [& args]
  (tokenize-data))
