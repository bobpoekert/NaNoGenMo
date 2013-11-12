(ns NaNoGenMo.gutenberg
  (use NaNoGenMo.core)
  (require [clojure.data.json :as json]
           [clojure.java.io :as io])
  (import [org.jsoup Jsoup]
          [org.jsoup.select Elements]
          [org.jsoup.nodes Element Document]))

(defn after
  [#^Element parent #^Element target]
  (.getElementsByIndexGreaterThan parent (.elementSiblingIndex target)))

(defn is-section-heading
  [#^Element e]
  (or (contains? #{"h1" "h2" "h3"} (.getTagName e))
      (and (= (.getTagName e) "a")
           (.hasAttr e "name"))))

(defn split-into-sections
  [elements]
  (partition-by is-section-heading elements))

(defn traverse 
  [#^Element root]
  (.getAllEments root))

(def copyright-selector ":contains(***START OF THE PROJECT GUTENBERG EBOOK)")
(def content-tags #{"p" "div"})
(def metadata-re #"^(.*?):(.*)$")
(def metadata-keys #{
  "title"
  "author"
  "release date"
  "language"})

(def metadata-parsers (atom {}))

(defmacro def-metadata-parser
  [k args & body]
  (do
    (swap! metadata-parsers
      #(assoc % (str k)
        (list 'fn (symbol k) args body)))
    (str k)))

(defmacro metadata-resolver
  [nom]
  (let [parsers @metadata-parsers
        k (gensym) v (gensym)]
    `(defn ~nom
      [~k ~v]
      ~(apply list (concat
        ['case k]
        (for [[n f] parsers] [n (list f k v)])
        nil)))))
        
(def-metadata-parser title
  [k v]
  [[:title v]])

(def-metadata-parser author
  [k v]
  [[:author v]])

(def-metadata-parser "release date"
  [k v]
  [
    [:release-date (parse-date "M d, y" (first (.split v "[")))]
    [:gutenberg-id (int (.group (re-find #"\#(\d+)\]" v) 1))]])

(def-metadata-parser language
  [k v]
  [[:language v]])

(metadata-resolver resolve-metadata)

(defn parse-metadata
  [metadata-block]
  (into {}
    (filter identity
      (mapcat resolve-metadata
        (for [match (re-seq metadata-re metadata-block)]
          [(.toLowerCase (.trim (.group match 0))) (.trim (.group match 1))])))))

(defn select-one
  [#^Element e selector]
  (.first (.select e selector)))

(defn extract-content
  [book-tree]
  (let [body (select-one book-tree "body")
        copyright (select-one body copyright-selector)
        metadata (parse-metadata (.text copyright))
        content (mapcat traverse (after body copyright))]
    (assoc metadata :content
      (apply vector
        (for [[heading & content] (split-into-sections content)]
          {
            :title (.text heading)
            :paragraphs (apply vector (map #(.text %)
                             (filter #(content-tags (.tagName %)) content)))})))))

(defn parse-content
  [text]
  (extract-content (Jsoup/parse text)))

(defn process-directory
  [dirname]
  (pmap parse-content (html-files dirname)))

(defn parse-to-file
  [dirname out-fname]
  (with-open [outf (io/output-stream (io/file out-fname))]
    (doseq [content (process-directory dirname)]
      (json/write content outf)
      (.write outf "\n"))))

(parse-to-file "/Volumes/Untitled 1/gutenberg"
               "/Volumes/Untitled 1/gutenberg/clean.jsons")
