(ns NaNoGenMo.gutenberg
  (use NaNoGenMo.core)
  (require [clojure.data.json :as json]
           [clojure.java.io :as io]
           [clj-time.format :as t])
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

(def copyright-selector "pre")
(def content-tags #{"p" "div"})
(def metadata-re #"[\r\n](.*?):(.*)[\r\n]")

(def metadata-parsers (atom {}))

(defmacro def-metadata-parser
  [k args & body]
  (do
    (swap! metadata-parsers
      #(assoc % (str k)
        (apply list (into ['fn args] body))))
    (str k)))

(defmacro metadata-resolver
  [nom]
  (let [parsers @metadata-parsers
        k (gensym) v (gensym)]
    `(defn ~nom
      [~k ~v]
      ~(apply list
        (into ['case k]
          (concat
            (reduce concat (for [[n f] parsers] [(str n) (list f k v)]))
            [nil]))))))
        
(def-metadata-parser title
  [k v]
  [[:title v]])

(def-metadata-parser author
  [k v]
  [[:author v]])

(def date-format (java.text.SimpleDateFormat. "M d, y"))

(def-metadata-parser "release date"
  [k v]
  (let [[_ date id] (re-find #"^(.*) \[EBook #(\d+)" v)]
    (println date)
    [
      [:release-date (.parse date-format (.trim date))]
      [:gutenberg-id (Integer/parseInt id)]]))

(def-metadata-parser language
  [k v]
  [[:language v]])

(metadata-resolver resolve-metadata)

(defn parse-metadata
  [metadata-block]
  (into {}
    (apply concat
      (filter identity
        (for [[_ k v] (re-seq metadata-re metadata-block)]
          (resolve-metadata (.toLowerCase (.trim k)) (.trim v)))))))

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

(defn to-tree
  [text]
  (Jsoup/parse text "UTF-8" "http://www.gutenberg.org"))

(defn parse-content
  [text]
  (extract-content (to-tree text)))

(defn process-directory
  [dirname]
  (map parse-content (html-files dirname)))

(defn parse-to-file
  [dirname out-fname]
  (with-open [outf (io/output-stream (io/file out-fname))]
    (doseq [content (process-directory dirname)]
      (println (:title content))
      (json/write content outf)
      (.write outf "\n"))))

(defn one-file
  [fname]
  (first (html-files fname)))

(let [b (select-one (to-tree (one-file "/Volumes/Untitled 1/gutenberg")) "body")]
  (parse-metadata (.text (select-one b copyright-selector))))

'(parse-to-file "/Volumes/Untitled 1/gutenberg"
               "/Volumes/Untitled 1/gutenberg/clean.jsons")
