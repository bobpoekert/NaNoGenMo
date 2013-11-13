(ns NaNoGenMo.gutenberg
  (use NaNoGenMo.core)
  (require [clojure.data.json :as json]
           ;[clojure.core.reducers :as r]
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
  (and
    (contains? #{"h1" "h2" "h3"} (.tagName e))
    (< (.length (.text e)) 1000)))

(defn split-into-sections
  [elements]
  (partition-by is-section-heading elements))

(defn traverse 
  [#^Element root]
  (.getAllElements root))

(def copyright-selector "pre")
(def content-tags #{"p" "div"})
(def metadata-re #"[\r\n](.*?):(.*?)[\r\n]")
(def date-format (java.text.SimpleDateFormat. "MMM d, yyyy"))

(defn resolve-metadata
  [k v]
  (try
    (case k
      "title" [[:title v]]
      "author" [[:author v]]
      "language" [[:language v]]
      "release date"
        (let [[_ date id] (re-find #"^(.*) \[EBook #(\d+)" v)]
          [
            [:release-date (.parse date-format (.trim date))]
            [:gutenberg-id (Integer/parseInt id)]])
      nil)
    (catch Exception e nil)))

(defn clean-whitespace
  [#^String s]
  (.replace (.trim s) "\\s+" " "))

(defn process-tag
  [#^Element tag] 
  (case (.toLowerCase (.tagName tag))
    "p" {:paragraphs [(clean-whitespace (.text tag))]}
    "div" {:paragraphs [(clean-whitespace (.text tag))]}
    "h1" {:title (clean-whitespace (.text tag))}
    "h2" {:title (clean-whitespace (.text tag))}
    "h3" {:title (clean-whitespace (.text tag))}
    "img" (if-let [alt (.attr tag "alt")] {:paragraphs [alt]} {})
    {}))

(defn join-titles
  [a b]
  (clean-whitespace (str (:title a) " " (:title b))))

(defn merge-tags
  ([tag-list]
    (reverse (filter identity (reduce merge-tags (list {}) tag-list))))
  ([result-list cur]
    (let [prev (first result-list)]
      (case [(apply hash-set (keys prev)) (apply hash-set (keys cur))]
        [#{:title :paragraphs} #{:title :paragraphs}] (cons cur result-list)
        [#{:title} #{:paragraphs}] (cons (merge prev cur) (rest result-list))
        [#{:paragraphs} #{:title}] (cons (merge prev cur)  (rest result-list))
        [#{:title :paragraphs} #{:title}] (cons cur result-list)
        [#{:title} #{:title :paragraphs}] (cons 
                                            (assoc cur :title (join-titles prev cur))
                                            (rest result-list))
        [#{:title} #{:title}] (cons 
                                {:title (join-titles prev cur)}
                                (rest result-list))
        [#{:title :paragraphs} #{:paragraphs}] (cons 
                                                (assoc prev :paragraphs
                                                  (into (:paragraphs prev)
                                                        (:paragraphs cur)))
                                                (rest result-list))
        [#{:paragraphs} #{:title :paragraphs}] (cons
                                                (assoc cur :paragraphs
                                                  (into (:paragraphs prev)
                                                        (:paragraphs cur)))
                                                (rest result-list))
        [#{:paragraphs} #{:paragraphs}] (cons 
                                          (assoc cur :paragraphs
                                            (into (:paragraphs prev)
                                                  (:paragraphs cur)))
                                          (rest result-list))
        [#{} #{:title :paragraphs}] (cons cur (rest result-list))
        [#{} #{:paragraphs}] (cons cur (rest result-list))
        [#{} #{:title}] (cons cur (rest result-list))
        [#{:title :paragraphs} #{}] result-list
        [#{:paragraphs} #{}] result-list
        [#{:title} #{}] result-list
        [#{} #{}] result-list))))

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
    (assoc metadata :content (merge-tags (map process-tag content)))))

(defn to-tree
  [text]
  (Jsoup/parse text "UTF-8" "http://www.gutenberg.org"))

(defn parse-content
  [#^java.io.InputStream text]
  (try
    (timeout 5000
      (extract-content (to-tree text)))
    (catch Exception e
      (do
        (println e)
        nil))
    (finally
      (.close text))))

(defn process-directory
  [dirname]
  (pmap parse-content (html-files dirname)))

(defn parse-to-file
  [dirname out-fname]
  (with-open [outf (io/writer (io/file out-fname))]
    (doseq [content (process-directory dirname)]
      (if content
        (do
          (println (str (:title content) " - " (:author content)))
          (json/write content outf)
          (.write outf "\n"))))))

(defn one-file
  [fname]
  (first (html-files fname)))

'(let [b (select-one (to-tree (one-file "/Volumes/Untitled 1/gutenberg")) "body")]
  (json/write-str (parse-metadata (.text (select-one b copyright-selector)))))

'(:content (first (process-directory "/Volumes/Untitled 1/gutenberg")))

(defn run
  []
  (parse-to-file "/Volumes/Untitled 1/gutenberg"
                 "/Volumes/Untitled 1/gutenberg/clean.jsons"))

(defn -main
  [& args]
  (run))
