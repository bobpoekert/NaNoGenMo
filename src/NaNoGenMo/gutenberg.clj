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

(defn resolve-metadata
  [k v]
  (case k
    "title" [[:title v]]
    "author" [[:author v]]
    "language" [[:language v]]
    "release date"
      (let [[_ date id] (re-find #"^(.*) \[EBook #(\d+)" v)]
        [
          [:release-date (.parse date-format (.trim date))]
          [:gutenberg-id (Integer/parseInt id)]])
    nil))

(defn process-tag
  [#^Element tag] 
  (case (.toLowerCase (.tagName tag))
    "p" {:paragraphs [(.trim (.text tag))]}
    "div" {:paragraphs [(.trim (.text tag))]}
    "h1" {:title (.trim (.text tag))}
    "h2" {:title (.trim (.text tag))}
    "h3" {:title (.trim (.text tag))}
    "img" (if-let [alt (.attr tag "alt")] {:paragraphs [alt]} {})
    {}))

(defn merge-tags
  ([tag-list]
    (reverse (filter identity (reduce merge-tags (list {}) tag-list))))
  ([result-list cur]
    (let [prev (first result-list)]
      (case [(apply hash-set (keys prev)) (apply hash-set (keys cur))]
        [#{:title :paragraphs} #{:title :paragraphs}] (cons cur result-list)
        [#{:title} #{:paragraphs}] (conj (rest result-list) (merge prev cur))
        [#{:paragraphs} #{:title}] (conj (rest result-list) (merge prev cur))
        [#{:title :paragraphs} #{:title}] (conj (rest result-list)
                                            (assoc prev :title
                                              (str (:title prev) " " (:title cur))))
        [#{:title} #{:title :paragraphs}] (conj (rest result-list)
                                            (assoc cur :title
                                              (str (:title prev) " " (:title cur))))
        [#{:title} #{:title}] (conj (rest result-list)
                                {:title (str (:title prev) " " (:title cur))})
        [#{:title :paragraphs} #{:paragraphs}] (conj (rest result-list)
                                                (assoc prev :paragraphs
                                                  (into (:paragraphs prev)
                                                        (:paragraphs cur))))
        [#{:paragraphs} #{:title :paragraphs}] (conj (rest result-list)
                                                (assoc cur :paragraphs
                                                  (into (:paragraphs prev)
                                                        (:paragraphs cur))))
        [#{:paragraphs} #{:paragraphs}] (conj (rest result-list)
                                                (assoc cur :paragraphs
                                                  (into (:paragraphs prev)
                                                        (:paragraphs cur))))
        [#{} #{:title :paragraphs}] (conj (rest result-list) cur)
        [#{} #{:paragraphs}] (conj (rest result-list) cur)
        [#{} #{:title}] (conj (rest result-list) cur)
        [#{:title :paragraphs} #{}] result-list
        [#{:paragraphs} #{}] result-list
        [#{:title} #{}] result-list
        [#{} #{}] (rest result-list)))))

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
  [text]
  (extract-content (to-tree text)))

(defn process-directory
  [dirname]
  (map parse-content (html-files dirname)))

(defn parse-to-file
  [dirname out-fname]
  (with-open [outf (io/output-stream (io/file out-fname))]
    (doseq [content (process-directory dirname)]
      (println (str (:title content) "-" (:author content)))
      (json/write content outf)
      (.write outf "\n"))))

(defn one-file
  [fname]
  (first (html-files fname)))

'(let [b (select-one (to-tree (one-file "/Volumes/Untitled 1/gutenberg")) "body")]
  (json/write-str (parse-metadata (.text (select-one b copyright-selector)))))

(:content (first (process-directory "/Volumes/Untitled 1/gutenberg")))

'(parse-to-file "/Volumes/Untitled 1/gutenberg"
               "/Volumes/Untitled 1/gutenberg/clean.jsons")
