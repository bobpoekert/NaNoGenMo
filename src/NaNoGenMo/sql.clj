(ns NaNoGenMo.sql
  (require [clojure.java.jdbc :as j]
           [clojure.java.jdbc.ddl :as ddl]
           [clojure.java.jdbc.sql :as s]
           [NaNoGenMo.markov :as markov]
           [clj-leveldb :as level]))

(defn sqlite-dbspec
  [fname]
  {
    :classname "org.sqlite.JDBC"
    :subprotocol "sqlite"
    :subname fname})

(defn create-table-query
  []
  (ddl/create-table
      :tokens
      [:l1 "varchar(32)"]
      [:l2 "varchar(32)"]
      [:r1 "varchar(32)"]
      [:r2 "varchar(32)"]
      [:count :int]))

(defn create-tokens-table
  [fname]
  (j/db-do-commands (sqlite-dbspec fname)
    (create-table-query)))

(defn create-index-query
  []
  (ddl/create-index
      :left_key
      :tokens
      [:l1 :l2]))

(defn create-tokens-index
  [fname]
  (j/db-do-commands (sqlite-dbspec fname) (create-index-query)))

(defn load-leveldb-data
  [level-dbname sql-dbname]
  (with-open [ldb (markov/create-db level-dbname)]
    (print "copying data...")
    (doseq [rows (partition 1000 (level/iterator ldb))]
      (print ".")
      (apply j/insert!
        (concat
          [(sqlite-dbspec sql-dbname)
           :tokens
           [:l1 :l2 :r1 :r2 :count]]
          (filter identity
            (for [[pair total] rows]
              (case (count pair)
                1 (let [[[l1 l2]] pair]
                    [l1 l2 nil nil total])
                2 (let [[[l1 l2] [r1 r2]] pair]
                    [l1 l2 r1 r2 total]) 
                nil))))))))

(defn get-total
  [[l1 l2]]
  (j/with-query-results rs [
    "select total from tokens where l1 = ? and l2 = ? and r1 = null and r2 = null limit 1"
    l1 l2]
    (:total (first rs))))

(defn get-sample
  ([[l1 l2] limit]
    (j/with-query-results rs [
      "select r1, r2, total from tokens where l1 = ? and l2 = ? and r1 is not null and r2 is not null order by random() * count desc limit ?"
      l1 l2 limit]
      (for [row rs]
        [[[(:l1 row) (:l2 row)] [(:r1 row) (:r2 row)]] (:total row)])))
  ([pair]
    (get-sample pair 1000)))

(defn get-transition-counts
  [[l1 l2]]
  {
    :total (get-total l1 l2)
    :counts (get-sample l1 l2)})

(defn -main
  [& args]
  (load-leveldb-data "bigrams.level" "bigrams.sqlite"))

