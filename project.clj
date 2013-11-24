(defproject NaNoGenMo "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :java-source-paths ["src/java"]
  :main NaNoGenMo.markov
  :jvm-opts ["-Xmx1500m" "-server"]
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
    [org.clojure/clojure "1.4.0"]
    [clojure-opennlp "0.3.1"]
    [org.jsoup/jsoup "1.4.1"]
    [factual/clj-leveldb "0.1.0"]
    [org.xerial/sqlite-jdbc "3.7.2"]
    [org.clojure/java.jdbc "0.3.0-beta1"]
    [org.clojure/data.json "0.2.3"]])
