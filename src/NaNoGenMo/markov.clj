(ns NaNoGenMo.markov
  (use NaNoGenMo.core)
  (require [clj-leveldb :as level]))

(defn serializable? [v]
  (instance? java.io.Serializable v))

(defn serialize 
  "Serializes value, returns a byte array"
  [v]
  (let [buff (java.io.ByteArrayOutputStream. 1024)]
    (with-open [dos (java.io.ObjectOutputStream. buff)]
      (.writeObject dos v))
    (.toByteArray buff)))

(defn deserialize 
  "Accepts a byte array, returns deserialized value"
  [#^bytes b]
  (with-open [dis (java.io.ObjectInputStream.
                   (java.io.ByteArrayInputStream. b))]
    (.readObject dis)))

(defn create-db
  [fname]
  (level/create-db
    fname
    {
      :key-encoder serialize
      :value-encoder serialize
      :key-decoder deserialize
      :value-decoder deserailzie}))
      
