(ns s3-journal-test
  (:require
    [clojure.java.shell :as sh]
    [byte-streams :as bs]
    [clojure.test :refer :all]
    [s3-journal :as s]
    [s3-journal.s3 :as s3])
  (:import
    [com.amazonaws.services.s3.model
     GetObjectRequest
     S3Object]
    [com.amazonaws.services.s3
     AmazonS3Client]))

(defn sometimes-explode [f]
  (fn [& args]
    (assert (not= 1 (rand-int 3)) "rolled the dice, you lost")
    (apply f args)))

(defn explode-in-streaks [f]
  (fn [& args]
    (assert
      (if-not (not= 1 (rem (long (/ (System/currentTimeMillis) 1e4)) 10))
        (do (Thread/sleep 1000) false)
        true)
      "things are down for a bit")
    (apply f args)))

(defn get-test-object [^AmazonS3Client client bucket directory]
  (let [test-objects (s3/complete-uploads client bucket directory)]
    (apply concat
      (map
        (fn [object]
          (->> object
            (GetObjectRequest. bucket)
            (.getObject client)
            .getObjectContent
            bs/to-reader
            line-seq
            (map #(Long/parseLong %))
            doall))
        (sort test-objects)))))

(defn clear-test-folder [^AmazonS3Client client bucket directory]
  (doseq [u (s3/multipart-uploads client bucket directory)]
    (s3/abort-multipart client u))
  (doseq [o (s3/complete-uploads client bucket directory)]
    (.deleteObject client bucket o)))

(defmacro with-random-errors [& body]
  `(with-redefs [s3/init-multipart (sometimes-explode s3/init-multipart)
                 s3/upload-part (sometimes-explode s3/upload-part)
                 s3/end-multipart (sometimes-explode s3/end-multipart)
                 ]
     ~@body))

(defmacro with-intermittent-errors [& body]
  `(with-redefs [s3/init-multipart (explode-in-streaks s3/init-multipart)
                 s3/upload-part (explode-in-streaks s3/upload-part)
                 s3/end-multipart (explode-in-streaks s3/end-multipart)
                 ]
     ~@body))

(defn run-stress-test [access-key secret-key bucket]
  (with-redefs [s3/max-parts 4]
    (let [s3-dir "stress-test"
          directory "/tmp/journal-stress-test"
          c (s3/client access-key secret-key)
          _ (clear-test-folder c bucket (str "0/" s3-dir))
          _ (sh/sh "rm" "-rf" directory)
          j (s/journal
              {:shards 1
               :s3-access-key access-key
               :s3-secret-key secret-key
               :s3-bucket bucket
               :s3-directory-format (str \' s3-dir \' "/yy/MM/dd/hh/mm")
               :local-directory directory
               :max-batch-size 1e5})
          n 5e6]
      (dotimes [i n]
        (s/put! j (str (inc i))))
      (prn (s/stats j))
      (.close j)
      (prn (s/stats j))
      (=
        (get-test-object c "journal-test" (str "0/" s3-dir))
        (map inc (range n))))))

(deftest test-journalling
  (let [{:keys [access-key secret-key bucket]} (read-string (slurp "test/credentials.edn"))]
    (is
      (and
        (run-stress-test access-key secret-key bucket)
        (with-random-errors (run-stress-test access-key secret-key bucket))
        (with-intermittent-errors (run-stress-test access-key secret-key bucket))))))
