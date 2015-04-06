(ns s3-journal.s3
  (:require
    [byte-streams :as bs]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [clojure.data :as data])
  (:import
    [com.amazonaws.regions
     Region
     Regions]
    [com.amazonaws.services.s3.model
     PartETag
     S3ObjectSummary
     PartSummary
     ListObjectsRequest
     ListPartsRequest
     ListMultipartUploadsRequest
     MultipartUpload
     UploadPartRequest
     UploadPartResult
     InitiateMultipartUploadRequest
     CompleteMultipartUploadRequest
     AbortMultipartUploadRequest]
    [com.amazonaws.auth
     BasicAWSCredentials]
    [com.amazonaws
     AmazonServiceException]
    [com.amazonaws.services.s3
     AmazonS3Client]))

;;;

(def min-part-size (* 5 1024 1024))

(def max-parts 500)

;;;

(defn ^AmazonS3Client client
  "Returns an S3 client that can be used with the other functions."
  [access-key secret-key]
  (AmazonS3Client.
    (BasicAWSCredentials.
      access-key
      secret-key)))

(defn complete-uploads
  "Returns a list of strings representing complete uploads."
  [^AmazonS3Client client bucket prefix]
  (let [rsp (.listObjects client
              (doto (ListObjectsRequest.)
                (.setBucketName bucket)
                (.setPrefix prefix)
                (.setMaxKeys (int 1e5))))]
    (->> rsp
      .getObjectSummaries
      (map (fn [^S3ObjectSummary s]
             (.getKey s))))))

(defn multipart-uploads
  "Returns a list of tuples representing open multipart uploads."
  [^AmazonS3Client client bucket prefix]
  (let [rsp (.listMultipartUploads client
              (doto (ListMultipartUploadsRequest. bucket)
                (.setPrefix prefix)
                (.setMaxUploads (int 1e5))))]
    (->> rsp
      .getMultipartUploads
      (map (fn [^MultipartUpload u]
             [bucket (.getKey u) (.getUploadId u)])))))

(defn parts
  "Given a multipart upload descriptor, returns a map of part numbers onto etags."
  [^AmazonS3Client client [bucket key upload-id :as upload]]
  (let [rsp (.listParts client
              (doto (ListPartsRequest. bucket key upload-id)
                (.setMaxParts (int 1e5))))]
    (->> rsp
      .getParts
      (map (fn [^PartSummary s]
             [(dec (.getPartNumber s))
              {:tag (.getETag s)
               :size (.getSize s)
               :uploaded? true}]))
      (into {}))))

(defn init-multipart
  "Creates a new multipart upload at the given `key`."
  [^AmazonS3Client client bucket key]
  (let [rsp (.initiateMultipartUpload client
              (InitiateMultipartUploadRequest. bucket key))]
    [bucket key (.getUploadId rsp)]))

(defn upload-part
  "Uploads a part.  If an upload with the `part-number` already exists, this is a no-op."
  [^AmazonS3Client client [bucket key upload-id] part-number contents last?]
  (let [ary (bs/to-byte-array contents)
        _ (assert (or last? (> (count ary) min-part-size)))
        rsp (try
              (.uploadPart client
                (doto (UploadPartRequest.)
                  (.setBucketName bucket)
                  (.setKey key)
                  (.setUploadId upload-id)
                  (.setPartNumber part-number)
                  (.setPartSize (count ary))
                  (.setInputStream (bs/to-input-stream (or ary (byte-array 0))))))
                  (catch AmazonServiceException e
                    (case (.getStatusCode e)

                      404
                      nil

                      (throw e))))]
    {:tag (.getETag ^UploadPartResult rsp)
     :uploaded? true
     :size (count ary)
     :last? last?}))

(defn abort-multipart
  "Cancels an open multipart upload."
  [^AmazonS3Client client [bucket key upload-id]]
  (.abortMultipartUpload client
    (AbortMultipartUploadRequest. bucket key upload-id)))

(defn end-multipart
  "Completes a multipart upload."
  [^AmazonS3Client client part->descriptor [bucket key upload-id :as upload]]
  (try
    (if (empty? part->descriptor)
      (abort-multipart client upload)
      (.completeMultipartUpload client
        (CompleteMultipartUploadRequest. bucket key upload-id
          (->> part->descriptor
            (sort-by first)
            (map
              (fn [[n {:keys [tag]}]]
                (PartETag. (inc n) tag)))))))
    (catch AmazonServiceException e
      (case (.getStatusCode e)

        #_400
        #_(abort-multipart client upload)

        404
        nil

        (throw e)))
    (catch Throwable e
      (throw e))))
