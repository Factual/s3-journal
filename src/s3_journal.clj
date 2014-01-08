(ns s3-journal
  (:require
    [clojure.tools.logging :as log]
    [clojure.repl :as repl]
    [byte-streams :as bs]
    [byte-transforms :as bt]
    [durable-queue :as q]
    [clojure.string :as str]
    [s3-journal.s3 :as s3]
    [clojure.java.io :as io])
  (:import
    [java.util.concurrent
     LinkedBlockingQueue]
    [java.io
     Closeable]
    [java.util.concurrent.atomic
     AtomicLong]
    [java.util
     ArrayList
     Date]
    [java.text
     SimpleDateFormat]
    [java.lang.ref
     WeakReference]
    [java.lang.management
     ManagementFactory]
    [com.amazonaws.services.s3.model
     PartETag
     S3ObjectSummary
     PartSummary
     ListObjectsRequest
     ListPartsRequest
     ListMultipartUploadsRequest
     MultipartUpload
     UploadPartRequest
     InitiateMultipartUploadRequest
     CompleteMultipartUploadRequest
     AbortMultipartUploadRequest]
    [com.amazonaws.auth
     BasicAWSCredentials]
    [com.amazonaws.services.s3
     AmazonS3Client]))

;;;

(defprotocol IExecutor
  (submit! [_ x] "Submits an object for processing.")
  (stats [_] "Returns a description of the state of the journal."))

(defn- batching-queue [max-size max-time callback]
  (assert (or max-size max-time))
  (let [q (LinkedBlockingQueue. (int (or max-size Integer/MAX_VALUE)))
        now #(System/currentTimeMillis)
        marker (atom (now))
        flush (fn [^LinkedBlockingQueue q]
                (let [c (ArrayList.)]
                  (.drainTo q c)
                  (when-not (.isEmpty c)
                    (locking callback
                      (callback c)))))]

    ;; background loop which cleans itself up if the queue
    ;; is no longer in use
    (when max-time
      (let [q (WeakReference. q)]
        (future
          (loop []
            (when-let [q (.get q)]
              (let [m @marker]
                (try
                  (Thread/sleep (max 0 (- (+ m max-time) (now))))
                  (when (compare-and-set! marker m (now))
                    (flush q))
                  (catch Throwable e
                    (log/warn e "error in batching queue"))))
              (recur))))))

    (reify
      IExecutor
      (submit! [_ x]
        (when-not (.offer q x)
          (reset! marker (now))
          (flush q)
          (recur x)))
      java.io.Closeable
      (close [_]
        (flush q)))))

;;;

(defn- hostname []
  (let [process+host (.getName (ManagementFactory/getRuntimeMXBean))]
    (-> process+host
      (str/split #"@")
      second
      (str/replace #"/" "_"))))

(defn- position->file [id [_ part dir]]
  (str dir "/" id "-" (format "%06d" (int (/ part s3/max-parts))) ".journal"))

(defn- file->position [path]
  (when-let [[dir n] (rest (re-find #"(.*)/.*-(\d+)\.journal" path))]
    [0 (* s3/max-parts (Long/parseLong n)) dir]))

(defn- open-uploads
  [id client bucket prefix]
  (let [re (re-pattern (str id "-(\\d+)\\.journal"))]
    (filter
      #(re-find re (second %))
      (s3/multipart-uploads client bucket prefix))))

(defn- current-file-count
  "Returns the number of pre-existing complete and pending uploads in the directory for
   the given hostname."
  [q id client bucket dir]
  (let [prefix (str dir "/" id)]
    (max

      ;; writes already in AWS
      (let [uploads (distinct
                      (concat
                        (s3/complete-uploads client bucket prefix)
                        (map second (s3/multipart-uploads client bucket prefix))))]
        (count uploads))

      ;; pending writes
      (let [tasks (q/immediate-task-seq q :s3)
            highest-part (->> tasks
                           (map #(try @% (catch Throwable e nil)))
                           (map second)
                           (filter #(= dir (last %)))
                           (map second)
                           (apply max 0))]
        (doseq [t tasks]
          (q/retry! t))
        (long (Math/ceil (/ highest-part s3/max-parts)))))))

(defn- format->directory
  "Returns the directory location for the current time."
  [directory-format]
  (.format
    (SimpleDateFormat. directory-format)
    (Date.)))

;;; utility functions for inner loop

(defn- advance
  "Given a new chunk, returns the location for where it should be appended, and any additional
   actions that should be performed."
  [[bytes part directory :as pos] directory-format chunk-size]
  (let [actions (atom [])
        add-actions! #(apply swap! actions conj %&)
        directory' (format->directory directory-format)
        pos' (if (not= directory directory')
               
               ;; we've moved to a new directory, so close up previous upload and roll over
               (let [pos' [chunk-size 0 directory']]
                 (add-actions! [:end pos] [:start pos'])
                 pos')

               (let [part' (if (> bytes s3/min-part-size)
                             (inc part)
                             part)
                     bytes' (if (= part part')
                              (+ bytes chunk-size)
                              chunk-size)
                     pos' [bytes' part' directory]]

                 ;; we've hit the maximum part size, so create a new file
                 (when (and
                         (not= part part')
                         (zero? (rem part' s3/max-parts)))
                   (add-actions! [:end pos] [:start pos']))

                 ;; we've hit the minimum part size threshold, upload the part
                 (when (> bytes' s3/min-part-size)
                   (add-actions! [:upload pos']))
                 
                 pos'))]
    [pos' @actions]))

(defn- get-in-state
  "A `get-in` function for the upload-state."
  [upload-state part dir ks]
  (let [file-number (long (/ part s3/max-parts))
        part' (* file-number s3/max-parts)]
    (get-in upload-state (cons [part' dir] ks))))

(defn- assoc-in-state
  "An `assoc-in` function for the upload-state."
  [upload-state part dir ks v]
  (let [file-number (long (/ part s3/max-parts))
        part' (* file-number s3/max-parts)]
    (assoc-in upload-state (cons [part' dir] ks) v)))

(defn- update-in-state
  "An `update-in` function for the upload-state."
  [upload-state part dir ks f & args]
  (let [file-number (long (/ part s3/max-parts))
        part' (* file-number s3/max-parts)]
    (apply update-in upload-state (cons [part' dir] ks) f args)))

(defn- dissoc-state
  "Removes the pending upload described by `[part, dir]`"
  [upload-state part dir]
  (let [file-number (long (/ part s3/max-parts))
        part' (* file-number s3/max-parts)]
    (dissoc upload-state [part' dir])))

(defn- upload-descriptor
  [upload-state part dir]
  (get-in-state upload-state part dir [:descriptor]))

(defn- initial-upload-state [id client bucket prefix]
  (loop [retries 0]
    (if-let [vals (try
                    (let [descriptors (open-uploads id client bucket prefix)
                          files (map second descriptors)]
                      (zipmap
                        (->> files
                          (map file->position)
                          (map rest))
                        (map
                          (fn [descriptor]
                            {:descriptor descriptor
                             :parts (s3/parts client descriptor)})
                          descriptors)))
                    (catch Throwable e
                      nil))]
      vals
      (recur (inc retries)))))

(defn- upload-part [client ^AtomicLong upload-counter upload-state part dir last?]
  (if (get-in-state upload-state part dir [:parts part :uploaded?])
    upload-state
    (let [tasks (get-in-state upload-state part dir [:parts part :tasks])
          task-descriptors (map deref tasks)
          counts (map #(nth % 2) task-descriptors)
          bytes (map #(nth % 3) task-descriptors)
          descriptor (upload-descriptor upload-state part dir)]
      (try
        
        (let [rsp (s3/upload-part client
                    descriptor
                    (inc (rem part s3/max-parts))
                    bytes
                    last?)]
          
          (doseq [t tasks]
            (q/complete! t))

          (.addAndGet upload-counter (reduce + counts))
          
          (assoc-in-state upload-state part dir [:parts part] rsp))
        
        (catch Throwable e
          
          (log/info e "error uploading part")
          
          upload-state)))))

(defn- start-consume-loop
  [id              ; journal identifier
   q               ; durable queue
   client          ; s3 client
   bucket          ; s3 bucket
   prefix          ; the unique prefix for this journal (typically only used when sharding)
   upload-counter  ; atomic long for tracking entry uploading
   close-latch     ; an atom which marks whether the loop should be closed
   ]
  (let [upload-state (initial-upload-state id client bucket prefix)]
    
    (doseq [upload (keys upload-state)]
      (q/put! q :s3 [:end (cons 0 upload)]))
    
    (loop [upload-state upload-state]
      (let [task (try
                   (if @close-latch
                     (q/take! q :s3 5000 ::exhausted)
                     (q/take! q :s3)))]
        (when-not (= ::exhausted task)
          (let [[action [bytes part dir] & params] (try
                                                     @task
                                                     (catch Throwable e
                                                       ;; something got corrupted, all we
                                                       ;; can do is move past it
                                                       (log/warn e "error deserializing task")
                                                       [:skip]))
                descriptor (when part (upload-descriptor upload-state part dir))]

            (recur
              (try
                (if-not (or (#{:start :flush} action) descriptor)
                  
                  ;; the upload this is for no longer is valid, just drop it
                  (do
                    (q/complete! task)
                    upload-state)
                  
                  (case action

                    :flush
                    (do
                      (doseq [[part dir] (keys upload-state)]
                        (q/put! q :s3 [:end [0 part dir]]))
                      (q/complete! task)
                      upload-state)
                    
                    ;; new batch of bytes for the part
                    :conj
                    (let [[bytes] params]
                      (update-in-state upload-state part dir [:parts part :tasks]
                        #(conj (or % []) task)))
                    
                    ;; actually upload the part
                    :upload
                    (let [upload-state' (upload-part client upload-counter upload-state part dir false)]
                      (if (get-in-state upload-state' part dir [:parts part :uploaded?])
                        (q/complete! task)
                        (q/retry! task))
                      upload-state')

                    ;; start a new multipart upload
                    :start
                    (let [descriptor (or
                                       (get-in-state upload-state part dir [:descriptor])
                                       (loop []
                                         (or
                                           (try
                                             (s3/init-multipart client bucket
                                               (position->file id [0 part dir]))
                                             (catch Throwable e
                                               ;; we can't proceed until this succeeds, so
                                               ;; retrying isn't a valid option
                                               (Thread/sleep 1000)
                                               nil))
                                           (recur))))]
                      (q/complete! task)
                      (assoc-in-state upload-state part dir [:descriptor] descriptor))

                    ;; close out the multipart upload, but only if all the parts have been
                    ;; successfully uploaded
                    :end
                    (let [parts (get-in-state upload-state part dir [:parts])
                          non-uploaded (remove #(:uploaded? (val %)) parts)
                          upload-state' (or
                                          (and
                                            (empty? non-uploaded)
                                            upload-state)
                                          (and
                                            (= 1 (count non-uploaded))
                                            (let [part' (-> non-uploaded first key)]
                                              (and
                                                (= (rem part' s3/max-parts) (dec (count parts)))
                                                (upload-part client upload-counter upload-state part' dir true))))
                                          upload-state)
                          parts' (get-in-state upload-state' part dir [:parts])]
                      
                      (if upload-state'
                        
                        ;; we only had one remaining part, check if it was uploaded
                        (if (->> parts' vals (every? :uploaded?))

                          ;; all the parts have been uploaded, close it out
                          (do
                            (s3/end-multipart client
                              (zipmap
                                (map #(rem % s3/max-parts) (keys parts'))
                                (vals parts'))
                              descriptor)
                            (q/complete! task)
                            (dissoc-state upload-state' part dir))

                          (do
                            (q/retry! task)
                            (Thread/sleep 1000)
                            upload-state'))

                        ;; wait until we're in a position to close it out
                        (do
                          (q/retry! task)
                          (Thread/sleep 1000)
                          upload-state)))

                    ))
                (catch Throwable e
                  (log/info e "error in task consumption")
                  (q/retry! task)
                  upload-state)))))))))

;;;

(defn- journal-
  [{:keys
    [s3-access-key
     s3-secret-key
     s3-bucket
     s3-directory-format
     local-directory
     encoder
     compressor
     delimiter
     fsync?
     max-batch-latency
     max-batch-size
     id]
    :or {delimiter "\n"
         encoder bs/to-byte-array
         id (hostname)
         compressor identity
         fsync? true
         max-batch-latency (* 1000 60)
         s3-directory-format "yyyy/MM/dd"}}]
  
  (assert local-directory "must define :local-directory for buffering the journal")

  (.mkdirs (io/file local-directory))
  
  (let [prefix (second (re-find #"^'(.*)'" s3-directory-format))
        delimiter (bs/to-byte-array delimiter)
        compressor (if (keyword? compressor)
                     #(bt/compress % compressor)
                     compressor)
        ->bytes (fn [s]
                  (->> s
                    (map encoder)
                    (mapcat #(vector (bs/to-byte-array %) delimiter))
                    bs/to-byte-array
                    compressor
                    bs/to-byte-array))
        c (s3/client s3-access-key s3-secret-key)
        q (q/queues local-directory
            {:fsync-put? fsync?})
        initial-directory (format->directory s3-directory-format)
        pos (atom
              [0
               (* s3/max-parts (current-file-count q id c s3-bucket initial-directory))
               initial-directory])
        enqueue-counter (AtomicLong. 0)
        upload-counter (AtomicLong. 0)
        pre-action? #(#{:start} (first %))
        pre-q (batching-queue
                max-batch-size
                max-batch-latency
                (fn [s]
                  (let [bytes (->bytes s)
                        cnt (count s)
                        [pos' actions] (advance @pos s3-directory-format (count bytes))]
                    (reset! pos pos')
                    (doseq [a (filter pre-action? actions)]
                      (q/put! q :s3 a))
                    (q/put! q :s3 [:conj pos' cnt bytes])
                    (doseq [a (remove pre-action? actions)]
                      (q/put! q :s3 a)))))
        close-latch (atom false)]

    (q/put! q :s3 [:start @pos])

    (let [consume-loop (future
                         (try
                           (start-consume-loop id q c s3-bucket prefix upload-counter close-latch)
                           (catch Throwable e
                             (log/warn e "error in journal loop"))))]

      ;; consumer loop
      (reify IExecutor
        (stats [_]
          {:enqueued (.get enqueue-counter)
           :uploaded (.get upload-counter)
           :queue (get (q/stats q) "s3")})
        (submit! [_ x]
          (if @close-latch
            (throw (IllegalStateException. "attempting to write to a closed journal"))
            (do
              (.incrementAndGet enqueue-counter)
              (submit! pre-q x))))
        Closeable
        (close [_]
          (.close ^java.io.Closeable pre-q)
          (q/put! q :s3 [:flush])
          (reset! close-latch true)
          @consume-loop
          nil)))))

(def ^:private shard-ids
  (concat
    (range 10)
    (map char (range (int \a) (inc (int \z))))))

(defn journal
  [{:keys
    [s3-access-key
     s3-secret-key
     s3-bucket
     s3-directory-format
     local-directory
     encoder
     compressor
     delimiter
     fsync?
     max-batch-latency
     max-batch-size
     id
     shards]
    :or {delimiter "\n"
         encoder bs/to-byte-array
         id (hostname)
         compressor identity
         fsync? true
         max-batch-latency (* 1000 60)
         s3-directory-format "yyyy/MM/dd"}
    :as options}]
  (if shards

    ;; we want to shard the streams
    (do
      (assert (<= shards 36))
      (let [journals (zipmap
                       (range shards)
                       (map
                         (fn [shard]
                           (journal-
                             (-> options
                               (update-in [:s3-directory-format]
                                 #(str \' (nth shard-ids shard) "'/" %))
                               (update-in [:local-directory]
                                 #(when % (str local-directory "/" (nth shard-ids shard)))))))
                         (range shards)))
            counter (AtomicLong. 0)]
        (reify IExecutor
          (stats [_]
            (let [stats (->> journals vals (map stats))]
              (merge
                (->> stats (map #(dissoc % :queue)) (apply merge-with +))
                {:queue (->> stats (map :queue) (apply merge-with +))})))
          (submit! [_ x]
            (submit!
              (journals (rem (.getAndIncrement counter) shards))
              x))
          java.io.Closeable
          (close [_]
            (doseq [^Closeable j (vals journals)]
              (.close j))))))

    (journal- options)))
