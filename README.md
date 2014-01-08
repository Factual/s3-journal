![](docs/scribe.jpg)

This library allows an ordered stream of entries to be uploaded to Amazon's S3 datastore.  It is implemented using Factual's [durable-queue](https://github.com/factual/durable-queue) library, which means that entries will survive process death, and that memory usage will not be affected by stalls in the uploading process.  Despite this, on a `c1.xlarge` AWS instance it can easily journal more than 10k entries/sec, comprising more than 10mb/sec in their compressed serialized form.

### usage

```clj
[factual/s3-journal "0.1.0"]
```

This library exposes only three functions in the `s3-journal` namespace: `journal`, which constructs a journal object that can be written to, `submit!`, which writes to the journal, and `stats`, which returns information about the state of the journal.

All configuration is passed in as a map to `(journal options)`, with the following parameters:

| name | optional? | description |
|------|-----------|-------------|
| `:s3-access-key` | yes | your AWS access key |
| `:s3-secret-key` | yes | your AWS secret key |
| `:s3-bucket` | yes | the AWS bucket that will be written to, must already exist |
| `:s3-directory-format` | no | the directory format, as a [SimpleDateFormat](http://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html) string, should not have leading or trailing slashes, defaults to `yyyy/MM/dd` |
| `:local-directory` | yes | the directory on the local file system that will be used for queueing, will be created if doesn't already exist |
| `:encoder` | no | a function that takes an entry and returns something that can be converted to bytes via [byte-streams](https://github.com/ztellman/byte-streams) |
| `:compressor` | no | Either one of `:gzip`, `:snappy`, `:lzma2`, or a custom function that takes a sequence of byte-arrays and returns a compressed representation |
| `:delimiter` | no | a delimiter that will be placed between entries, defaults to a newline character |
| `:max-batch-latency` | yes | a value, in milliseconds, of how long entries should be batched before being written to disk |
| `:max-batch-size` | yes | the maximum number of entries that can be batched before being written to disk |
| `:fsync?` | no | describes whether the journal will fsync after writing a batch to disk, defaults to true | 
| `:id` | no | a globally unique string describing the journal which is writing to the given location on S3, defaults to the hostname |
| `:shards` | no | the number of top-level directories within the bucket to split the entries across, useful for high-throughput applications, defaults to `nil` |

Fundamentally, the central tradeoff in these settings are data consistency vs throughput. 

If we persist each entry as it comes in, our throughput is limited to the number of [IOPS](http://en.wikipedia.org/wiki/IOPS) our hardware can handle.  However, if we can afford to lose small amounts of data (and we almost certainly can, otherwise we'd be writing it to a replicated datastore), we can bound our loss using the `:max-batch-latency` and `:max-batch-size` parameters.  At least one of these parameters must be defined, but usually it's best to define both.  Defining our batch size bounds the amount of memory that can be used by the journal, and defining our batch latency bounds the amount of time that a given entry is susceptible to the process dying.  Setting `:fsync?` to false can greatly increase throughput, but removes any safety guarantees from the other two parameters - use this parameter only if you're sure you know what you're doing.

If more than one journal on a given host is writing to the same bucket and directory on S3, a unique identifier for each must be chosen.  This identifier should be consistent across process restarts, so that partial uploads from a previous process can be properly handled.  One approach is to add a prefix to the hostname, which can be determined by `(s3-journal/hostname)`.

Calling `(.close journal)` will flush all remaining writes to S3, and only return once they have been successfully written.  A journal which has been closed cannot accept any further entries.

Calling `(stats journal)` returns a data structure in this form:

```clj
{:queue {:in-progress 0
	     :completed 64 
	     :retried 1
	     :enqueued 64 
	     :num-slabs 1 
	     :num-active-slabs 1}
 :enqueued 5000000 
 :uploaded 5000000}
```

The `:enqueued` key describes how many entries have been enqueued, and the `:uploaded` key how many have been uploaded to S3.  The `:queue` values correspond to the statistics reported by the underlying [durable-queue](https://github.com/factual/durable-queue).

### logging

The underlying AWS client libraries will log at the `INFO` level whenever there is an error calling into AWS.  This is emulated by `s3-journal` - recoverable errors are logged as `INFO`, and unrecoverable errors, such as corrupted data read back from disk, are logged as WARN.  In almost all cases, the journal will continue to work in the face of these errors, but a block of entries may be lost as a result.

### license

Copyright © 2013 Factual, Inc.

Distributed under the Eclipse Public License version 1.0.
