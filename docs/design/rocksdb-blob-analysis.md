# RocksDB + BlobDB

Claude prompt:

> is there an advantage to using RocksDB with BlobDB enabled for storing the chunks in chishiki? one thing i noticed in the deployment on a local server is that the chunk sizes are all over the place, from 4kb to 160kb. there are over 9,000 files among the 256 directories. it scales and its simple, but would another strategy have an advantage? i'm not so much concerned about the file sizes or number of files per se, maybe this allows the files to be reassembled more quickly? what do you think?

Short answer: no, not at your scale, and it wouldn't make reassembly meaningfully faster. I'd keep the loose-file store.

**The size spread is expected.** Your chunker is configured at 16K min / 64K avg / 256K max (`crates/chunker/src/lib.rs:17`). Content-defined chunking produces a distribution around the average by design. Anything under 16K is a whole small file or the tail chunk of a file.

**Reassembly speed.** Reading a chunk today costs an open, a read and a close on a path the kernel has usually cached. A 1 GB video is about 16,000 chunks, so that is a few tens of milliseconds of syscall overhead against seconds of actual I/O and network time. With BlobDB you swap the open for an LSM lookup (bloom filter, index block, data block) and then a pread into a blob file. That is fewer syscalls but more CPU per lookup, so roughly a wash.

BlobDB also doesn't buy you locality. Keys are blake3 hashes, and blob files are written in key order at flush, so a file's consecutive chunks land scattered through the blob file. On a spinning disk with a cold cache you would still seek per chunk.

**What RocksDB would cost you:**
- **Build weight:** a heavy C++ dependency (`librocksdb-sys`), with longer builds and a more complicated Docker image.
- **Slower space reclamation:** your mark-and-sweep remove becomes a tombstone, and space only comes back when compaction and blob GC get around to it. That also means tuning.
- **Worse backups:** loose, immutable, content-named files are ideal for rsync or restic. Blob files get rewritten by GC and churn.
- **Opacity:** today you can verify a blob with `b3sum` and inspect the store with `ls`.
- **Double writes:** each chunk goes to the write-ahead log and then the blob file, unless you disable the log.

**Where it would win:**
- **Millions of small blobs:** inode and directory overhead start to matter there. At about 35 files per shard directory you are three orders of magnitude from that.
- **Ingest throughput:** you currently `sync_all` every blob (`crates/blobstore/src/lib.rs:121`), and RocksDB would group-commit those. This is the one real advantage, and it is on the write side, not the read side.

**If you want faster reads or writes, cheaper options are:**
- **Prefetch:** read the next chunk or two ahead in ChunkStream, which hides per-chunk latency on cold or spinning disks.
- **Bigger chunks for media:** video and images barely dedupe, so a 1 MiB average would cut the chunk count 16× for those files.
- **Batched fsyncs:** write all of a file's chunks, then sync once, if upload speed ever bothers you.
- **Pack files:** append-only packs plus an index in the SQLite database you already have, git-style, if you ever do reach millions of chunks. That gets the locality BlobDB doesn't, without the dependency.
