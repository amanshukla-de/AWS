# Amazon S3

---

## 1. S3 Fundamentals

**What it is:** Amazon S3 (Simple Storage Service) is **object storage** — you store and retrieve whole objects (files + metadata) via an HTTP API, not by mounting a filesystem.

**Why companies use it / problem it solves:** need to store massive, growing amounts of data (structured, semi-structured, unstructured) cheaply and durably, accessible from anywhere, without managing disks/servers. It became the default "bottom layer" for almost every modern data platform.

**Object vs File vs Block storage (know the distinction):**
| Type | Unit | Access | Example use |
|---|---|---|---|
| Block storage | Fixed-size blocks | OS mounts a volume, low-level disk access | EBS — databases needing fast random I/O |
| File storage | Files in a folder hierarchy | Filesystem (POSIX) access | EFS/NFS — shared app file access |
| Object storage | Whole objects + metadata | HTTP API (PUT/GET/DELETE) | **S3** — data lakes, backups, large-scale storage |

**Why it matters:** S3 is not a filesystem — no in-place edits, no true directories, no POSIX locking. Every "why does Spark do X on S3" question traces back to this.

**Core terms:**
- **Bucket:** top-level container, globally unique name, tied to one region.
- **Object:** the actual data (bytes) + metadata, identified by a **key**.
- **Key:** the full object identifier (e.g. `sales/year=2026/month=08/file.parquet`) — this is just a **string**, not a real path.
- **Prefix:** the part of a key before the object name (e.g. `sales/year=2026/month=08/`) — used for filtering/listing, and for partition-style organization.
- **Metadata:** key-value info attached to an object (content-type, custom tags, etc.).
- **Region:** buckets live in one AWS region — affects latency, cost, and compliance.
- **Namespace:** S3 is a flat global key-value store per bucket — there's no real nested folder tree underneath.

**Bucket → Prefix → Object (important distinction):**
```
Bucket: company-data
Prefix: sales/year=2026/month=08/
Object (key): sales/year=2026/month=08/day=15/part-0001.parquet
```
**Why a prefix is NOT a physical folder:** S3 stores objects as flat key-value pairs. `sales/year=2026/` is just a string prefix, not a directory inode. The AWS console *simulates* folders by grouping keys with common prefixes for display — but there's no actual folder object unless you explicitly create one. **This matters** because operations like "list objects under this folder" are really prefix-filtered LIST calls, not real directory traversal — this affects performance at scale (see Section 5).

---

## 2. How S3 Works Internally

*(High-level, reliable architectural understanding — not undocumented internals.)*

**How an object is stored:** when you PUT an object, S3 stores the data redundantly across multiple devices, in multiple Availability Zones within the region, automatically. You never manage this placement — S3 handles replication and physical distribution transparently.

**How S3 achieves massive scalability:** S3 is a distributed system — keys are hashed/distributed across many partitions internally, and requests are routed to the right storage nodes automatically. Because it's not a single filesystem tree, S3 can scale to trillions of objects without a "directory bottleneck."

**Request routing:** each request (PUT/GET/LIST/DELETE) is an independent HTTP call routed by AWS to available backend capacity — this is why S3 handles **massive request parallelism** well: many clients hitting different keys hit different backend partitions.

**Durability vs Availability (core internal reason):** S3 stores multiple copies of your data across facilities within a region — this redundancy is *why* it's extremely durable, and separately, that same distributed design (multiple nodes able to serve requests) is *why* it's highly available. These are related but distinct guarantees (see Section 8).

**What happens on PUT:** object data + metadata sent to S3 → durably written across multiple devices/AZs → S3 confirms success only after redundant storage is complete → object is then immediately readable (strong consistency, see Section 12).

**What happens on GET:** client requests a key → S3 routes to a node holding the data → returns bytes + metadata. No traversal of a directory tree — it's a direct key lookup.

**Why S3 doesn't behave like a traditional filesystem:**
- No true in-place modification — updating an object means **replacing it entirely** (or using specific APIs for append-like patterns, which are limited).
- No real rename — "renaming" is actually copy + delete under the hood, which is expensive for large objects/many objects.
- No real directories — "folders" are a UI/tooling convenience over flat keys.
- No file locks/POSIX semantics — this is why tools like Spark use special commit protocols when writing to S3 instead of just "moving files" like on HDFS.

**MEMORIZE:** S3 = flat key-value object store, not a filesystem. Every S3 quirk in Spark/data engineering traces back to this one fact.

---

## 3. S3 Data Engineering Architecture

**Why S3 became the foundation of modern data platforms:** cheap, durable, virtually unlimited storage, decoupled from compute — any engine (Spark, Redshift, Athena, Databricks) can read/write the same data without owning it. This is what makes the modern "storage separate from compute" architecture possible.

**S3 as a Data Lake — Medallion pattern:**
- **Bronze/Raw:** data landed as-is from source systems (minimal transformation) — the durable source of truth.
- **Silver/Processed:** cleaned, deduplicated, validated, joined data.
- **Gold/Curated:** business-ready aggregated datasets for BI/ML consumption.

**How S3 pairs with the ecosystem:**
| Service | Role with S3 |
|---|---|
| **Glue** | Catalogs S3 data (schema/metadata registry) + serverless ETL jobs reading/writing S3 |
| **Athena** | Runs SQL directly on S3 data (serverless query engine, uses Glue Catalog) |
| **Redshift** | Loads from S3 via `COPY`, or queries S3 directly via Spectrum |
| **EMR/Spark** | Distributed compute reading/writing S3 as its primary data source |
| **Databricks** | Lakehouse compute layer, reads/writes S3 (often via Delta Lake tables) |
| **Lambda** | Event-driven compute triggered by S3 object events (small/fast processing) |
| **Step Functions** | Orchestrates multi-step pipelines involving S3 stages (e.g. raw → processed → curated) |

**End-to-end example architecture:**
```
Source Systems (DBs, APIs, Kafka)
        │
        ↓
   S3 Raw/Bronze  ──────────────► Glue Catalog (schema registry)
        │
        ↓  Glue/EMR/Spark/Databricks (transform)
   S3 Silver
        │
        ↓  transform/aggregate
   S3 Gold  ──────► Redshift (COPY) / Athena (direct query) / BI tools
        │
        └──────► Databricks/Delta Lake for ML/AI
```
**Role of S3 here:** the single shared storage layer underneath every compute engine — nothing "owns" the data exclusively, which is the core value proposition versus siloed warehouse-only storage.

---

## 4. Object Naming & Data Organization

**Example layout:**
```
s3://company-data/sales/year=2026/month=08/day=15/part-0001.parquet
```

**Hive-style partitioning:** using `key=value` segments in the prefix (`year=2026/month=08/day=15/`) lets query engines (Athena, Spark, Redshift Spectrum) automatically recognize partition columns from the path itself, without scanning the data first.

**Why this matters — partition pruning:** if a query filters `WHERE year = 2026 AND month = 8`, the engine can skip listing/reading every other prefix entirely, because the partition values are encoded in the path. This is a huge performance/cost win — less data scanned = faster + cheaper.

**Good organization practices:**
- Partition by columns that are **commonly filtered** (date is the most common).
- Keep partition granularity reasonable — daily is common; hourly only if query patterns justify it (too fine = too many small partitions/files).
- Avoid partitioning by high-cardinality columns (e.g. `user_id`) — explodes into huge numbers of tiny prefixes (same problem as Hive/Spark disk partitioning elsewhere).

**Bad layouts and their problems:**
- Random/no structure (`s3://bucket/data1.csv`, `data2.csv`, ...) → every query must scan everything, no pruning possible.
- Over-partitioned (partitioning by `user_id` or `event_id`) → millions of tiny prefixes/files → small-file problem, slow listing.
- Under-partitioned (one giant flat prefix with huge files) → no pruning, every query reads everything.

**S3 prefix organization vs table partitioning (important distinction):**
- **S3 prefix organization** is just how keys are named — S3 itself has no concept of "partitions," it's purely a naming convention.
- **Table partitioning** (in Hive Metastore, Glue Catalog, or a table format like Delta/Iceberg) is **metadata** that maps partition values to those prefixes, letting query engines skip irrelevant data without listing S3 at all.
- **Why this distinction matters:** without a catalog, an engine has to LIST S3 prefixes to discover partitions (slow at scale); with a catalog, it looks up partition locations from metadata directly (fast).

---

## 5. S3 Performance

**Why S3 handles massive workloads:** it's a distributed system designed for high request parallelism — many clients can PUT/GET different keys simultaneously, each routed independently to available backend capacity. Performance scales by spreading requests across many keys/prefixes, not through one fast sequential path.

**Multipart upload:** large files split into parts uploaded in parallel (see Section 6) — improves throughput and resilience for big files.

**Large vs small objects — the core trade-off:**
| | Large files | Small files |
|---|---|---|
| Per-request overhead | Amortized over more data | High relative to data moved |
| Parallelism | Fewer objects = less request-level parallelism | More objects = more parallelism, but overload risk |
| Listing cost | Cheap (few keys) | Expensive at scale (millions of keys to list) |
| Spark/engine cost | Good — fewer, well-sized tasks | Bad — many tiny tasks, huge overhead (see below) |

**Why millions of tiny files become a problem:**
- Every file requires a separate GET request — request overhead (latency, connection setup) dominates over actual data transfer for tiny files.
- LIST operations to discover files become slow and expensive as file counts grow.
- Downstream engines (Spark) must create one task per file (roughly) — task scheduling overhead swamps actual processing time.

**The relationship: S3 file size → Spark partitions → tasks → performance:**
```
S3 files → Spark read → each file (or file-chunk) becomes ~1 input partition → 1 task per partition
```
- **Practical example:** 10,000 tiny 1MB files → Spark creates ~10,000 tasks, most of which finish almost instantly but each still pays scheduling/open-connection overhead → job is dominated by overhead, not real work.
- Versus: same data as 100 files of 100MB each → ~100 well-sized tasks → parallelism without overhead domination.
- **Rule of thumb:** target file sizes around 128MB–1GB (aligns with `spark.sql.files.maxPartitionBytes`, default 128MB) for balanced parallelism.

**Prefix considerations:** older S3 guidance recommended spreading high-throughput workloads across multiple prefixes to avoid hot-partitioning a single prefix; modern S3 auto-scales per-prefix throughput much better than in the past, but **extremely bursty single-prefix access at very high request rates** can still benefit from prefix diversity.

**MEMORIZE:** performance = request parallelism × good file sizing. Too few large files = poor parallelism; too many tiny files = overhead-dominated jobs. This single trade-off explains most S3 + Spark performance issues.

---

## 6. Multipart Upload

**What it is:** splitting a large object into multiple parts, uploading them independently (often in parallel), then telling S3 to assemble them into one final object.

**Why it exists:**
- Improves throughput for large files — parts upload in parallel instead of one long sequential stream.
- Improves resilience — if one part fails, only that part needs retrying, not the entire file.
- Required (not optional) above a certain object size (S3 enforces a max single-PUT size, and multipart is required beyond it).

**How it works (conceptual flow):**
```
Initiate multipart upload
      ↓
Split file into parts
      ↓
Upload parts in parallel (each part gets an ETag)
      ↓
Complete upload (S3 assembles parts using the part list)
      ↓
Object becomes available as a single object
```

**When to use it:** any large file (typically hundreds of MB and above) — most SDKs and tools (Spark, `aws s3 cp`) trigger this automatically above a size threshold; you rarely orchestrate it manually.

**Failure/retry behavior:** if the process fails midway, an **incomplete multipart upload** can linger, consuming storage silently until cleaned up — this is a real production cost issue (see Sections 10 & 19).

---

## 7. S3 Storage Classes

| Class | Access pattern | Retrieval | Relative cost | Typical DE use case | Avoid when |
|---|---|---|---|---|---|
| **Standard** | Frequent access | Immediate | Highest of "hot" tiers | Active bronze/silver/gold data being queried regularly | Data rarely accessed |
| **Intelligent-Tiering** | Unknown/changing access pattern | Immediate | Auto-optimizes | Data lakes where access patterns are unpredictable | Access pattern is well-known and stable (manual tiering may be cheaper) |
| **Standard-IA** | Infrequent, but need fast access when needed | Immediate | Lower storage, retrieval fee per GB | Monthly reports, older-but-occasionally-queried data | Data accessed frequently (retrieval fees add up) |
| **One Zone-IA** | Infrequent, recreatable data | Immediate | Cheaper than Standard-IA | Easily reproducible data (e.g. derived/reprocessable datasets) | Data that must survive an AZ failure |
| **Glacier Instant Retrieval** | Rarely accessed, but need instant access | Immediate | Cheap storage | Compliance/archive data occasionally needed instantly | Frequent access (expensive per-request) |
| **Glacier Flexible Retrieval** | Archival | Minutes–hours | Very cheap storage | Long-term backups, historical archives | Any time-sensitive access need |
| **Glacier Deep Archive** | Long-term archival | Hours | Cheapest | Compliance data kept for years, rarely if ever accessed | Anything needing timely access |

**Why this matters for a Data Engineer:** matching storage class to actual access pattern is one of the biggest S3 cost levers with almost no performance downside if done correctly (see Sections 10 & 18).

---

## 8. Durability, Availability & Replication

**Clear definitions (interview-critical distinction):**
- **Durability:** will the data survive over time without being lost/corrupted? S3 Standard is designed for extremely high durability (redundant copies across multiple AZs).
- **Availability:** can you access the data right now? Slightly different guarantee — a system can be highly durable (data isn't lost) but briefly unavailable (can't serve a request at this instant).
- **Backup:** a separate, deliberate copy of data (possibly in another account/region) protecting against *accidental deletion, corruption, or application bugs* — durability alone does NOT protect you from deleting your own data by mistake.
- **Replication (CRR/SRR):** automatic copying of objects to another bucket (same or different region) — protects against regional failure, supports compliance/latency needs, but is not a substitute for versioning/backup against accidental deletes (a delete can replicate too, depending on config).

**Why S3 is highly durable:** data is redundantly stored across multiple devices and Availability Zones automatically — no single hardware failure loses your data.

**Versioning:** keeps multiple versions of an object as it's overwritten/deleted — this is what actually protects against **accidental overwrite/delete**, which durability alone does not.

**Cross-Region Replication (CRR) vs Same-Region Replication (SRR):**
- **CRR:** replicate to another region — disaster recovery, latency for global users, regulatory requirements.
- **SRR:** replicate within the same region — separate accounts for compliance/isolation, aggregating logs, etc.

**MEMORIZE the distinction:** Durability ≠ Availability ≠ Backup ≠ Replication. Durability = data won't be lost by AWS's infrastructure. Availability = can you reach it right now. Backup = protection against *your own* mistakes. Replication = automatic copy elsewhere, mainly for resilience/compliance/latency — not a full backup strategy by itself.

---

## 9. Versioning & Data Protection

**S3 Versioning:** once enabled, every PUT to the same key creates a new version instead of overwriting — old versions remain retrievable.

**Delete markers:** a "delete" on a versioned bucket doesn't actually erase data — it adds a delete marker as the new "current" version, hiding the object from normal listing while prior versions remain recoverable.

**Accidental deletion recovery:** because of delete markers, you can simply remove the delete marker to "undelete" an object — this is the practical safety net versioning provides.

**Versioning + lifecycle:** without lifecycle rules, old versions accumulate forever and quietly increase storage cost — pair versioning with lifecycle rules that expire old versions after N days.

**Object Lock / WORM (Write Once Read Many):** prevents an object (or version) from being deleted or overwritten for a defined retention period — even by an admin, until retention expires (or indefinitely in "compliance mode"). Used for regulatory/legal-hold requirements (e.g. financial records that must be immutable for 7 years).

**When these matter in production:** any data lake holding data subject to accidental-deletion risk (careless job overwriting a table) or compliance requirements (immutable audit/financial records) — not needed for freely reproducible/transient data.

---

## 10. Lifecycle Management

**What it is:** automated rules that transition objects between storage classes or expire them based on age, without manual intervention.

**Common Data Lake lifecycle strategy:**
```
Day 0–30:    Standard (hot, actively queried)
Day 30–90:   Intelligent-Tiering / Standard-IA (less frequent access)
Day 90+:     Glacier (historical, rarely queried)
Years+:      Glacier Deep Archive (compliance retention only)
```

**Why this makes sense:** most data engineering data follows a clear "hot → cold" access pattern — recent data is queried constantly, older data is rarely touched but must be retained for compliance/history. Lifecycle rules capture the cost savings of that pattern automatically, without engineers manually moving data.

**Also commonly used for:** expiring incomplete multipart uploads (see Section 6/19) and old object versions (see Section 9) — both silent cost drains if left unmanaged.

---

## 11. Security (concepts a Senior Data Engineer needs)

- **IAM:** identities (users/roles) and policies defining who can do what — the foundation of all AWS access control.
- **IAM policies (identity-based):** attached to a user/role — "this role can read/write this bucket."
- **Bucket policies (resource-based):** attached to the bucket itself — "this bucket can be accessed by these principals." Useful for cross-account access.
- **Identity-based vs resource-based:** functionally similar outcome, different attachment point — resource-based policies are essential when granting access *to* your bucket *from* another AWS account.
- **S3 Block Public Access:** an account/bucket-level safety switch that overrides any policy attempting to make data public — should be on by default for virtually all data engineering buckets; a very common real-world misconfiguration/incident source when accidentally disabled.
- **Encryption at rest:**
  - **SSE-S3:** S3-managed keys — simplest, no extra config.
  - **SSE-KMS:** AWS KMS-managed keys — adds audit trail (CloudTrail) and fine-grained key policies, standard for regulated/enterprise data.
  - **Client-side encryption:** data encrypted before it ever reaches S3 — used when you don't trust the storage layer with plaintext at all (rare, high-sensitivity cases).
- **Encryption in transit:** enforced via HTTPS/TLS — should be required (deny non-TLS requests) via bucket policy in production.
- **Least privilege:** grant only the specific prefixes/actions a role actually needs — avoid broad `s3:*` on `*` policies, a very common audit finding.
- **VPC Endpoint (Gateway Endpoint for S3):** lets resources in a VPC reach S3 without traversing the public internet — reduces exposure and can reduce data transfer cost.
- **Access logging / CloudTrail (conceptual):** records who accessed what and when — essential for auditing and incident investigation, not for real-time blocking.

**Common security mistakes:** publicly accessible buckets (Block Public Access disabled), overly broad IAM policies, unencrypted sensitive data, no logging enabled until after an incident happens.

---

## 12. Consistency & Reliability

**Strong read-after-write consistency (current S3 behavior):** as of the modern S3 consistency model, S3 provides **strong consistency** for all operations — PUT, GET, LIST, and DELETE. This means: after a successful PUT, an immediate GET or LIST will reflect that write. There's no "eventually consistent" window to worry about anymore (this was a real historical limitation in older S3, but is no longer the case).

**Why this matters for Data Engineering:** pipelines that write a file and then immediately expect a downstream process (Spark job, Athena query, Lambda trigger) to see it reliably work correctly — no need for artificial delays or retry-until-visible workarounds that used to be common patterns. Spark, Athena, Redshift Spectrum, and Databricks all rely on this guarantee for correctness in multi-stage pipelines (write in stage 1 → immediately readable in stage 2).

**MEMORIZE:** strong consistency removed a whole historical class of "phantom missing file" bugs in S3-based pipelines — know this is current behavior, not the old eventual-consistency model, for interviews.

---

## 13. S3 + Spark / PySpark

**How Spark reads from S3:** Spark lists objects under the given path/prefix, splits large files into logical chunks (respecting `spark.sql.files.maxPartitionBytes`, default 128MB) to form **input partitions** — each partition becomes one task.

**How Spark writes to S3:** each output partition (post-shuffle, if any) is written as one or more output files under the target path — this is why the number of Spark partitions at write time directly controls the number of output files (see Section 5's small-file relationship).

**The full flow:**
```
S3 Files
   ↓  list + split by maxPartitionBytes
Input Partitions (1 partition ≈ 1 task)
   ↓
Transformations (narrow = no data movement; wide = shuffle)
   ↓
Shuffle (redistributes data by key, if wide transformation used)
   ↓
Output Partitions (post-shuffle partition count = spark.sql.shuffle.partitions unless AQE coalesces)
   ↓
S3 Files (one/more files per output partition)
```

**Where performance problems occur:**
- **Too many small input files** → too many tiny tasks → overhead-dominated job (Section 5).
- **Too few output partitions before write** → very large output files, possible memory pressure per task.
- **Too many output partitions before write** → small-file problem created *by the job itself* for the next consumer.
- **Unnecessary shuffle** → expensive network/disk cost when a narrower transformation would've sufficed.

**Controlling output file count:**
```python
df.coalesce(20).write.parquet("s3://bucket/gold/")   # cheap, no full shuffle, decrease-only
df.repartition(50, "country").write.parquet("s3://bucket/gold/")  # full shuffle, rebalances, can increase/decrease
```

**Predicate pushdown & partition pruning (with S3 + Parquet):**
- **Partition pruning:** if the S3 path is Hive-style partitioned (`year=2026/month=08/`) and the query filters on those columns, Spark skips listing/reading irrelevant prefixes entirely.
- **Predicate pushdown:** within a Parquet file, filters are pushed down to the file format itself — Parquet's column statistics let Spark skip entire row groups that can't match the filter, without decompressing/reading them.

**MEMORIZE:** file size at rest on S3 directly determines Spark's input partition count, which determines task count, which determines whether the job is well-parallelized or overhead-dominated. This single chain explains most S3+Spark performance tuning.

---

## 14. S3 + Parquet + Data Lake

**Why Parquet (not CSV/JSON) is the standard data lake format:**

| | CSV | JSON | Parquet | ORC |
|---|---|---|---|---|
| Storage layout | Row-based, text | Row-based, text (verbose) | **Columnar**, binary | Columnar, binary |
| Compression | Poor (generic text compression) | Poor | **Excellent** (column-wise, type-aware) | Excellent |
| Schema | None built-in | Semi-structured, no enforced schema | **Embedded schema** | Embedded schema |
| Column pruning | Impossible (must read whole row) | Impossible | **Yes** — read only needed columns | Yes |
| Predicate pushdown | No | No | **Yes** — column stats per row group | Yes |
| Read performance (analytics) | Slow at scale | Slow at scale | **Fast** | Fast (esp. Hive ecosystem) |
| Cost impact | Expensive (more storage + more scanned data) | Expensive | **Cheap** (small size, less scanned) | Cheap |

**Why it matters:** columnar format + embedded schema + compression + pushdown together mean analytical queries touch a fraction of the bytes CSV/JSON would require — directly reducing both query time and scan-based cost (e.g. Athena/Spectrum charge per byte scanned).

**Parquet vs ORC (brief):** functionally similar; Parquet has broader ecosystem support (Spark's default, wide tool compatibility) and is the more common default choice; ORC is more common in Hive-centric shops. Not usually a major interview differentiator — know Parquet deeply, know ORC exists.

**How this leads to the Lakehouse:** plain Parquet files on S3 give great read performance but no transactional guarantees (no ACID, no safe concurrent writes, no easy row-level update/delete). Table formats — **Delta Lake, Apache Iceberg, Apache Hudi** — add a transaction log/metadata layer on top of Parquet files to solve exactly that gap, turning "a folder of Parquet files" into "a reliable table." This is the S3 Data Lake → Lakehouse evolution.

---

## 15. S3 and Lakehouse

**The relationship, stated plainly:**
```
S3            = raw object storage (durable, cheap, flat key-value)
Data Lake     = the architecture of storing diverse data on S3 (schema-on-read)
Delta/Iceberg/Hudi = table format layer on top of S3 — adds ACID, versioning, schema enforcement
Databricks    = compute + platform layer that uses Delta Lake tables on S3
Lakehouse     = S3 (storage) + Delta/Iceberg/Hudi (table layer) + Spark-based compute (engine)
```

**Why S3 alone does NOT provide transactional table capabilities:**
- No atomic multi-file commit — writing many Parquet files that together represent "one table update" isn't atomic on S3 by itself; a failed job can leave a table in a half-written, inconsistent state.
- No native versioning **at the table level** (S3 object versioning is per-object, not "give me the table as of yesterday").
- No schema enforcement/evolution tracking.
- No safe concurrent writes to the same logical table (two jobs writing simultaneously can conflict/corrupt without a coordinating layer).

**What Delta/Iceberg/Hudi add:** an ordered, atomic transaction log (or equivalent metadata) recording every change to the table — this is what turns a "folder of files" into "a table" with ACID guarantees, time travel, and safe concurrent access — all still physically stored as files on plain S3.

**MEMORIZE (interview one-liner):** *"S3 is just storage. Delta/Iceberg/Hudi add the table layer — transaction log, ACID, schema enforcement — on top of that storage. Without one of these, a 'data lake table' is really just a folder of files with no safety guarantees."*

---

## 16. S3 Event-Driven Architecture

**Core concepts:**
- **S3 Event Notifications:** S3 can emit an event when an object is created/deleted, etc.
- **SQS:** a queue that can receive S3 events — decouples the event from immediate processing.
- **SNS:** pub/sub — can fan out one S3 event to multiple subscribers.
- **Lambda:** can be triggered directly by S3 events for lightweight, fast processing.
- **EventBridge:** more advanced event routing/filtering across many AWS services, not just S3.
- **Step Functions:** orchestrates multi-step workflows, often triggered by one of the above.

**Practical example:**
```
File uploaded to S3
        ↓
S3 Event Notification
        ↓
SQS queue
        ↓
Lambda / Spark job (consumes from queue)
        ↓
Data Processing
```

**Why SQS is often preferred over directly triggering processing:**
- **Buffering/decoupling:** if 10,000 files land at once, direct Lambda triggers could overwhelm downstream systems; SQS smooths the load, letting consumers process at a sustainable rate.
- **Retry/durability:** failed processing can be retried from the queue rather than losing the event.
- **Backpressure control:** you control concurrency of the consumer independently of the burst rate of uploads.

**MEMORIZE:** direct S3→Lambda triggers are simple but fragile under bursty load; S3→SQS→processing is the more production-resilient pattern.

---

## 17. S3 Data Loading Patterns

**Batch:**
```
Source (DB/API/files) → S3 → Spark/Glue (transform) → Data Warehouse (Redshift/BI)
```
Typical for scheduled nightly/hourly ETL.

**Streaming/micro-batch:**
```
Kafka/Kinesis → S3 (landed in small time-windowed batches) → Processing (Spark Structured Streaming/Glue)
```
S3 acts as a durable, replayable landing zone even for streaming sources — decouples ingestion rate from processing rate.

**Database ingestion (CDC pattern):**
```
RDS (source DB) → DMS (Database Migration Service, captures changes) → S3 (raw CDC files) → Spark/Glue/Redshift
```
Common for replicating OLTP data into the analytics platform without hitting the production DB with heavy queries.

**Why S3 is the go-to intermediate/staging layer:** decouples producers from consumers (each can run on its own schedule/pace), gives a durable replay point if downstream processing fails, and is cheap enough to hold raw data indefinitely as an audit trail — this is the same reasoning behind the Bronze layer in Section 3.

---

## 18. Cost Optimization

**Main S3 cost components:**
- **Storage** — GB/month, varies by storage class.
- **Requests** — PUT/GET/LIST/DELETE all cost per-request (small at individual scale, adds up fast with millions of tiny files).
- **Data transfer** — especially cross-region or out-to-internet transfer; same-region access to compute (e.g. EC2/EMR in the same region) is typically free or cheap.
- **Retrieval** — Glacier/IA classes charge retrieval fees; frequent access to "cold" tiers can backfire cost-wise.
- **Lifecycle transitions** — each transition itself has a small cost, but usually far outweighed by storage savings over time.

**How to reduce cost without sacrificing performance:**
1. **Right-size storage class** by actual access pattern (Section 7) — the single biggest lever.
2. **Use Parquet + compression** instead of CSV/JSON — smaller storage AND less scanned data on query (Section 14).
3. **Fix the small-file problem** — fewer, well-sized files means fewer billed requests and less listing overhead (Sections 5 & 19).
4. **Partition well** — reduces bytes scanned per query on Athena/Spectrum/Spark, which directly reduces both time and (for scan-based pricing engines) cost.
5. **Lifecycle old data** to IA/Glacier automatically instead of leaving everything on Standard forever.
6. **Clean up incomplete multipart uploads** and old object versions via lifecycle rules — a very common silent cost leak.

**MEMORIZE:** the cost optimizations and the performance optimizations are almost entirely **the same actions** (good file sizes, good partitioning, Parquet, right storage class) — this is a useful thing to say explicitly in an interview.

---

## 19. Common Production Problems

| Problem | Why it happens | How to identify | Solution |
|---|---|---|---|
| Too many small files | Frequent small writes (streaming micro-batches, over-partitioned writes) | Slow Spark jobs with many tiny tasks; high LIST/GET request cost | Compact files (coalesce before write, periodic compaction job / `OPTIMIZE` in Delta) |
| Very large files | No splitting on write, single-writer bulk export | Few tasks, some very long-running, possible memory pressure | Repartition before write to balance file sizes |
| Poor partition layout | Wrong/no partition column chosen, high-cardinality partitioning | Queries scan far more data than expected; huge number of prefixes | Redesign partitioning around actual filter columns, moderate cardinality |
| Excessive LIST operations | Deeply nested prefixes, no catalog, engine must discover files by listing | Slow query start time, high request cost | Use a catalog (Glue/Hive Metastore) or table format (Delta/Iceberg) instead of raw listing |
| Expensive cross-region transfer | Compute in one region, data in another | Unexpected data transfer charges | Co-locate compute and storage in the same region |
| Wrong storage class | Frequently accessed data left in Glacier/IA, or rarely accessed data left in Standard | High retrieval fees, or unnecessarily high storage bill | Right-size storage class to true access pattern; consider Intelligent-Tiering if pattern is unclear |
| Unexpected retrieval cost | Repeated access to Glacier/IA-tier data | Cost spikes tied to specific archived prefixes | Move frequently-needed data back to a warmer tier |
| Access denied | Overly restrictive/misconfigured IAM or bucket policy, Block Public Access conflicts | 403 errors in logs/CloudTrail | Review policy least-privilege scope, check resource vs identity policy interaction |
| Slow data processing | Small-file problem, poor partitioning, unnecessary shuffles | Spark UI: many tiny tasks or few huge tasks, long stage times | Fix file sizing/partitioning (Sections 5 & 13) |
| Duplicate files | Retried failed jobs writing without cleanup, no idempotent write pattern | Row counts higher than expected, downstream dedup logic needed | Use idempotent write patterns (overwrite partition, or table-format `MERGE`) |
| Incomplete multipart uploads | Failed/aborted large uploads not cleaned up | Storage cost higher than object listing would suggest | Lifecycle rule to auto-abort incomplete multipart uploads after N days |
| Accidental deletion | No versioning, human/job error | Missing data, no recovery path | Enable versioning ahead of time; without it, recovery is often impossible |
| Poor lifecycle configuration | No rules, or rules misaligned with actual access pattern | Old/cold data still on expensive Standard tier | Design lifecycle rules matching real access patterns (Section 10) |

---

## 20. Senior Data Engineer Interview Section

**Architecture & Internals**
1. *What is S3 and why is it not a filesystem?* → Object storage, flat key-value namespace; no true directories, no in-place edits, no POSIX semantics.
2. *What's the difference between a bucket, prefix, and object?* → Bucket = container; object = data+metadata identified by a key; prefix = the string before the object name, used for filtering/organization, not a real folder.
3. *Why is a prefix not a real folder?* → S3 has no directory tree internally — prefixes are just substrings of flat keys, folders are a UI simulation.
4. *What happens internally on a PUT?* → Data is durably written redundantly across multiple devices/AZs before S3 confirms success; then immediately readable (strong consistency).
5. *Explain durability vs availability.* → Durability = data won't be lost; availability = can you access it right now — related but distinct guarantees.

**Performance**
6. *Why is the small-file problem bad for S3 + Spark?* → Each file needs a separate request; many tiny files create many tiny tasks, and per-task/request overhead dominates actual work.
7. *What file size should I target on S3 for analytics workloads?* → ~128MB–1GB, aligned with Spark's `maxPartitionBytes` default, to balance parallelism against overhead.
8. *How does file size relate to Spark task count?* → Roughly 1 input partition ≈ 1 task; file size and count directly determine partition count on read.
9. *What is multipart upload and why does it help?* → Splits large uploads into parallel parts, improving throughput and making failures cheaper to retry.

**Data Lake / Parquet**
10. *Why Parquet over CSV/JSON for a data lake?* → Columnar storage enables column pruning and predicate pushdown, plus far better compression — less data scanned = faster and cheaper.
11. *What is partition pruning and how does it work on S3?* → If data is Hive-style partitioned in the S3 path and a catalog/engine recognizes it, queries skip irrelevant prefixes entirely instead of scanning everything.
12. *Difference between S3 prefix organization and table partitioning?* → Prefix organization is just key naming; table partitioning is metadata (in a catalog/table format) mapping partition values to those keys, enabling pruning without listing S3.

**Lakehouse**
13. *Why can't S3 alone provide ACID transactions like Delta Lake?* → No atomic multi-file commit, no table-level versioning/schema enforcement, no safe concurrent-write coordination — S3 is just storage, not a transactional table engine.
14. *What do Delta/Iceberg/Hudi actually add on top of S3?* → An ordered transaction log/metadata layer giving ACID, time travel, schema enforcement, and safe concurrent writes over plain Parquet files.
15. *How does S3 relate to a Lakehouse architecture?* → S3 is the storage layer; the table format (Delta/Iceberg/Hudi) is the reliability layer; Spark/Databricks is the compute layer — together they form the Lakehouse.

**Security**
16. *Identity-based vs resource-based policy?* → Identity-based attaches to a user/role ("this role can access this bucket"); resource-based attaches to the bucket itself ("this bucket allows these principals") — resource-based is key for cross-account access.
17. *What's Block Public Access and why does it matter?* → An account/bucket-level override preventing accidental public exposure regardless of policy misconfiguration — a critical safety net; disabling it is a common real-world incident cause.
18. *SSE-S3 vs SSE-KMS?* → Both encrypt at rest; SSE-KMS adds customer-managed keys, audit trail via CloudTrail, and fine-grained key policies — preferred for regulated data.

**Consistency**
19. *What is S3's consistency model today?* → Strong read-after-write consistency for PUT/GET/LIST/DELETE — a write is immediately visible to subsequent reads, no eventual-consistency window (unlike older S3 behavior).
20. *Why does this matter for pipelines?* → Multi-stage pipelines (write then immediately read/trigger downstream) work correctly without artificial delays or retry-until-visible workarounds.

**Cost & Production**
21. *How would you reduce S3 cost without hurting performance?* → Right storage class per access pattern, Parquet+compression, fix small files, good partitioning, lifecycle old data — largely the same actions that improve performance.
22. *What causes "access denied" errors most often?* → Overly restrictive or misconfigured IAM/bucket policy, or conflict with Block Public Access settings.
23. *Why do incomplete multipart uploads matter?* → They silently consume storage and cost until explicitly cleaned up via lifecycle rules.

**Scenario-based**
24. *You have 10 billion records in S3 and Spark processing is slow — how do you investigate?* → Check file count/size distribution (small-file problem?), check partition layout (pruning working? over/under partitioned?), check Spark UI for task count/duration skew, check for unnecessary shuffles, check format (Parquet vs CSV/JSON).
25. *Your data lake has millions of small files — how do you fix it?* → Run a compaction job (`coalesce`/`repartition` before rewrite, or `OPTIMIZE` if using Delta), fix the upstream write pattern causing fragmentation (e.g. batch micro-batch writes less frequently), add lifecycle/cleanup for future prevention.
26. *Why choose S3+Parquet over storing raw CSV?* → Columnar compression and pushdown drastically cut scanned bytes and storage size, directly reducing both query time and (for scan-based pricing) cost.
27. *Design a batch pipeline moving operational DB data into a queryable data lake.* → DB → DMS/extract → S3 raw (bronze) → Glue/Spark transform → S3 curated (gold, Parquet, partitioned) → cataloged (Glue) → queried via Athena/Redshift Spectrum/Databricks.
28. *How would you protect a critical data lake table against accidental deletion?* → Enable versioning ahead of time (recovery via delete-marker removal), consider Object Lock for compliance-critical data, and rely on a table format (Delta/Iceberg) for atomic, recoverable writes rather than raw file overwrites.

---

## FINAL REVISION SECTION

### S3 in 5 Minutes
S3 is flat object storage — buckets hold objects identified by keys; prefixes simulate folders but aren't real directories. It's the default foundation of modern data platforms because it decouples cheap, durable, virtually unlimited storage from compute, letting Spark, Redshift, Athena, and Databricks all read/write the same data. Performance and cost both hinge on the same few things: right file sizes (~128MB–1GB), good partition layout matching query filters, using Parquet instead of CSV/JSON, and matching storage class to real access patterns. S3 alone provides no ACID/table guarantees — Delta Lake/Iceberg/Hudi add that layer on top, turning a folder of files into a Lakehouse table.

### S3 Architecture Cheat Sheet
- S3 = flat key-value object store, not a filesystem — no true folders, no in-place edits.
- Bucket → Prefix (naming convention) → Object (key + data + metadata).
- Strong read-after-write consistency for all operations (PUT/GET/LIST/DELETE).
- Durability (data won't be lost) ≠ Availability (can access it now) ≠ Backup (protects against your mistakes) ≠ Replication (auto-copy elsewhere).
- S3 is the shared storage layer under Glue, Athena, Redshift, EMR/Spark, Databricks, Lambda.

### S3 Performance Cheat Sheet
- Target file size ~128MB–1GB for analytics workloads.
- Too many small files → overhead-dominated jobs (many tiny tasks/requests).
- Too few large files → poor parallelism, memory pressure.
- File size on S3 directly drives Spark input partition count → task count.
- Use `coalesce()` to cheaply shrink output file count; `repartition()` to rebalance/increase (full shuffle).
- Partition pruning + Parquet predicate pushdown = less data scanned = faster + cheaper.

### S3 Data Engineering Cheat Sheet
- S3 (storage) + Parquet (efficient columnar format) + partitioning (pruning) = the analytics-ready data lake baseline.
- S3 alone has no ACID — Delta Lake/Iceberg/Hudi add the transaction log/table layer.
- Lakehouse = S3 (storage) + table format (reliability) + Spark/Databricks (compute).
- Medallion pattern: Bronze (raw) → Silver (cleaned) → Gold (curated) — all just organized S3 prefixes/tables.
- S3 is the standard staging/intermediate layer in batch, streaming, and CDC ingestion patterns.

### S3 Security Cheat Sheet
- IAM (identity-based) grants what a role can do; bucket policy (resource-based) grants what's allowed on the bucket — resource-based needed for cross-account.
- Block Public Access should be on by default — overrides any accidental public-exposing policy.
- SSE-S3 (simple) vs SSE-KMS (audit trail, key control) for encryption at rest; enforce TLS for in-transit.
- Least privilege — scope IAM to specific prefixes/actions, not `s3:*` on `*`.

### S3 Storage Class Cheat Sheet
- **Standard** — hot, frequently accessed.
- **Intelligent-Tiering** — unpredictable access pattern, auto-optimized.
- **Standard-IA** — infrequent but needs instant access when queried.
- **One Zone-IA** — infrequent + reproducible data only (no multi-AZ redundancy).
- **Glacier Instant Retrieval** — rarely accessed, but instant when needed.
- **Glacier Flexible Retrieval** — archival, minutes–hours retrieval.
- **Glacier Deep Archive** — long-term compliance archive, hours retrieval, cheapest.

### S3 CLI Cheat Sheet

```bash
# ── Bucket Operations ──────────────────────────────────────────
aws s3 mb s3://my-bucket                         # Create bucket
aws s3 rb s3://my-bucket --force                 # Delete bucket + contents
aws s3 ls                                        # List all buckets
aws s3 ls s3://my-bucket/                        # List bucket contents
aws s3 ls s3://my-bucket/ --recursive            # List recursively

# ── Object Operations ──────────────────────────────────────────
aws s3 cp file.txt s3://my-bucket/               # Upload file
aws s3 cp s3://my-bucket/file.txt ./             # Download file
aws s3 cp s3://bucket-a/k s3://bucket-b/k        # Copy between buckets
aws s3 mv s3://my-bucket/old.txt s3://my-bucket/new.txt   # Move/rename
aws s3 rm s3://my-bucket/file.txt                # Delete object
aws s3 rm s3://my-bucket/ --recursive            # Delete all objects

# ── Sync ───────────────────────────────────────────────────────
aws s3 sync ./local-folder s3://my-bucket/prefix/    # Upload folder
aws s3 sync s3://my-bucket/prefix/ ./local-folder    # Download folder
aws s3 sync s3://bucket-a/ s3://bucket-b/            # Sync buckets
aws s3 sync ./folder s3://my-bucket/ --delete        # Delete files not in source

# ── With Options ───────────────────────────────────────────────
aws s3 cp file.csv s3://my-bucket/ \
  --storage-class STANDARD_IA \
  --sse aws:kms \
  --metadata "author=data-team,version=1.0"

# ── API Level ──────────────────────────────────────────────────
aws s3api head-object --bucket my-bucket --key file.csv   # Get object metadata
aws s3api get-object --bucket my-bucket --key file.csv out.csv
aws s3api list-objects-v2 --bucket my-bucket --prefix logs/
aws s3api get-bucket-location --bucket my-bucket
```

### S3 Pricing Model Cheat Sheet

**Pricing has four main dimensions:**

| Dimension                | What You Pay For                                          |
|--------------------------|-----------------------------------------------------------|
| **Storage**              | Per GB per month (varies by storage class)                |
| **Requests**             | Per 1,000 PUT/GET/LIST/DELETE requests                    |
| **Data Transfer Out**    | Per GB transferred out to internet or other regions       |
| **Management Features**  | Inventory, Analytics, S3 Lens, Object Tagging, Batch Ops  |

**Data Transfer Rules:**

| Transfer Type                            | Cost   |
|------------------------------------------|--------|
| Upload to S3 (data-in)                   | Free   |
| S3 to EC2/Lambda in **same region**      | Free   |
| S3 to EC2/Lambda in **different region** | Paid   |
| S3 to Internet                           | Paid   |
| S3 → CloudFront (all regions)            | Free   |
| CloudFront → Internet                    | Charged to CloudFront (cheaper) |

**Cost Optimization Tips:**

1. Use **Intelligent-Tiering** for unpredictable workloads — auto-optimizes storage class.
2. Set **Lifecycle rules** to move old data to Glacier — massive savings for cold data.
3. Enable **Bucket Key** for KMS-encrypted buckets — reduces KMS API costs ~99%.
4. Delete **incomplete multipart uploads** via lifecycle rules — silent storage cost leak if left.
5. Use **CloudFront** in front of S3 for frequently downloaded public content — cheaper egress.
6. Use **S3 Select** instead of downloading entire objects — reduce data scanned and transfer.
7. Delete **old versions** and **delete markers** if using versioning — versioning multiplies storage cost.

### Top 25 Things a Senior Data Engineer Must Remember
1. S3 = flat object store, not a filesystem — no true folders, no in-place edits.
2. A prefix is just a naming convention, not a physical directory.
3. Object storage vs file storage vs block storage — know the distinction cold.
4. S3 achieves scale via distributed, independently-routed requests — parallelism across keys is the performance model.
5. PUT confirms only after redundant durable storage; GET is then immediately strongly consistent.
6. S3 provides strong read-after-write consistency today — no eventual consistency window.
7. Durability ≠ availability ≠ backup ≠ replication — four distinct guarantees.
8. Versioning + delete markers are what actually protect against accidental deletion, not durability alone.
9. Object Lock/WORM enforces true immutability for compliance data.
10. Target file size ~128MB–1GB for Spark/analytics workloads.
11. Millions of tiny files cause overhead-dominated jobs and expensive LIST operations.
12. File size on S3 directly drives Spark input partition count and task count.
13. `coalesce()` = cheap, decrease-only, no full shuffle; `repartition()` = full shuffle, can increase/rebalance.
14. Hive-style partitioning (`key=value` in path) enables partition pruning by query engines.
15. A catalog (Glue/Hive Metastore) avoids expensive S3 LIST calls by storing partition metadata directly.
16. Parquet beats CSV/JSON via columnar storage, compression, column pruning, and predicate pushdown.
17. S3 alone has no ACID — Delta Lake/Iceberg/Hudi add the transactional table layer on top.
18. Lakehouse = S3 (storage) + table format (reliability) + Spark/Databricks (compute).
19. S3 is the standard staging layer for batch, streaming, and CDC ingestion patterns — decouples producers from consumers.
20. S3→SQS→processing is more resilient under bursty load than direct S3→Lambda triggers.
21. Storage class should match real access pattern — the single biggest S3 cost lever.
22. Cost and performance optimizations on S3 largely overlap (good file size, Parquet, partitioning, right tier).
23. Incomplete multipart uploads and old object versions are common silent cost leaks — clean up via lifecycle rules.
24. Block Public Access and least-privilege IAM are the two most common real-world security safeguards.
25. Cross-region data transfer and repeated Glacier/IA retrieval are common sources of unexpected cost.

### Common Interview Traps
- Calling S3 "just a bucket with folders" — it's flat key-value storage; folders are simulated.
- Saying S3 is "eventually consistent" — outdated; modern S3 is strongly consistent for all operations.
- Confusing durability with availability, or replication with backup — these are four separate concepts.
- Claiming S3 provides ACID transactions on its own — it doesn't; that's what Delta/Iceberg/Hudi add.
- Assuming bigger files are always better — very large files also hurt parallelism; the goal is *well-sized*, not maximal.
- Forgetting that partition pruning requires either a catalog or Hive-style path recognition — it isn't automatic just because data is "organized in folders."
- Treating storage class selection as a one-time decision — access patterns change, and lifecycle rules/Intelligent-Tiering exist specifically to handle that.
