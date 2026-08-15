# Amazon Redshift 

---

## 1. Fundamentals

**What it is:** A cloud data warehouse — an OLAP, MPP, columnar SQL engine built to scan and aggregate huge structured datasets fast.

**OLAP vs OLTP (one line):** OLTP = many small transactional writes (normalized, row-store). OLAP = large analytical reads/aggregations (denormalized, column-store). Redshift is OLAP — never treat it like an OLTP database (no row-by-row inserts).

**Why companies choose it:**
- Fast SQL analytics over billions of rows without managing infra.
- Mature ecosystem (BI tools, JDBC/ODBC, tight AWS integration — S3, Glue, IAM).
- Predictable performance for structured, repeatable reporting workloads.

**Problems it solves:** running heavy aggregate queries that would kill an OLTP database; centralizing structured data from many sources into one governed, queryable place; giving BI/analysts fast SQL without needing Spark/data-engineering skills.

**Typical use cases:** enterprise BI/reporting, finance/sales dashboards, curated data marts, scheduled batch analytics.

**When it's a bad choice:**
- Unstructured/semi-structured raw data at scale (images, JSON blobs, ML feature stores) → data lake fits better.
- High-frequency small transactional writes → use an OLTP DB.
- Highly variable, bursty, exploratory workloads with unpredictable scaling needs → lakehouse/Spark often more flexible and cost-efficient.

**MEMORIZE:** Redshift = OLAP + MPP + columnar. That's the whole engine philosophy — every other feature exists to serve one of these three.

---

## 2. Architecture & Internals

**Cluster / workgroup:** the Redshift deployment unit. Traditional = provisioned cluster (Leader Node + Compute Nodes you size). Modern = **Serverless** (a "workgroup" that auto-scales — you don't manage nodes directly, but the internal execution model is the same).

**Leader Node:** receives SQL, parses it, builds a query plan, distributes plan pieces to compute nodes, merges/returns the final result. Does no heavy data scanning itself.

**Compute Nodes:** do the actual data storage and parallel processing. Each is split into **Slices** — a slice is a unit of CPU + memory + storage that processes one portion of the data independently.

```
Query → Leader Node (parse/plan) → Compute Nodes → Slices (parallel work) → Leader Node (merge) → Result
```

**Why slices matter:** more slices = more parallelism. A table's rows are spread across all slices (per its distribution style), so each slice only scans its own portion — this is the root of MPP speed.

**Columnar storage:** data stored column-by-column, not row-by-row. `SELECT AVG(salary)` only reads the `salary` column blocks — not the whole row. **Why it's fast:** far less disk I/O for aggregate queries that only touch a few columns out of many.

**Compression:** columnar data compresses very well (similar values stored together). Redshift auto-picks per-column encodings. **Why it's fast:** smaller data on disk = less I/O = faster scans; also more data fits in memory/cache.

**MPP (Massively Parallel Processing):** the query is split into pieces that run **simultaneously** across all slices, instead of one machine scanning everything sequentially. **Why it's fast:** total scan time ≈ (data size / number of slices), not the full data size.

### End-to-end example: how a query actually flows

```sql
SELECT c.region, SUM(o.amount)
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region;
```

1. **Leader Node** parses SQL, checks statistics, builds a distributed execution plan.
2. Plan is pushed to all **Compute Nodes / Slices**.
3. Each slice scans **only its own local columnar data** for `orders` and `customers` (columnar + compression = fast local scan).
4. **Join step:** if `orders` and `customers` are distributed on `customer_id`, matching rows already sit on the same slice → **local join, no network movement**. If not, Redshift must **redistribute** rows across the network first (expensive — see Section 3).
5. **Aggregation (`SUM`, `GROUP BY`):** each slice computes a partial aggregate locally.
6. **Leader Node** merges the partial aggregates from all slices into the final result and returns it.

**UNDERSTAND (don't just memorize):** almost every Redshift performance topic (distribution, sort keys, skew) is really about minimizing step 4 (data movement) and maximizing how much of step 3 each slice can do independently.

---

## 3. Data Distribution

**Why it matters:** distribution decides which slice each row lives on. If a join's rows aren't already co-located, Redshift must move data across the network at query time — this is usually the single biggest cause of slow Redshift queries.

**DISTSTYLE options:**

| Style | Behavior | Choose when |
|---|---|---|
| **KEY** (`DISTKEY(col)`) | Rows hashed by one column, all rows with the same value land on the same slice | Large fact tables that are frequently joined on that column — co-locates the join, avoids redistribution |
| **ALL** | Full copy of the table on every node | Small, slowly-changing dimension tables (e.g. `country`, `date`) joined often — the "small side" never needs to move |
| **EVEN** | Rows spread round-robin, ignoring content | No dominant join key, or table rarely joined — maximizes even parallelism for scans |
| **AUTO** | Redshift picks (usually ALL for small tables, EVEN/KEY as it grows) | Default when unsure — safe starting point, Redshift adapts |

**Broadcast vs redistribution (what actually happens in a join):**
- **Co-located (best):** both sides distributed on the join key → no movement, pure local join.
- **Broadcast:** the small side (e.g. `ALL`-distributed dimension) is already everywhere → no movement needed either.
- **Redistribution (worst):** neither side is aligned on the join key → Redshift shuffles one or both tables across the network before joining. This is the expensive case — shows as `DS_DIST_*` operators in `EXPLAIN`.

**Data skew:** if the `DISTKEY` has very uneven value frequency (e.g. one `customer_id` = 40% of rows), that slice gets overloaded — it becomes the straggler that the whole query waits on, even though other slices finish quickly. **Why it hurts:** MPP speed depends on *even* work distribution; one hot slice negates the benefit of parallelism.

**How to choose a DISTKEY:**
1. Pick the column most frequently used to **join** large fact tables (not just any "unique-looking" column).
2. Check it has **even distribution** (no dominant value) — a high-skew key is worse than no key.
3. Use `ALL` for small dimensions, not `KEY`, to avoid unnecessary hashing/skew risk on tiny tables.
4. When unsure or the table is small/growing, start with `AUTO`.

**Real-world example:** `fact_orders` (billions of rows) distributed by `customer_id` because it's the main join column with millions of distinct, evenly-spread values. `dim_country` (200 rows) distributed as `ALL` since it's joined everywhere and tiny. `dim_date` similarly `ALL`.

**MEMORIZE:** Bad distribution doesn't just slow the scan — it forces a network shuffle on every join, which is usually the dominant cost in a slow Redshift query.

---

## 4. Sort Keys

**What it is:** the physical on-disk row ordering within each slice — like a persistent `ORDER BY` for stored data.

**Why it improves performance — Zone Maps / Data Skipping:** Redshift keeps min/max metadata per storage block. If the table is sorted by `sale_date` and a query filters `WHERE sale_date = '2026-01-05'`, Redshift can skip entire blocks whose min/max range can't contain that date — **without reading them at all**. This is the same principle as partition pruning in Spark, but at the block level.

**Compound vs Interleaved (understand, don't over-invest):**
- **Compound (default, most common):** sorted by column 1, then column 2, etc. Fast for filters on the leading column(s); less useful if you filter on a non-leading column.
- **Interleaved:** gives roughly equal weight to multiple columns for filtering, at the cost of more expensive maintenance (VACUUM REINDEX). Rarely worth it — compound covers the vast majority of real cases.

**How to choose a sort key:** the column(s) most commonly used in `WHERE` range filters or `ORDER BY` — almost always a **date/timestamp** for fact tables, since most analytical queries filter by time range.

**Unsorted data & maintenance:** as new rows are loaded, they can land unsorted at the end of a table, degrading zone-map effectiveness over time. `VACUUM` (or **Automatic Table Optimization**, which does this automatically in modern Redshift) re-sorts and reclaims space. **Why it matters:** an unsorted table silently loses its data-skipping benefit even if the `SORTKEY` is well chosen.

**Relationship between filtering, sorting, and performance (the core idea):** sort key + filter predicate together let Redshift skip reading most of the table. No filter on the sort key = no skipping benefit, regardless of how well the key was chosen.

**MEMORIZE:** SORTKEY = which blocks can be skipped. DISTKEY = which slice data lives on. They solve two different problems — don't confuse them in interviews.

---

## 5. Query Performance

**Why Redshift is fast (all forces multiply together):**
- **MPP** — work split across many slices in parallel.
- **Columnar storage** — only relevant columns read from disk.
- **Compression** — less data to read per column.
- **Distribution / data locality** — joins avoid network movement when co-located.
- **Sort keys / zone maps** — irrelevant blocks skipped entirely (predicate filtering benefits directly from this).
- **Query optimizer + statistics** — the optimizer uses table statistics (row counts, distribution info) to choose join order and strategy; stale stats → bad plans.
- **Result caching** — identical repeated queries can return cached results instantly without re-scanning.
- **Concurrency Scaling** — extra transient capacity spun up automatically under concurrent load (see Section 11).

**Common reasons a query becomes slow, and how to diagnose each:**

| Cause | Symptom | How to check |
|---|---|---|
| Data redistribution | Join is slow despite small data | `EXPLAIN` shows `DS_DIST_*` (esp. `DS_DIST_BOTH`) |
| Data skew | One slice takes far longer than others | `SVV_TABLE_INFO` / `STV_PARTITIONS`, uneven slice sizes |
| Missing/poor sort key | Full scans despite selective filters | Check zone-map skip ratio, `EXPLAIN` scan cost |
| Stale statistics | Optimizer picks a bad join order/plan | `SVV_TABLE_INFO.stats_off`, run `ANALYZE` |
| Too much data scanned | Query touches far more rows than needed | Check filter selectivity, missing predicate pushdown |
| Bad join strategy | Nested loop instead of hash/merge join | `EXPLAIN` plan join type |
| Poor compression / wrong data types | High storage & scan cost | `SVV_TABLE_INFO` encoding columns |
| Concurrency contention | Fast query alone, slow under load | WLM / queue wait time metrics |

**UNDERSTAND:** almost every slow-query root cause maps back to Section 2's execution flow — either (a) too much data scanned (columnar/sort key issue), (b) too much data moved (distribution issue), or (c) too much time waiting for a busy slice/queue (skew or concurrency issue).

---

## 6. Table Design & Optimization

Design order of priority for a new fact table (**memorize this sequence**):
1. **Distribution** — pick DISTKEY/DISTSTYLE based on join patterns (Section 3).
2. **Sort key** — pick based on filter patterns, usually a date column (Section 4).
3. **Data types** — use the smallest correct type (e.g. don't use `VARCHAR(max)` for a 3-char code) — smaller types compress better and scan faster.
4. **Compression encoding** — usually let Redshift auto-choose (`COPY` auto-applies encodings); manual tuning is rarely worth it today.
5. **Statistics** — keep `ANALYZE` current (or rely on Automatic Table Optimization) so the optimizer has accurate row/distribution info.
6. **VACUUM** — reclaim deleted space and re-sort; largely automated in modern Redshift but still worth understanding.

**Materialized Views:** precompute and store the result of an expensive, frequently-run query (e.g. a daily aggregate). Redshift can refresh them incrementally. **Why it helps:** turns a repeated heavy scan+aggregate into a cheap lookup — use for dashboards hitting the same aggregation repeatedly.

**Automatic Table Optimization (ATO):** Redshift monitors query patterns and can automatically choose/adjust distribution and sort keys for you. **Why it matters conceptually:** it means table design isn't "set once forever" — Redshift adapts as workload patterns change, reducing manual tuning burden.

**Redshift Advisor:** built-in recommendations (missing sort/dist keys, skew, unused columns) based on actual query history. Treat it as a starting point for tuning, not a substitute for understanding *why* (this whole document).

**MEMORIZE:** distribution + sort key design decisions matter far more than compression tuning or type micro-optimization — get the first two right, the rest is secondary.

---

## 7. Data Loading

**Production pattern:** `S3 → COPY → Redshift`. This is the standard bulk-load path, not row-by-row `INSERT`.

**Why COPY is preferred over INSERT:**
- `COPY` loads in **parallel across all slices simultaneously**, reading multiple files at once.
- `INSERT` (especially row-by-row) is a single-threaded, transactional operation — extremely slow at scale and generates excessive commit overhead.
- **Rule of thumb:** never bulk-load with `INSERT`; always use `COPY` from S3.

**How parallel loading works:** split your data into **multiple files** (ideally a multiple of the number of slices, roughly equal size, compressed) — `COPY` assigns files to slices so loading happens concurrently. One giant single file = no parallelism, one slice does all the work.

**File sizing:** aim for many evenly-sized files (roughly 1MB–1GB compressed range depending on cluster size) rather than one huge file or thousands of tiny files — mirrors the small-file problem seen in Spark/data lakes.

```sql
COPY orders
FROM 's3://bucket/orders/'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'
FORMAT AS PARQUET;
```

**UNLOAD (reverse direction):** exports query results from Redshift back to S3 — used to hand off curated data to a lake/lakehouse, or archive.

```sql
UNLOAD ('SELECT * FROM orders WHERE order_date = CURRENT_DATE')
TO 's3://bucket/export/'
IAM_ROLE 'arn:aws:iam::123:role/RedshiftRole'
FORMAT AS PARQUET;
```

**Simple real ETL example:**
```
Operational DB → nightly extract → S3 (Parquet) → COPY → Redshift staging table
    → transform (SQL) → curated fact/dim tables → BI tools query directly
```

**MEMORIZE:** COPY = parallel, bulk, preferred. INSERT = row-by-row, avoid at scale. Many right-sized files > one huge file > many tiny files.

---

## 8. Redshift Spectrum (Data Lake Integration)

**What it is:** lets Redshift run SQL directly against data sitting in **S3** (as external tables) without loading it into Redshift storage first.

**Why it exists:** not all data needs to live in expensive warehouse storage — Spectrum lets you query rarely-used or huge historical data straight from S3, and join it with "hot" data that *is* loaded into Redshift.

**How it works (high level):** the query planner recognizes the external table, pushes as much filtering/projection as possible down to a separate Spectrum compute layer that reads directly from S3 (**predicate/filter pushdown** — only matching rows/columns are pulled back, not the whole S3 dataset), then joins that result with native Redshift tables using normal execution.

**When to use Redshift tables vs S3 external data:**
- **Redshift native tables:** frequently queried, performance-critical, "hot" data — benefits fully from distribution/sort keys/compression.
- **S3 + Spectrum:** large historical/cold data, infrequently queried, or data also needed by other engines (Spark, Athena) — avoid duplicating it into Redshift storage.

**Architecture:**
```
Redshift Cluster ── native tables (hot data)
       │
       └── Spectrum ── queries external tables → S3 (cold/raw/shared data)
```

**MEMORIZE:** Spectrum = Redshift's bridge to the data lake — keep hot data native, cold/shared data in S3, query both together.

---

## 9. Redshift vs Data Lake vs Lakehouse (Databricks)

No universal winner — the right choice depends on data type, workload, and team.

| Dimension | Redshift | Data Lake (raw S3) | Lakehouse (Databricks/Delta) |
|---|---|---|---|
| Architecture | MPP warehouse, compute+storage historically coupled (now separated via RA3) | Object storage only, no compute | Object storage + transactional table layer (Delta/Iceberg) + Spark compute |
| Storage | Managed, columnar | Raw files (any format) | Delta/Parquet on object storage |
| SQL analytics | Excellent, mature optimizer | Weak natively (needs an engine on top) | Good, improving fast (Databricks SQL) |
| Performance (structured BI) | Very strong — purpose-built | Poor without an engine | Strong, close to warehouse-level now |
| Scalability | Very scalable, some elasticity via RA3/Serverless | Practically unlimited (cheap storage) | Practically unlimited, elastic compute |
| Cost | Higher for storage, good for stable structured workloads | Cheapest storage | Pay-per-compute, flexible but can be costly if not managed |
| ACID transactions | Native | None natively | Native via Delta/Iceberg |
| Data types | Structured only | Any (structured/semi/unstructured) | Any, with structure via Delta tables |
| ETL/ELT | Good for SQL-based ELT | Needs external engine (Spark) | Excellent (Spark-native) |
| BI workloads | Best fit | Poor alone | Very good, catching up to warehouses |
| ML/AI workloads | Weak (not built for it) | Good (raw access) | Best fit (native Spark/ML/AI tooling) |
| Semi/unstructured data | Poor fit | Excellent fit | Excellent fit |
| Governance | Strong, mature | Weak unless tooled | Strong via catalogs (Unity Catalog etc.) |
| Streaming | Limited | Depends on tooling | Strong (Structured Streaming) |
| Operational complexity | Low (managed warehouse) | Low storage, high complexity to build a platform | Moderate (more moving parts, more powerful) |

**Where Redshift wins and why:** stable, structured, SQL-heavy BI workloads where query latency and concurrency for dashboards matter most — the purpose-built MPP+columnar engine simply outperforms a general-purpose compute engine for this narrow job.

**Where Lakehouse wins and why:** when you need one platform for ETL + BI + ML/AI + streaming + unstructured data — avoids maintaining separate lake and warehouse copies of data, and scales compute independently of storage more flexibly.

**Realistic examples:**
- **Enterprise BI warehouse** → Redshift (curated marts, fast dashboards, many concurrent analysts).
- **Large-scale ETL / heavy transformation** → Lakehouse (Spark scales better for complex, code-heavy pipelines).
- **Data science / ML** → Lakehouse (native access to raw + curated data, ML runtime).
- **Raw S3 data lake, multiple consuming engines** → Data Lake, queried by Spectrum/Athena/Spark as needed.
- **Unified analytics org (Eng + BI + ML on one platform)** → Lakehouse.

**MEMORIZE (interview one-liner):** *"Redshift wins on pure structured SQL/BI performance and simplicity; Lakehouse wins on flexibility — unifying ETL, BI, ML, streaming, and unstructured data on one platform."*

---

## 10. Performance Troubleshooting — "Query is slow, what do I check?"

**Order of investigation (memorize this checklist):**

1. **Look at the query plan first** (`EXPLAIN`) — identifies the expensive step (scan, join, aggregate, sort).
2. **Check for data redistribution** — `DS_DIST_*` operators in the plan mean a join isn't co-located → review DISTKEY choice.
3. **Check for data skew** — compare row counts/time across slices (`SVV_TABLE_INFO`, `STV_PARTITIONS`) — one slice doing disproportionate work.
4. **Check sort key effectiveness** — is the query filtering on the sort key column? If not, zone maps can't help; check unsorted-row percentage.
5. **Check statistics freshness** — stale stats can cause the optimizer to pick a bad join order; run `ANALYZE`.
6. **Check how much data is actually scanned** — overly broad filters, missing partitioning/predicate pushdown (especially with Spectrum) scanning far more than necessary.
7. **Check join strategy** — nested loop joins (usually from missing/poor join conditions) are much slower than hash/merge joins.
8. **Check compression/data types** — oversized types or poor encodings inflate scan cost.
9. **Check concurrency/WLM** — a query that's fast alone but slow under load points to queue wait time, not the query itself; review workload management (WLM) queues or Concurrency Scaling activity.
10. **Check disk/storage pressure** — high disk-based query spilling indicates memory pressure, often tied to WLM memory allocation per queue.

**UNDERSTAND:** steps 2–4 (distribution, skew, sort key) cause the majority of real-world Redshift slowness — check these before anything else.

---

## 11. Scaling & Modern Redshift

**RA3 + Managed Storage — separation of compute and storage:** older Redshift node types tied storage directly to compute nodes (scale compute → forced to scale storage too, and vice versa). RA3 decouples them — data lives in managed storage (backed by S3), compute nodes cache hot data locally. **Why it matters architecturally:** you can scale compute for performance without over-paying for storage you don't need, and scale storage without adding unnecessary compute — much closer to the lakehouse cost model.

**Serverless:** no manually sized cluster — Redshift auto-provisions and scales compute (measured in RPUs) based on workload. **Why it matters:** removes capacity-planning guesswork, good fit for variable/unpredictable workloads; provisioned clusters remain more cost-predictable for steady, heavy, 24/7 workloads.

**Concurrency Scaling:** automatically spins up additional transient compute capacity when concurrent query load spikes, then spins back down. **Why it matters:** prevents queue backups during peak BI usage (e.g. Monday morning dashboard rush) without permanently over-provisioning the cluster.

**Automatic Table Optimization / Redshift Advisor:** covered in Section 6 — architecturally significant because they shift Redshift from "design once, tune manually forever" toward adaptive, workload-aware self-tuning.

**MEMORIZE:** RA3 = compute/storage decoupling. Serverless = no manual sizing. Concurrency Scaling = elastic burst capacity. All three exist to solve the same underlying problem — rigid, over-provisioned, manually-sized clusters.

---

## 12. Security & Reliability (concepts only)

- **IAM:** controls who/what can access Redshift and what it can do in AWS (e.g. `COPY`/`UNLOAD` needs an IAM role with S3 permissions).
- **VPC:** Redshift clusters run inside a VPC — network-level isolation, security groups control inbound/outbound access.
- **Encryption:** at rest (KMS-backed) and in transit (SSL) — standard expectation for any production warehouse.
- **Access control:** database-level users/groups/roles and grants — separate from IAM, controls SQL-level permissions on schemas/tables.
- **Backups/snapshots:** automated and manual snapshots to S3, enabling point-in-time restore.
- **High availability:** data replicated across nodes/AZ-aware managed storage (RA3); failed nodes can be replaced without data loss.
- **Disaster recovery:** cross-region snapshot copies for a full-region failure scenario.

**MEMORIZE (senior-level framing):** you're expected to know these exist and how they fit the architecture — not to recite console click-paths.

---

## 13. Real Production Architectures

**A) S3 → Redshift → BI** (simple warehouse pattern)
```
S3 (curated Parquet) → COPY → Redshift tables → BI tool (dashboards)
```
Why: straightforward when the source data is already clean/curated; Redshift is purely the serving/analytics layer.

**B) Operational DB → ETL → S3 → Redshift → BI** (typical enterprise pattern)
```
OLTP DB → extract (CDC/batch) → S3 (raw/staged) → COPY → Redshift staging
   → SQL transform → curated fact/dim tables → BI
```
Why: decouples extraction from transformation; S3 acts as a durable, replayable staging layer before the expensive warehouse load — mirrors the migration flow discussed in your Redshift→Databricks project, just terminating in Redshift instead.

**C) S3 Data Lake + Redshift hybrid (Spectrum)**
```
S3 (full historical raw data) ──Spectrum──┐
                                            ├── joined in Redshift queries
Redshift (hot curated tables) ─────────────┘
```
Why: keeps storage cost low for large/cold historical data while still serving fast queries on hot data, joining both when needed — avoids loading everything into expensive warehouse storage.

---

## 14. Senior Data Engineer Interview Section

**Architecture**
1. *What is Redshift and why is it fast?* → MPP + columnar + compression + distribution/sort keys minimizing scanned/moved data.
2. *Explain the Leader Node vs Compute Node vs Slice.* → Leader plans/coordinates; compute nodes store/process data; slices are the parallel execution units within a node.
3. *Walk through how a JOIN + GROUP BY query executes.* → parse/plan → parallel local scan per slice → co-located or redistributed join → local partial aggregation → merge at leader.
4. *What changed with RA3?* → decoupled compute and storage (managed storage backed by S3).

**Distribution**
5. *DISTKEY vs DISTSTYLE ALL vs EVEN — when would you use each?* → KEY for large frequently-joined fact tables; ALL for small dimensions; EVEN when no dominant join pattern.
6. *What happens if a join's key isn't the DISTKEY?* → Redshift redistributes data across the network before joining — expensive.
7. *What is data skew and why is it bad?* → uneven value distribution on the DISTKEY overloads one slice, becoming the straggler that delays the whole query.

**Sort Keys**
8. *What is a SORTKEY and how does it help?* → physical ordering enabling zone maps, so Redshift can skip storage blocks that can't match a filter.
9. *DISTKEY vs SORTKEY — what's the difference?* → DISTKEY decides *where* data lives (which slice); SORTKEY decides *ordering* within storage (which blocks can be skipped).
10. *Compound vs interleaved sort keys?* → compound favors leading-column filters and is cheaper to maintain; interleaved balances multiple columns but costs more to maintain — rarely needed.

**Performance & Troubleshooting**
11. *A query is slow — what do you check first?* → `EXPLAIN` plan → redistribution → skew → sort key effectiveness → stats → join strategy → concurrency.
12. *How do you detect data skew?* → compare per-slice row counts/query time via system views (`SVV_TABLE_INFO`, `STV_PARTITIONS`).
13. *Why would fast statistics matter?* → the optimizer uses stats to choose join order/strategy; stale stats can cause a bad plan even on well-designed tables.
14. *Why is one huge query fast alone but slow with many users?* → concurrency/WLM queue contention, not query design — Concurrency Scaling or WLM tuning addresses this.

**Table Design & Loading**
15. *Why is COPY preferred over INSERT for bulk loads?* → COPY loads in parallel across all slices; INSERT is single-threaded/transactional and far slower at scale.
16. *How should input files be sized for COPY?* → many evenly-sized files (matching slice count roughly) — avoids both single-file bottlenecks and small-file overhead.
17. *What is a materialized view and when would you use one?* → precomputed result of an expensive repeated query (e.g. daily aggregate) — trades storage/refresh cost for query speed.
18. *What does Automatic Table Optimization do?* → Redshift observes query patterns and adjusts distribution/sort keys automatically, reducing manual tuning.

**Redshift vs Lakehouse**
19. *When would you choose Redshift over a Lakehouse?* → stable, structured, SQL-heavy BI workloads needing best-in-class concurrency and dashboard latency.
20. *When would you choose a Lakehouse over Redshift?* → need to unify ETL, BI, ML/AI, streaming, and semi/unstructured data on one platform without duplicating data.
21. *Does ACID exist in a data lake?* → not natively on raw files; only via a table layer like Delta Lake/Iceberg on top.
22. *What is Redshift Spectrum and why does it exist?* → lets Redshift query S3 external tables directly, avoiding the need to load all data into expensive warehouse storage.

**Scenario/Design**
23. *Design a pipeline moving data from an OLTP source into Redshift for BI.* → OLTP → extract → S3 staging → COPY → transform → curated tables → BI (architecture B, Section 13).
24. *Your fact table has grown 10x and joins are slower — what do you investigate?* → distribution key still appropriate at new scale? skew increased? stats stale? sort key still matches query filters?
25. *How would you reduce cost for a large historical table rarely queried?* → move it to S3 and query via Spectrum instead of keeping it in native Redshift storage.
26. *You need near-real-time ingestion — is Redshift a good fit?* → limited native streaming; consider streaming ingestion features or a lakehouse/Kafka-based pipeline feeding in near-real-time micro-batches.

---

## FINAL REVISION SECTION

### 1. Redshift in 5 minutes
Redshift is a cloud OLAP data warehouse built on MPP + columnar storage. A Leader Node plans queries; Compute Nodes (split into Slices) execute them in parallel. Performance depends on three levers: **distribution** (minimize data movement across the network during joins), **sort keys** (minimize data scanned via block skipping), and **compression/columnar storage** (minimize I/O). Data is bulk-loaded from S3 via `COPY` (parallel) and can query S3 directly via Spectrum. Best for structured, SQL-heavy BI workloads; a Lakehouse (Databricks) is the better fit when you need to unify ETL, BI, ML, streaming, and unstructured data on one platform.

### 2. Architecture cheat sheet
- Leader Node = plan + coordinate + merge (no heavy scanning).
- Compute Node = storage + processing, split into Slices.
- Slice = parallel execution unit; more slices = more parallelism.
- MPP = query split across slices, run simultaneously.
- Columnar + compression = read only what's needed, in less space.
- RA3 = compute/storage decoupled (managed storage on S3).
- Serverless = no manual cluster sizing, auto-scaled RPUs.

### 3. Performance cheat sheet
Fast because: MPP parallelism + columnar scans + compression + co-located joins (good distribution) + block skipping (good sort key) + accurate stats → good optimizer plans.
Slow because (check in this order): redistribution (bad DISTKEY) → skew → poor sort key match to filters → stale stats → too much data scanned → bad join type → concurrency/WLM contention.

### 4. Distribution vs Sort Key cheat sheet
| | DISTKEY | SORTKEY |
|---|---|---|
| Controls | Which **slice** a row lives on | **Order** of rows within storage |
| Solves | Join co-location (avoid network shuffle) | Block skipping via zone maps (avoid unneeded I/O) |
| Bad choice causes | Redistribution cost, skew | Full scans despite selective filters |
| Typical pick | Main large-table join column | Date/timestamp filter column |

### 5. Redshift vs Lakehouse cheat sheet
- **Structured BI, stable schema, heavy concurrent dashboards →** Redshift.
- **Unified ETL + BI + ML/AI + streaming + unstructured data →** Lakehouse.
- **Cheapest raw storage, multiple consuming engines →** Data Lake (+ Spectrum/Athena/Spark).
- Redshift = performance & simplicity for one job. Lakehouse = flexibility across many jobs.

### 6. Top 20 things to remember
1. Redshift = OLAP + MPP + columnar — every feature serves one of these.
2. Leader Node plans; Compute Nodes/Slices execute in parallel.
3. Columnar + compression = less I/O = faster scans.
4. DISTKEY controls join co-location; mismatched keys cause expensive redistribution.
5. Use `ALL` for small dimension tables, `KEY` for large frequently-joined fact tables.
6. Data skew on a DISTKEY creates a straggler slice that slows the whole query.
7. SORTKEY enables zone-map block skipping — only helps if queries filter on it.
8. Unsorted/unvacuumed tables lose their sort-key benefit over time.
9. DISTKEY = where data lives; SORTKEY = what can be skipped — different problems.
10. Optimizer relies on statistics — stale stats can silently ruin performance.
11. COPY loads in parallel from S3; INSERT is slow at scale — never bulk-load with INSERT.
12. File sizing for COPY: many evenly-sized files, not one huge file or thousands of tiny ones.
13. UNLOAD exports Redshift data back to S3 for lake/lakehouse handoff.
14. Spectrum queries S3 directly — keep hot data native, cold/shared data in S3.
15. RA3 decoupled compute and storage — a major architectural shift.
16. Serverless removes manual cluster sizing; Concurrency Scaling adds burst capacity under load.
17. Materialized views precompute expensive repeated aggregations.
18. Automatic Table Optimization adapts distribution/sort keys based on real query patterns.
19. Redshift wins on structured SQL/BI performance; Lakehouse wins on platform flexibility.
20. Diagnosing a slow query: check `EXPLAIN` → redistribution → skew → sort key match → stats → concurrency, in that order.

### 7. Common interview traps
- Confusing DISTKEY and SORTKEY (different jobs — placement vs ordering) — a very common mix-up.
- Saying "always use INSERT for loading" — wrong, COPY is the production pattern.
- Claiming a data lake has ACID natively — it doesn't; only via a table layer like Delta/Iceberg.
- Saying Redshift or Lakehouse is "always better" — the correct interview answer is trade-off-based, tied to workload type.
- Forgetting that a well-chosen but **skewed** DISTKEY is still bad — cardinality alone isn't enough, distribution evenness matters too.
- Thinking sort key alone helps without a matching filter in the query — it doesn't; skipping only works when the query actually filters on the sorted column.