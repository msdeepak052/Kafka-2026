# Kafka Performance Tuning

For a **Senior Platform Engineer**, don't think of "Kafka performance tuning" as:

> "A list of config values to paste into `server.properties`."

Think of it as:

> **Five hardware/OS resources — I/O, Network, RAM, CPU, and the OS itself — plus a handful of client-side levers, where the actual skill is figuring out *which one* is your real bottleneck before you touch a single config.**

This file is conceptual — the "why does my cluster feel slow, and what actually explains it" reference. No new AWS Console steps happen here; it builds on the `kf1`/`kf2`/`kf3` broker cluster this Lesson-4 series has been building (the same `t3.small` instance type used for the `zk1`/`zk2`/`zk3` ensemble in Lesson-3, by the lab's established convention of reusing one instance size across ensembles) and the multi-listener setup from file `05` of this series.

---

# 1. What "Kafka Performance" Actually Means

- There is no single "performance knob."
  - Newcomers assume a slow cluster means "increase some number in `server.properties`."
  - In reality, a Kafka broker sits at the intersection of five independent resources, and a bottleneck in any one of them produces symptoms that *look* similar from the outside (higher latency, growing consumer lag, timeouts) but need completely different fixes.
- The five resources, and what each one actually governs:
  - **I/O** — how fast the broker can write to and read from disk.
  - **Network** — how fast bytes move between clients, brokers, and replicas.
  - **RAM** — how much of the "hot" data Kafka can serve straight from memory instead of disk.
  - **CPU** — how much work (compression, TLS, request handling) the broker can do per second.
  - **OS** — the kernel-level settings (file descriptors, swappiness, filesystem) that determine whether the other four resources are even usable at their full potential.
- On top of those five, there's a sixth category this lesson groups as **"Other"** — client-side levers (producer batching, consumer fetch tuning) and **monitoring**, because you can't tune what you can't measure.

```text
                    Kafka Performance
                           │
        ┌──────┬──────┬───┴───┬──────┬───────┐
        │      │      │       │      │       │
       I/O  Network  RAM     CPU    OS    Other
        │      │      │       │      │       │
      disk   sockets page   compress fd    batching
      seq/   threads cache  TLS     limits fetch tuning
      rand   buffers heap   request swap   monitoring
                     size   handling fs
```

- A message's actual end-to-end latency is shaped by **all six**, in sequence:

```text
Producer ──▶ Network ──▶ I/O (write) ──▶ RAM (page cache) ──▶ Replication (Network + I/O)
                                                                       │
                                                                       ▼
                                                              Consumer ◀── Network ◀── I/O/RAM (read)
```

- Don't diagnose "Kafka is slow" by guessing. Diagnose it by asking, in order: *which of these six is actually saturated right now?*

---

# 2. Sequential vs. Random Disk I/O — Why Kafka's Log Design Avoids the Slow Path

- This is the foundation everything else in the I/O section builds on. It's written fresh here, self-contained — you don't need to have seen it explained elsewhere in this repo to follow it.
- **Random I/O** — the disk head (or, on SSDs, the underlying flash controller) has to jump around to unrelated locations for every read/write.
  - Analogy: a librarian asked to fetch one page from book #4, then one page from book #91, then one page from book #12 — constant repositioning, very little actual reading.
  - This is what a traditional database index update pattern often looks like: writes scattered across many different locations on disk as records are inserted/updated in arbitrary order.
- **Sequential I/O** — the disk only ever reads/writes the *next* contiguous block.
  - Analogy: the same librarian reading a single book start to finish, one page after another, never leaving their seat.
  - Sequential I/O is dramatically faster than random I/O on spinning disks (no seek time) and still meaningfully faster on SSDs (better throughput, less controller overhead, more predictable latency).
- **Kafka's log is append-only, by design, specifically to stay on the sequential-I/O side of that line:**
  - A partition is physically a set of **log segment files** on disk.
  - A producer write is *always* appended to the end of the **active segment** — never inserted in the middle, never overwriting an earlier offset.
  - A consumer read is *almost always* sequential too — a consumer reading from offset 1000 reads 1001, 1002, 1003... in order, which is exactly the access pattern the underlying filesystem and disk are best at.
- Concretely, for one partition:

```text
Partition-0 log segments (on disk, in log.dirs)

segment-00000000000.log   [oldest]
segment-00000104857600.log
segment-00000209715200.log  [active — new writes append here]

New message
    │
    ▼
always appended to the END of the active segment
    (never inserted into an earlier segment)
```

- This is why Kafka can sustain very high write throughput on ordinary disks without needing exotic hardware: it never asks the disk to do the expensive thing (random seeks), only the cheap thing (sequential append).
- Practical implication for this lab's `kf1`/`kf2`/`kf3` cluster: even on `gp3` EBS volumes (not the fastest disks available), sequential-append throughput is what Kafka actually needs — which is a large part of why Kafka performs well on comparatively modest storage compared to a random-access-heavy workload like a relational database.

---

# 3. `log.dirs` — Multiple Disks (JBOD) to Parallelize I/O Across Partitions

- `log.dirs` is a **comma-separated list of directories** where the broker stores its log segments.
  - No default — must be explicitly set (echoing file `02`'s note on this).
  - It accepts *multiple* paths, not just one.
- Why multiple paths matter for performance, not just capacity:
  - Apache Kafka's own operations guidance recommends using **multiple drives** for Kafka data, specifically to get good throughput, and explicitly recommends *not* sharing those drives with application logs or other OS filesystem activity, to keep latency predictable.
  - Two ways to use multiple drives, with a real tradeoff between them:
    - **RAID them into a single volume**, and give Kafka one `log.dirs` path — simpler, but Kafka's own replication already provides redundancy, so RAID's redundancy is often redundant-on-redundancy.
    - **Format and mount each drive separately**, and list each as its own entry in `log.dirs` — this is the **JBOD** ("Just a Bunch Of Disks") approach.
  - With JBOD, Kafka assigns **partitions to data directories round-robin** — each partition lives entirely on one directory (one disk), not striped across all of them.
- What this buys you:
  - Partition A's writes go to disk 1, partition B's writes go to disk 2, partition C's writes go to disk 3 — all three can be written **in parallel**, each disk doing its own sequential append, instead of every partition competing for the I/O queue of a single disk.
- The real tradeoff Kafka's own docs call out:
  - If data isn't well balanced across partitions (some partitions much busier than others), JBOD can create **load imbalance between disks** — one disk hot, others idle — because placement is round-robin by partition count, not by measured load.
- Example, for this lab's cluster:

```text
kf1 broker, single log.dirs (today's lab setup)
  log.dirs=/var/lib/kafka/data

  P0 P1 P2 P3 P4 P5   ← all 6 partitions, all writes,
                        all compete for ONE disk's I/O queue
```

```text
kf1 broker, JBOD across 3 disks (production-style)
  log.dirs=/data1/kafka,/data2/kafka,/data3/kafka

  /data1 → P0 P3
  /data2 → P1 P4
  /data3 → P2 P5

  3 disks writing in parallel instead of 1 disk serializing everything
```

- On AWS specifically: this usually means attaching multiple EBS volumes (or using multiple NVMe instance-store volumes on instance types that have them) rather than one large volume, and listing each mount point in `log.dirs`.

---

# 4. `num.io.threads` — Sizing the I/O Thread Pool

- `num.io.threads` controls the number of threads the broker uses for **processing requests, which may include disk I/O** — Apache Kafka's default value is **8**.
- Where this fits in the request pipeline (previewed here, detailed alongside `num.network.threads` in the Network section):

```text
Client request
     │
     ▼
Network threads (num.network.threads)
     │  read the request off the socket
     ▼
Request queue
     │
     ▼
I/O threads (num.io.threads)   ← this setting
     │  actually do the work: write to the log segment,
     │  read from the log segment, replicate, etc.
     ▼
Response queue → Network threads → client
```

- How to reason about sizing it, rather than treating 8 as sacred:
  - I/O threads are the ones that actually touch disk (and, in some request types, CPU-bound work like decompression). If you have **more physical disks** in `log.dirs` (Section 3) than you have I/O threads capable of keeping them all busy, some disks sit idle waiting for a thread.
  - A reasonable starting heuristic operators use: keep `num.io.threads` at least as large as the number of disks backing `log.dirs`, and don't push it wildly past your CPU core count — beyond some point, more threads just means more context-switching overhead, not more real parallelism.
  - Community tuning guidance commonly raises this toward **16** for throughput/latency-sensitive workloads, particularly on HDDs or brokers hosting many partitions — but this is a *starting point to benchmark from*, not a universal correct value.
- How you'd actually know if this is your bottleneck rather than guessing: the JMX metric `RequestHandlerAvgIdlePercent` (Section 20) is the direct signal — if it's consistently low, your I/O threads are saturated and raising `num.io.threads` (assuming you have the disks/cores to back it) is the right lever.
- On this lab's `t3.small` brokers (2 vCPUs — see Section 14): there's little value in pushing `num.io.threads` far above the default, because the CPU itself becomes the ceiling long before thread count does.

---

# 5. Segment Size and Flush Behavior — Why Kafka Doesn't `fsync` Every Write

- **`log.segment.bytes`** — the maximum size of a single log segment file.
  - Apache Kafka default: **1073741824 bytes (1 GiB)**.
  - Once the active segment hits this size, Kafka rolls to a new segment file (Section 2's diagram) — this also interacts with retention, since whole segments (not individual messages) are what get deleted once they age out.
- **`log.flush.interval.messages`** — number of messages accumulated on a partition before Kafka forces a flush to disk.
  - Apache Kafka default: **9223372036854775807** — i.e. `Long.MAX_VALUE`, effectively "never force a flush based on message count."
- **`log.flush.interval.ms`** — max time a message sits in memory before being flushed.
  - Apache Kafka default: **null** (falls back to the separate `log.flush.scheduler.interval.ms` background-check interval).
- Put plainly: **out of the box, Kafka does not call `fsync` on every write.** Both flush-interval settings are configured, by default, to essentially never force a synchronous flush.
- Why this is a deliberate design choice, not an oversight — straight from Apache's own operations guidance:
  - Kafka immediately writes data to the filesystem, then relies on a *flush policy* that controls when data actually gets forced from the OS page cache onto physical disk.
  - Kafka's own recommendation: **use the default flush settings, which disable application-level `fsync` entirely** — rely on the OS's own background flush plus Kafka's own background flush instead of forcing a sync on every produced message.
  - The reasoning given directly in that guidance: **"durability in Kafka does not require syncing data to disk, as a failed node will always recover from its replicas."** Durability comes from **replication** (multiple brokers holding the data), not from every single broker guaranteeing every single byte is physically on platter before acknowledging.
- What this buys you architecturally:

```text
Producer write
     │
     ▼
Page cache (RAM)  ──immediately readable, immediately replicable──▶ Followers
     │
     │  background OS flush + Kafka background flush
     │  (NOT a synchronous fsync per message)
     ▼
Disk (durable)
```

- If you set `log.flush.interval.messages=1` to force an `fsync` on every message, you get much stronger single-broker durability guarantees — at the cost of collapsing your write throughput down toward single-disk `fsync` latency, defeating the sequential-append design from Section 2. Kafka's model deliberately trades that off: **let the OS batch flushes efficiently, and let replication (via `acks`/`min.insync.replicas`, covered in file `02`) be the actual durability mechanism.**

---

# 6. `num.network.threads` — What It Actually Controls

- `num.network.threads` — the number of threads the broker uses for **receiving requests from the network and sending responses to the network**.
  - Apache Kafka default: **3**.
  - Important detail from the current broker-configs documentation: **each listener (except the controller listener) creates its own thread pool** — so a broker with a public client listener and a separate internal replication listener (file `05`'s multi-listener setup) isn't sharing one pool of 3 threads across both; each listener gets its own 3 (by default).
- Where it sits, restated from Section 4's diagram:

```text
Client / Follower request
     │
     ▼
Network threads (num.network.threads)   ← reads bytes off the socket
     │
     ▼
Request queue
     │
     ▼
I/O threads (num.io.threads)   ← does the actual disk work
     │
     ▼
Network threads   ← writes the response back to the socket
     │
     ▼
Client / Follower
```

- Network threads do relatively cheap, fast work (moving bytes, not touching disk) — so they saturate under **connection volume and request rate**, not disk speed. A broker fielding a very large number of concurrent client connections (many small producers/consumers) is a more likely candidate for network-thread pressure than a broker with few connections but huge payloads.
- Detecting saturation: `NetworkProcessorAvgIdlePercent` (Section 20) is the direct JMX signal.

---

# 7. Socket Buffers and Max Request Size

- **`socket.send.buffer.bytes`** — the `SO_SNDBUF` size for the broker's server sockets. Apache Kafka default: **102400 (100 KiB)**. `-1` means "use the OS default" instead.
- **`socket.receive.buffer.bytes`** — the `SO_RCVBUF` size for the broker's server sockets. Apache Kafka default: **102400 (100 KiB)**. Same `-1` behavior.
- **`socket.request.max.bytes`** — the maximum number of bytes allowed in a single socket request. Apache Kafka default: **104857600 (100 MiB)**.
- What these mean in practice:
  - The send/receive buffer sizes are the kernel-level TCP buffers the broker's socket layer uses per connection. Too small, and high-latency or high-throughput links (e.g. cross-AZ or cross-region replication) can't keep the pipe full — Kafka's own documentation notes these can be increased specifically to enable higher-performance data transfer between data centers.
  - `socket.request.max.bytes` is a hard ceiling — any single request larger than this is rejected outright, as a basic protection against a misbehaving or malicious client sending an enormous payload in one request.
- These are broker-wide settings — they apply the same to a client connection and a fellow-broker replication connection on the same listener, which is exactly why *which listener that connection is on* (Section 8) matters as much as the buffer size itself.

---

# 8. Why Replication Traffic Competes With Client Traffic — and Why Listener Separation Matters

- Every byte a broker receives from a producer, it typically has to **send again** to its followers for that partition (replication). With `RF=3`, one produced message can mean roughly two more copies flowing broker-to-broker.
- If client traffic and replication traffic share the same network path, they compete for the same NIC bandwidth:

```text
                     Broker (single NIC, no listener separation)

Producer ──▶ ┐
             ├──▶  NIC  ──▶  [client traffic + replication traffic, same pipe]
Consumer ◀── ┘                          │
                                          ▼
                              Follower brokers (replication)
```

- This is exactly why file `05`'s multi-listener design matters for performance, not just for security/addressing:
  - `inter.broker.listener.name` should point at a **listener on the internal/private network** — the one carrying broker-to-broker replication traffic — kept separate from the listener clients on the public internet connect to.
  - With that separation, replication traffic isn't fighting public-internet client traffic for the same bandwidth/socket-buffer budget, and a burst of external client load doesn't starve replication (which, if starved for too long, turns partitions **under-replicated** — the same failure mode covered from a broker-outage angle in file `01`).

```text
                Broker (client listener + internal replication listener)

Producer/Consumer ──▶ Public/Client Listener ──▶ NIC (path A)
                                                      │
Follower brokers   ◀── Internal Listener      ◀── NIC (path B, private network)
                        (inter.broker.listener.name)
```

- On this lab's `kf1`/`kf2`/`kf3` cluster, that means: client-facing listener bound to whatever address external producers/consumers reach, `inter.broker.listener.name` bound to the brokers' private VPC addresses — the same private-network principle the `zk1`/`zk2`/`zk3` ensemble used for its own peer traffic in Lesson-3.

---

# 9. Compression — the Network-vs-CPU Trade-off

- `compression.type` exists at **two** layers, with different defaults, easy to conflate:
  - **Producer-side `compression.type`** (client config) — Apache Kafka default: **`none`** (no compression). Valid values: `none`, `gzip`, `snappy`, `lz4`, `zstd`.
  - **Broker-side `compression.type`** (broker config) — Apache Kafka default: **`producer`**, meaning the broker keeps whatever codec the producer already used rather than re-compressing.
- The trade-off, stated plainly:
  - Compressing a batch **before** sending it over the network means fewer bytes on the wire — helps Network (Section 6-8) — but costs CPU cycles to compress on the producer/broker side and decompress on the consumer/broker side, feeding directly into the CPU section below.
  - Different codecs sit at different points on that curve — `gzip` compresses smaller but costs more CPU; `lz4`/`snappy` compress less aggressively but are much cheaper on CPU; `zstd` is a newer middle ground with generally good ratio for moderate CPU cost.
- Concrete framing for this lab's cluster: if `kf1`/`kf2`/`kf3` are network-constrained (small NICs, cross-AZ replication) but have CPU headroom, enabling `lz4` compression trades cheap CPU cycles for scarce network bandwidth. If those same brokers are already CPU-bound (Section 13/14), adding compression makes the CPU bottleneck worse to fix a problem you don't have.
- This is the direct forward-link to Section 13 — you cannot reason about compression in isolation from CPU headroom.

---

# 10. Kafka's Reliance on the OS Page Cache

- This is the RAM section's foundation — written fresh here, though the underlying idea (rely on page cache, keep heap small) echoes the same principle file `01` touched on in its cluster-sizing context.
- Every EC2 instance's RAM is really split into two very different consumers, from Kafka's point of view:

```text
EC2 Instance RAM
       │
       ├── JVM Heap           (Kafka broker process's own managed memory)
       │
       └── OS Page Cache      (everything else — managed by the Linux kernel)
                │
                └── holds recently-written / recently-read Kafka log segment data
```

- Why the page cache matters so much specifically for Kafka:
  - A produced message is written to a log segment file. That write lands in the **OS page cache** immediately (Section 5) — it doesn't have to be physically flushed to disk before it becomes readable.
  - A consumer reading recent data (the common case — most consumers read close to the tail of the log) is very likely reading data that's **still sitting in page cache**, meaning the read never has to touch the disk at all.
  - Kafka's own operations guidance highlights this as a real advantage of using pagecache over an in-process application cache: the I/O scheduler can batch small writes into larger physical writes, re-sequence writes to reduce disk-head movement, and — critically — **it automatically uses all the free memory on the machine**, no application-level cache-sizing logic required.
- The consequence for JVM heap sizing:
  - **Don't allocate all — or even most — available RAM to the JVM heap.** A large heap doesn't make Kafka faster; Kafka's own hot-path data isn't primarily living in heap, it's living in page cache.
  - A large heap instead makes **garbage collection pauses bigger and less predictable**, which shows up as latency spikes.
  - Common production guidance keeps broker heap in the range of a few GB (commonly cited starting points are around 4–6 GB) *regardless of how much total RAM the instance has* — the rest is deliberately left for page cache to hold as much of the active log as possible.
- Restated as the mental model:

```text
More instance RAM
        ≠
More JVM heap

More instance RAM
        =
More room for the OS page cache to hold Kafka's hot log data
```

---

# 11. This Lab's `t3.small` RAM Budget

- `t3.small` provides **2 GiB of memory** (and 2 vCPUs) — this is the exact instance type this lab's `zk1`/`zk2`/`zk3` ensemble ran on in Lesson-3, and (by this series' convention) the same instance type `kf1`/`kf2`/`kf3` run on here.
- Run that 2 GiB number through Section 10's model:

```text
t3.small total RAM: 2 GiB

  JVM heap (kept small, per Section 10)   ~0.5–1 GiB
  OS + other processes                     some fixed overhead
  Remaining for page cache                 whatever's left
                                            (often well under 1 GiB)
```

- That's a **very** thin page cache budget. Anything beyond a small working set of "hot" recent data falls out of cache almost immediately, forcing reads back to disk — which is fine for this lab's low, bursty, manually-generated traffic, but would fall over quickly under any real sustained production load.
- Honest framing: `t3.small` is **fine for learning** — it's cheap, it lets you stand up a real 3-broker cluster and see real broker behavior, real replication, real failure handling. It is **not** sized for real production throughput. A production broker sizing exercise — instance family, RAM, disk throughput, all worked through with real numbers — is exactly what the earlier Cluster-Size / Production-on-AWS material in this series covers in depth; that reasoning isn't repeated here.

---

# 12. What Actually Consumes CPU on a Broker

- CPU on a Kafka broker isn't one thing — it's several distinct sources of work, worth naming individually:
  - **Compression / decompression** (Section 9) — every compressed batch costs CPU cycles on the way in (or out, if the broker has to decompress to validate/re-compress) and again on the consumer side.
  - **TLS** — encrypting/decrypting every byte on the wire. This lab's `kf1`/`kf2`/`kf3` cluster doesn't have TLS enabled yet (plaintext listeners so far, per the multi-listener work in file `05`), but it's coming later in this series — and it is a real, often underestimated, CPU cost once enabled, especially at high throughput.
  - **Request handling** — the I/O thread pool (Section 4) work: parsing requests, appending to log segments, serving fetch requests.
  - **Replication** — `num.replica.fetchers` (default: **1** per source broker) controls how many threads pull data from each leader for replication; more fetcher threads means more parallel replication I/O, at the cost of more CPU/memory pressure, per Kafka's own documentation on that setting.
- The general shape to keep in mind:

```text
Broker CPU
   │
   ├── Compression/decompression   (Section 9 — a chosen trade-off)
   ├── TLS                          (not yet enabled in this lab)
   ├── Request handling             (num.io.threads work)
   └── Replication fetching         (num.replica.fetchers work)
```

- None of these are things you "turn off" to get free performance — they're each a deliberate feature (durability via replication, security via TLS, efficiency via compression) that costs CPU in exchange for something else. The tuning question is never "eliminate CPU use," it's "is the CPU I'm spending buying me something I actually need, on hardware that actually has the headroom for it" — which is exactly why Section 13 matters for this specific lab.

---

# 13. `t3.small` Is a Burstable Instance — CPU Credits and Why That Matters Here

- `t3.small` belongs to AWS's **T-family (burstable performance)** instance types — a fundamentally different CPU model than a fixed-performance instance (like an `m5` or `c5`), and this matters directly for anyone benchmarking Kafka on this exact lab hardware.
- How the credit mechanic actually works, per AWS's own documentation:
  - Each T-family instance has a **baseline CPU utilization** it can sustain indefinitely for a net-zero credit balance. For `t3.small`: **20% baseline utilization per vCPU**, across its 2 vCPUs.
  - The instance **earns CPU credits continuously** while running below that baseline, and **spends credits** whenever it runs above baseline. `t3.small` earns **24 CPU credits per hour**, with a maximum accrual cap of **576 credits** (24 hours' worth) — new credits earned past that cap are discarded, not banked indefinitely.
  - One CPU credit = one vCPU at 100% utilization for one minute (or the equivalent spread across time/vCPUs).
- What happens when credits run out — and this is the part that matters most for benchmarking:
  - **Standard mode**: once accrued credits are exhausted, the instance's CPU performance **gradually drops back down to the baseline** and cannot burst again until it re-accrues credits. On a `t3.small`, that baseline is a fairly small slice of 2 vCPUs — a sustained Kafka load test can look fine for the first several minutes (burning down accrued credits) and then visibly throttle once the credit balance hits zero.
  - **Unlimited mode**: T3 instances (unlike the older T2 family) launch in **Unlimited mode by default**. In this mode, once accrued credits are spent, the instance can keep bursting above baseline by spending *surplus* credits instead of hard-throttling — but if the **average CPU utilization over a rolling 24-hour period** ends up above baseline, AWS bills the difference at a flat additional per-vCPU-hour rate. So Unlimited mode trades "unpredictable throttling" for "unpredictable extra billing" — it doesn't remove the underlying constraint, it just changes which surprise you get.
- Why this specifically matters for anyone running a load test against this lab's `kf1`/`kf2`/`kf3` cluster:
  - A short burst of producer/consumer traffic will likely look great — you're spending accrued credits, effectively borrowing CPU performance above what the instance can sustain.
  - A longer, sustained benchmark can quietly hit a wall (Standard mode) or a quietly larger bill (Unlimited mode, the T3 default) — either way, a number you measured in minute 2 of a test is **not** a number you can assume holds at minute 20.
  - This is a direct, practical reason `t3.small` is unsuitable for real production sizing decisions (Section 11 made the RAM version of this same point): its CPU capacity isn't a fixed number, it's a function of *recent* utilization history.

---

# 14. File Descriptor Limits — `ulimit -n`

- Kafka brokers need file descriptors for two distinct things, both at real scale:
  - **One per open log segment** — a broker hosting many partitions, each with several segments (Section 2/5), accumulates file descriptors quickly.
  - **One per open client/replication connection.**
- Apache Kafka's own operations guidance gives the formula and the number directly:
  - Estimate needed descriptors as roughly `(number_of_partitions) × (partition_size / segment_size)`, plus the number of open connections.
  - **Recommendation: at least 100,000 allowed file descriptors for the broker process, as a starting point.**
- This is the same theme file `05` of the ZooKeeper hands-on lesson touched, in miniature — that systemd unit set `LimitNOFILE=65536` for the ZooKeeper process. **Kafka's real number needs to be bigger than that ZooKeeper value**, and the reason is scale: a ZooKeeper ensemble typically manages a modest number of znodes and client sessions, while a Kafka broker in a real deployment can easily be tracking many thousands of partition-segments plus a large number of concurrent producer/consumer/replication connections simultaneously. The mechanism (raise `nofile` via `/etc/security/limits.conf` or a systemd unit's `LimitNOFILE=`) is identical — only the target number differs, because Kafka's per-broker resource footprint (partitions × segments × connections) is structurally larger than ZooKeeper's (znodes × sessions).
- On this lab's small `kf1`/`kf2`/`kf3` cluster with a handful of topics/partitions, you won't come close to exhausting even the default OS limit — but setting the broker's `nofile` limit correctly (rather than relying on whatever the OS default happens to be) is a standard production checklist item precisely because the failure mode (a broker unable to open a new log segment or accept a new connection because it's hit its descriptor ceiling) is ugly and easy to misdiagnose as something else.

---

# 15. `vm.swappiness` — Why Swapping Is Bad for Kafka Specifically

- `vm.swappiness` is a Linux kernel parameter, 0–100, controlling how aggressively the kernel moves memory pages out to swap space on disk versus keeping them resident in RAM.
- Common operational guidance (from Confluent's and Cloudera's Kafka tuning documentation) is to set it to a **very low value — commonly `1`** — rather than leaving it at a typical desktop-oriented default.
- Why this matters for Kafka *specifically*, beyond the generic "swapping is slow" point:
  - Kafka leans hard on the OS page cache for its actual performance (Section 10). Every byte the kernel decides to swap out is memory taken away from that page cache, directly undermining the exact mechanism Kafka's performance model depends on.
  - If the *JVM heap itself* gets swapped (rather than page cache), the effect is worse: a garbage-collection pause that would normally take milliseconds can balloon dramatically once the JVM has to page memory back in from disk mid-collection — and a sufficiently long GC pause can look, to the rest of the cluster, like the broker died (missed heartbeats, session timeouts), triggering unnecessary leader elections and rebalancing.
- This is the same underlying warning the ZooKeeper hands-on lesson (Lesson-3, file `05`) already raised for ZooKeeper's own process — "do not allow ZooKeeper to swap," attributed there directly to Apache ZooKeeper's own admin guide. The mechanism and the fix (`vm.swappiness`) are identical for Kafka; only the specific process being protected changes.
- Practically: don't disable swap entirely (a small swap space is still a useful safety valve against an abrupt OOM-kill during a genuine memory spike) — instead, tune `vm.swappiness` low so the kernel strongly prefers reclaiming page cache pages (cheap — they can just be re-read from disk) over swapping out active process memory (expensive and disruptive).

---

# 16. Filesystem Choice — XFS vs. EXT4 for `log.dirs`

- This is a case where it's worth checking the *current* guidance rather than repeating older folklore, since filesystem performance characteristics do shift over time.
- Apache Kafka's own current operations documentation is direct about this:
  - Kafka has no hard dependency on a specific filesystem — it uses regular files.
  - The two filesystems with the most real-world usage are **EXT4** and **XFS**.
  - The documentation states plainly: *"Historically, EXT4 has had more usage, but recent improvements to the XFS filesystem have shown it to have better performance characteristics for Kafka's workload with no compromise in stability."*
  - Kafka's own comparison testing (cited in that same documentation) measured **"Request Local Time"** (how long append operations take) under significant message load: **XFS came out around 160ms vs. 250ms+ for the best-tuned EXT4 configuration**, with XFS also showing **less variability**.
  - **Current recommendation: XFS**, and it needs comparatively little manual tuning — Kafka's docs note it has significant auto-tuning built in, with only a couple of optional mount-time tweaks (`largeio`, `nobarrier`) worth even considering, neither of which is required.
  - EXT4 remains a "serviceable" choice per that same documentation, but reaching EXT4's best performance requires several mount-option changes (`data=writeback`, disabling journaling, tuning `commit=`, etc.) that trade away crash-safety — acceptable for a single-broker-failure scenario (Kafka just rebuilds the replica from other brokers) but riskier in a correlated multi-broker failure (e.g. a power outage hitting several brokers at once).
  - One general-filesystem recommendation applies regardless of which you pick: mount data directories with **`noatime`**, since Kafka never relies on file access-time metadata, and tracking it is pure overhead.
- For this lab: the exact filesystem underlying `kf1`/`kf2`/`kf3`'s `gp3` volumes (whatever Ubuntu 24.04 defaults to) is fine for learning purposes at this traffic volume — but if you were taking this cluster toward production, **XFS is the currently-recommended choice for `log.dirs`**, not an assumption carried over from older Kafka-tuning folklore.

---

# 17. Producer-Side Batching — `batch.size` and `linger.ms`

- These are **client-side** levers (file `02`'s "producer config" layer) — the broker has no say in them, but they materially change perceived performance, which is why they belong in this lesson's "Other" catch-all.
- **`batch.size`** — the max number of bytes the producer tries to batch per partition before sending. Apache Kafka default: **16384 bytes (16 KiB)**.
- **`linger.ms`** — how long the producer will wait, after a batch has accumulated some data but hasn't hit `batch.size` yet, before sending anyway. Apache Kafka 3.9.x default: **0** — meaning "don't wait, send immediately, batch only opportunistically if requests happen to be in flight already."
  - Worth flagging precisely, since this lab pins **Kafka 3.9.2** (per file `02`): Kafka **4.0** changed this default from `0` to `5` ms, an explicit acknowledgment that the old default under-batched. That change doesn't apply to this lab's 3.9.x brokers — on `kf1`/`kf2`/`kf3`, `linger.ms=0` is still what you get unless you set it yourself.
- The trade-off in plain terms:
  - `linger.ms=0` sends every record (or whatever happened to accumulate) as fast as possible — lowest latency per message, but smaller/less efficient batches, more requests, worse compression ratios (Section 9 — compression works on whole batches, so tiny batches compress poorly).
  - Raising `linger.ms` (even a small amount, e.g. 5–20ms) deliberately trades a small amount of added latency for meaningfully larger, more efficient batches — fewer broker requests, better compression, generally higher sustained throughput.
- This is exactly the FinPay case study's producer contrast from file `02`, restated through a performance lens rather than a durability lens: the card-swipe ingestion service's `linger.ms=10` isn't just a style choice — it's a deliberate throughput-over-latency trade that this section explains the mechanism for.

---

# 18. Consumer-Side Fetch Tuning — `fetch.min.bytes` and `fetch.max.wait.ms`

- The consumer-side mirror of Section 17 — also entirely client-side (file `02`'s "consumer config" layer).
- **`fetch.min.bytes`** — the minimum amount of data the broker should try to have ready before answering a fetch request. Apache Kafka default: **1 byte** — meaning "respond as soon as there's anything at all" (subject to the wait cap below).
- **`fetch.max.wait.ms`** — the max time the broker will hold a fetch request open, waiting for `fetch.min.bytes` worth of data to become available, before answering anyway (even with less data than requested). Apache Kafka default: **500 ms**.
- What this pair actually controls: **latency vs. request efficiency on the read side**, the mirror image of Section 17's write-side trade-off.
  - Default behavior (`fetch.min.bytes=1`) means a consumer gets data almost immediately as it arrives — great for low-latency consumers, but means the consumer issues a large number of small fetch requests under low-throughput conditions.
  - Raising `fetch.min.bytes` (e.g. to a few KB) tells the broker "don't bother responding until you've got a meaningful chunk of data" — fewer, larger, more efficient fetch responses, at the cost of the consumer waiting slightly longer per batch (bounded by `fetch.max.wait.ms`, so it never waits forever even if the topic is quiet).
- Why this belongs in a performance lesson rather than a pure "how consumers work" lesson: a consumer configured for low per-request overhead (`fetch.min.bytes` raised) versus one left at the aggressive-low-latency default can show meaningfully different broker-side request load under the same message volume — this is a lever on the *consumer* side that changes *broker* CPU/network load, which is exactly the kind of cross-cutting effect this lesson is about.

---

# 19. Monitoring — How You'd Actually Detect Which Dimension Is the Bottleneck

- Everything in Sections 2–18 describes mechanisms and levers. This section is the missing piece: **how do you know which one is actually the problem**, rather than guessing and tuning blind?
- Kafka exposes broker-internal metrics via **JMX** — the standard mechanism most Kafka monitoring stacks (Prometheus JMX Exporter + Grafana, Datadog, etc.) scrape from.
- Two of the most directly diagnostic metrics, tying straight back to the I/O and Network sections above:
  - **`RequestHandlerAvgIdlePercent`** (`kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent`) — the average fraction of time the **I/O thread pool** (`num.io.threads`, Section 4) sits idle. A value near 1.0 means plenty of headroom; a value that's fallen well below that (commonly cited operational thresholds: below ~0.7 means threads are busy roughly 30%+ of the time and performance is starting to degrade, below ~0.2 points squarely at the I/O subsystem — disk write pressure or GC-induced stalls — as the actual bottleneck) is your signal to look at `num.io.threads`, disk throughput, or GC pauses (Section 10), not to start randomly changing network settings.
  - **`NetworkProcessorAvgIdlePercent`** (`kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent`) — the equivalent idle-fraction metric for the **network thread pool** (`num.network.threads`, Section 6). Healthy is commonly cited as staying above roughly 0.4; falling below roughly 0.3 points at network-thread saturation — often a connection storm or a spike in short-lived connections — rather than a disk problem.
- The broader operational habit this section is really arguing for:

```text
"Kafka feels slow"
        │
        ▼
Don't guess — check metrics first
        │
        ├── RequestHandlerAvgIdlePercent low?      → I/O bottleneck   (Sections 2–5)
        ├── NetworkProcessorAvgIdlePercent low?     → Network bottleneck (Sections 6–9)
        ├── GC pauses long / heap near max?          → RAM misconfigured (Sections 10–11)
        ├── CPU pegged / CPU credit balance near 0?  → CPU bottleneck   (Sections 12–13)
        └── fd errors / swap activity in metrics?     → OS misconfigured (Sections 14–16)
```

- This is the honest answer to "how do I tune Kafka performance": not a checklist you apply blindly, but a measurement-first loop — identify the saturated resource via metrics, apply the specific lever from the matching section above, re-measure.

---

# 20. Case Study — "Why Is My Producer Slow, and Why Is Consumer Lag Growing?"

- A platform team is running a topic on this lab's `kf1`/`kf2`/`kf3` cluster and starts seeing two symptoms at once:

```text
Symptom 1: Producer send() latency has crept up from ~5ms to ~180ms average.
Symptom 2: Consumer group lag, which normally sits near zero, is now
           growing steadily — currently 40,000 messages behind and rising.
```

- **Step 1 — resist the urge to guess.** Both symptoms *could* mean disk trouble, network trouble, or something else entirely. Pull metrics first (Section 19).
- **Step 2 — check the obvious broker-side signal first: `RequestHandlerAvgIdlePercent`.**
  - Reading: **0.09** on `kf2` (the current partition leader for this topic), versus a healthy ~0.9+ on `kf1` and `kf3`.
  - This immediately narrows the search: it's not a cluster-wide network problem (that would show up as low `NetworkProcessorAvgIdlePercent` on all three brokers), and it's isolated to one broker — `kf2`.
- **Step 3 — why would `kf2` specifically be I/O-starved?** Check what's different about `kf2`:
  - `kf2` is hosting this topic's leader partitions, so it's absorbing every produce request directly, plus serving every consume request, plus handling replication fetches from `kf1` and `kf3` — three sources of I/O-thread load landing on one broker.
  - `kf2`'s `log.dirs` is a single volume (Section 3 — this cluster hasn't been converted to JBOD yet), so every one of this topic's 6 partitions, if they're all leader-elected onto `kf2` (an unbalanced leader distribution — the same kind of imbalance file `01` warned about for partition placement generally), is competing for one disk's I/O queue.
- **Step 4 — rule out RAM/CPU as the root cause, not just I/O as a symptom.**
  - `kf2` is a `t3.small` — 2 GiB RAM (Section 11). Checking the JVM: heap usage is healthy, no unusual GC pause times in the logs. RAM isn't the culprit here.
  - CPU utilization on `kf2` is elevated but not pegged, and its CPU credit balance (Section 13) hasn't crashed to zero — so this isn't the burstable-instance throttling scenario, at least not yet.
  - Conclusion: this specific incident traces to **I/O**, specifically **unbalanced leader/partition placement concentrating disk load onto a single broker with a single `log.dirs` volume** — not Network, RAM, or CPU.
- **Step 5 — connect cause to the two symptoms.**
  - Producer latency rose because `acks=all` produce requests to `kf2` (the leader) can't be acknowledged until the write — and the replication that follows it — actually lands, and `kf2`'s I/O threads are backed up processing the queue.
  - Consumer lag is growing because those same overloaded I/O threads are also what serves fetch requests (Section 6's diagram) — a consumer's `fetch.max.wait.ms` (Section 18) window keeps expiring with less data than expected, and the consumer falls further behind with every cycle.
- **Step 6 — the fix, mapped directly to the sections above.**
  - Immediate: trigger a leader-rebalance (`kafka-leader-election.sh` / preferred-leader election) to spread this topic's leadership across `kf1`, `kf2`, and `kf3` more evenly, rather than concentrating it on one broker.
  - Structural: move `kf2` (and the other brokers) from a single `log.dirs` volume to JBOD across multiple disks (Section 3), so no single disk absorbs the full I/O load of every partition a broker happens to lead.
  - Verify: watch `RequestHandlerAvgIdlePercent` return toward its healthy baseline on all three brokers as the fix takes effect — that's the confirmation the actual bottleneck (I/O) was correctly identified and addressed, not just papered over.
- The point of walking through this end to end: the *symptoms* (slow producer, growing lag) were ambiguous by themselves — they could have pointed to Network, RAM, or CPU just as easily from the outside. The metrics in Section 19, checked in the right order, are what actually pinned it down to I/O specifically, and to *which broker and why*.

---

# 21. Senior Platform Engineer Mental Model

```text
                              "Kafka feels slow"
                                      │
                    ┌─────────┬───────┼───────┬─────────┐
                    │         │       │       │         │
                   I/O     Network   RAM     CPU        OS
                    │         │       │       │         │
              seq/random   threads  page   compress   fd limits
              log.dirs     socket   cache  TLS        swappiness
              io.threads   bufs    heap    bursting   filesystem
              flush        listen   size   (t3.small)  (XFS)
              behavior     sep.
                    │         │       │       │         │
                    └─────────┴───┬───┴───────┴─────────┘
                                  │
                         Diagnose with metrics
                         (RequestHandlerAvgIdlePercent,
                          NetworkProcessorAvgIdlePercent,
                          GC logs, CPU credit balance,
                          fd/swap counters)
                                  │
                                  ▼
                    Apply the ONE matching lever,
                    then re-measure — don't tune blind
```

- One-line summary:

> **Kafka performance isn't one number to chase — it's five hardware/OS resources and two client-side levers, each with its own failure signature, and the actual skill is measuring which one is saturated before touching a config.**

---

# 22. Interview Answer

If asked:

> **"How would you approach tuning Kafka for performance?"**

A weak answer:

> "I'd increase `num.io.threads` and `num.network.threads`, turn on compression, and give the brokers more RAM."

A strong Senior Platform Engineer answer:

> "I wouldn't start by changing configs — I'd start by measuring, because 'Kafka is slow' can mean five completely different things underneath: disk I/O, network, memory/page-cache pressure, CPU, or an OS-level limit like file descriptors or swapping. JMX metrics like `RequestHandlerAvgIdlePercent` and `NetworkProcessorAvgIdlePercent` tell me directly whether the I/O thread pool or the network thread pool is actually saturated, rather than guessing. Once I know which resource is the real bottleneck, the fix is specific to it — if it's I/O, that's `log.dirs` spread across multiple disks and `num.io.threads` sized to match; if it's network, that's socket buffer sizes and making sure replication traffic has its own internal listener separate from client traffic; if it's memory, it's almost never 'add more heap' — Kafka relies on OS page cache, so the fix is usually a smaller heap and more RAM left for the OS, or in extreme cases genuinely more RAM on the instance. On top of the broker-side resources, there are client-side levers — producer `batch.size`/`linger.ms` and consumer `fetch.min.bytes`/`fetch.max.wait.ms` — that shift the latency-vs-throughput trade-off without touching the broker at all. And OS-level basics matter more than people expect: file descriptor limits, `vm.swappiness` kept near zero, and filesystem choice — XFS is Kafka's own currently-recommended filesystem over EXT4 for the data directories."

Then, if pushed for a concrete example:

> "On a small lab cluster I diagnosed exactly this: producer latency and consumer lag both degraded at once, which looked ambiguous. `RequestHandlerAvgIdlePercent` on one specific broker was down near 0.09 while the other two brokers were healthy — that isolated it to I/O on one broker, not a cluster-wide network or CPU issue. The root cause was unbalanced partition leadership concentrating disk I/O onto a single-volume `log.dirs` on that one broker. The fix was rebalancing leadership and moving that broker to JBOD across multiple disks — and I confirmed the fix by watching that same metric recover, not just by the symptom going away."

That answer demonstrates the measurement-first mental model — diagnose the specific resource, apply the specific lever — rather than treating "performance tuning" as a fixed checklist of configs to paste in.
