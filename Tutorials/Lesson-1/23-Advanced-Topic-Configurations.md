# Kafka Advanced Topic Configuration — Complete Notes

---

# 1. Changing Topic Configuration

## 1.1 What is Topic Configuration?

When you create a Kafka topic, Kafka applies a set of configurations that control things such as:

```text
Topic
│
├── Retention
├── Cleanup policy
├── Segment size
├── Segment rolling
├── Message size
├── Compression
└── Leader-election behavior
```

Kafka has:

```text
Broker default
      │
      ▼
Topic configuration
      │
      ▼
Per-topic override
```

If you don't explicitly configure a property for a topic, the broker's default applies.

You can override many settings **for an individual topic**. Apache Kafka documents both setting overrides during topic creation and changing them later with `kafka-configs.sh`. ([Apache Kafka][1])

---

# 1.2 Easy Analogy

Think of a company warehouse.

The company has a global rule:

```text
Default:
Keep packages for 7 days
```

But for one special warehouse:

```text
Medical warehouse:
Keep packages for 30 days
```

You don't change the policy for every warehouse.

You override it for that particular warehouse.

Kafka:

```text
Broker default
      │
      ├── orders → 7 days
      ├── payments → 7 days
      └── audit → 30 days
```

---

# 1.3 Architecture

```text
                    Kafka Cluster
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
           Broker 1    Broker 2    Broker 3
             │
             │ Broker defaults
             │
             ▼
        ┌─────────────────────┐
        │      Topics         │
        │                     │
        │ orders              │
        │ payments            │
        │ audit               │
        └─────────────────────┘
             │
             ▼
      Topic-level override
```

---

# 1.4 Check Topic Configuration

Use:

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --describe
```

Example:

```text
Dynamic configs for topic orders are:
  retention.ms=259200000
  cleanup.policy=delete
```

---

# 1.5 Change Configuration

Suppose:

```text
orders
```

currently retains data for 7 days.

You want:

```text
3 days
```

Run:

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --add-config retention.ms=259200000
```

---

# 1.6 Verify

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --describe
```

---

# 1.7 Remove an Override

Suppose you explicitly configured:

```text
retention.ms=259200000
```

and want the topic to go back to the broker default.

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name orders \
  --alter \
  --delete-config retention.ms
```

Now the topic inherits the broker's default again. Apache documents this exact `--delete-config` pattern. ([Apache Kafka][1])

---

# 1.8 Configure During Topic Creation

You can also configure the topic during creation:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete
```

---

# 1.9 What Problem Does This Solve?

Without topic-level configuration:

```text
One broker default
      ↓
Every topic behaves similarly
```

But different workloads need different policies.

Example:

```text
application-logs
→ retain 7 days

audit-events
→ retain 1 year

customer-state
→ compact

temporary-events
→ retain 1 hour
```

Topic-level configuration allows this.

---

# 1.10 Platform Engineer Example

You have:

```text
orders
payments
audit-events
customer-state
```

You could have:

```text
orders
cleanup.policy=delete
retention.ms=7 days

payments
cleanup.policy=delete
retention.ms=30 days

audit-events
cleanup.policy=delete
retention.ms=365 days

customer-state
cleanup.policy=compact
```

This is exactly why topic-level configuration matters.

---

# 2. Kafka Log Segments and Indexes

This is one of the most important storage concepts.

## 2.1 The Problem

Imagine a partition containing:

```text
500 GB
```

You don't want Kafka to store it as:

```text
ONE 500 GB FILE
```

Instead, Kafka breaks the partition into **log segments**.

---

# 2.2 Architecture

```text
Topic
 │
 └── Partition 0
       │
       ├── Segment 1
       │    ├── .log
       │    ├── .index
       │    └── .timeindex
       │
       ├── Segment 2
       │    ├── .log
       │    ├── .index
       │    └── .timeindex
       │
       └── Segment 3
            ├── .log
            ├── .index
            └── .timeindex
```

The partition is therefore a sequence of segments.

---

# 2.3 Easy Analogy

Imagine a huge library.

Instead of:

```text
1 giant book containing 10 million pages
```

you have:

```text
Book 1
Book 2
Book 3
Book 4
...
```

And each book has an index.

Kafka works similarly.

---

# 2.4 What is a Segment?

A segment is a physical chunk of a partition's log.

Suppose:

```text
segment.bytes = 1 GB
```

You might have:

```text
P0

Segment 1 → 1 GB
Segment 2 → 1 GB
Segment 3 → 1 GB
Segment 4 → 700 MB ← active
```

The current segment is where Kafka is appending new records.

---

# 2.5 `segment.bytes`

Current Kafka documentation lists:

```text
segment.bytes = 1073741824
```

which is:

```text
1 GiB
```

It controls the maximum size of a segment. Larger segments mean fewer files but less granular retention/cleanup. ([Apache Kafka][1])

Example:

```bash
--config segment.bytes=1073741824
```

---

# 2.6 Why Not Make Segments Huge?

Suppose:

```text
segment.bytes = 100 GB
retention = 7 days
```

A segment might contain:

```text
Day 1
Day 2
Day 3
...
Day 7
```

Kafka's cleanup operates on segments.

So retention becomes less granular.

You might have data that is older than the retention target but still sitting inside a segment that isn't eligible as a whole.

---

# 2.7 Why Not Make Segments Tiny?

Suppose:

```text
segment.bytes = 1 MB
```

You could create thousands/millions of segment files.

That creates:

* More files
* More index files
* More filesystem operations
* More metadata overhead
* More segment-management overhead

So segment sizing is a tradeoff.

---

# 2.8 `segment.ms`

Kafka can roll a segment based on time as well.

Example:

```bash
--config segment.ms=3600000
```

Meaning:

```text
1 hour
```

Even if the segment isn't full, Kafka can roll it after that time.

Apache documents `segment.ms` as forcing a log roll so retention/compaction can operate on older segments. ([Apache Kafka][1])

---

# 2.9 Segment Rolling

Example:

```text
Active Segment
      │
      │ reaches segment.bytes
      ▼
Segment closed
      │
      ▼
New segment created
      │
      ▼
New active segment
```

Or:

```text
segment.ms expires
      │
      ▼
Segment rolled
```

---

# 2.10 Indexes

Kafka also maintains indexes to efficiently locate records.

Suppose:

```text
P0

Offset 0
Offset 1
Offset 2
...
Offset 1,000,000
```

A consumer asks:

```text
Give me offset 700000
```

Kafka doesn't need to scan every record from offset 0.

The offset index helps map:

```text
Kafka Offset
     ↓
Approximate physical file position
```

Example:

```text
Offset Index

Offset      File Position

100000  →   20 MB
200000  →   41 MB
300000  →   63 MB
```

---

# 2.11 `segment.index.bytes`

Current Kafka documentation lists:

```text
segment.index.bytes = 10 MiB
```

It controls the maximum size of the offset index. Kafka notes that this generally should not need to be changed. ([Apache Kafka][1])

---

# 2.12 `segment.jitter.ms`

This adds random variation to scheduled segment rolling.

Why?

Imagine 1,000 partitions all created at the same time.

If every segment rolls at exactly the same moment:

```text
10:00:00

Partition 1 → roll
Partition 2 → roll
Partition 3 → roll
...
Partition 1000 → roll
```

That can create a burst.

Jitter spreads those operations out.

---

# 2.13 Platform Engineer Mental Model

```text
Partition
   │
   ▼
Segments
   │
   ├── Log file
   ├── Offset index
   └── Time index
```

And:

```text
segment.bytes
      ↓
Size-based rolling

segment.ms
      ↓
Time-based rolling
```

---

# 3. Log Cleanup Policies

Now that you understand segments, cleanup becomes much easier.

Kafka needs a way to answer:

> **When can old data be removed?**

The main policies are:

```text
delete
compact
delete,compact
```

Kafka's current topic configuration documents these policies and their behavior. ([Apache Kafka][1])

---

# 3.1 `delete`

```text
cleanup.policy=delete
```

Means:

> Remove old segments according to retention rules.

Example:

```text
retention.ms=7 days
```

---

# 3.2 `compact`

```text
cleanup.policy=compact
```

Means:

> Keep the latest value for each key in the compacted portion of the log.

---

# 3.3 `delete,compact`

```text
cleanup.policy=delete,compact
```

Means:

```text
Compaction
+
Retention-based deletion
```

---

# 3.4 Architecture

```text
                  Kafka Topic
                       │
                       ▼
                cleanup.policy
                       │
             ┌─────────┼─────────┐
             ▼         ▼         ▼
           delete   compact   delete,compact
             │         │         │
             ▼         ▼         ▼
          Retention  Key-based   Both
          cleanup    cleanup
```

---

# 3.5 Example

### Logs

```text
application-logs
cleanup.policy=delete
retention.ms=7 days
```

### Current state

```text
customer-state
cleanup.policy=compact
```

### State + expiration

```text
device-state
cleanup.policy=delete,compact
retention.ms=30 days
```

---

# 3.6 What Problem Does This Solve?

Without cleanup:

```text
Kafka disk
│
├── Day 1
├── Day 2
├── Day 3
...
└── Forever
```

Disk eventually fills.

Cleanup gives Kafka controlled storage lifecycle.

---

# 4. Log Cleanup — Delete

Now let's go deeper into the `delete` policy.

---

# 4.1 What Does Delete Actually Delete?

Important:

> Kafka generally deletes **old segments**, not individual records one by one.

Example:

```text
Partition
│
├── Segment 1 → old
├── Segment 2 → old
├── Segment 3 → recent
└── Segment 4 → active
```

Cleanup:

```text
Segment 1 ❌
Segment 2 ❌
Segment 3 ✅
Segment 4 ✅
```

Kafka's documentation specifically explains that retention and cleaning operate a file/segment at a time. ([Apache Kafka][1])

---

# 4.2 `retention.ms`

Example:

```text
retention.ms=604800000
```

= 7 days.

Meaning:

```text
Records/segments older than retention
          ↓
Become eligible for deletion
```

Current Kafka documentation lists 7 days as the default `retention.ms`. ([Apache Kafka][1])

---

# 4.3 `retention.bytes`

Example:

```text
retention.bytes=107374182400
```

= 100 GiB per partition.

Important:

> `retention.bytes` is applied **per partition**.

So if:

```text
10 partitions
retention.bytes = 100 GiB
```

the topic can retain approximately:

```text
10 × 100 GiB
= 1 TiB
```

subject to the actual cleanup/segment behavior. Kafka explicitly documents this per-partition behavior. ([Apache Kafka][1])

---

# 4.4 Time + Size Retention

Suppose:

```text
retention.ms = 7 days
retention.bytes = 100 GB
```

Now Kafka has two constraints.

Conceptually:

```text
             Retention
                 │
        ┌────────┴────────┐
        ▼                 ▼
    Time limit        Size limit
     7 days             100 GB
        │                 │
        └────────┬────────┘
                 ▼
        Old segments eligible
```

You should think of them as independent retention constraints.

---

# 4.5 Cleanup Check Interval

Kafka periodically checks whether segments are eligible for deletion.

The broker-level setting:

```text
log.retention.check.interval.ms
```

controls how frequently Kafka checks.

So don't expect:

```text
Exactly 7 days
       ↓
Instant deletion at 7d 00:00:00
```

Think:

```text
Retention threshold reached
       ↓
Eligible
       ↓
Cleanup check
       ↓
Segment deletion
```

---

# 4.6 `file.delete.delay.ms`

Current Kafka topic configuration documents:

```text
file.delete.delay.ms
```

with a default of 1 minute.

It controls how long Kafka waits before deleting a file from the filesystem. ([Apache Kafka][1])

---

# 4.7 Real Case Study — Application Logs

Suppose:

```text
Topic = application-logs
Traffic = 100 GB/day
Retention = 7 days
```

Approximate storage:

```text
100 GB × 7
≈ 700 GB per topic
```

If the topic has multiple partitions, distribution happens across those partitions.

Architecture:

```text
Applications
     │
     ▼
application-logs
     │
     ├── P0
     ├── P1
     ├── P2
     └── ...
          │
          ▼
       Segments
          │
          ▼
    Retention cleanup
          │
          ▼
    Old segments removed
```

This prevents unbounded disk growth.

---

# 5. Log Compaction — Theory

Now we move to a completely different problem.

## 5.1 Delete Retention Isn't Always What You Want

Suppose:

```text
customer-state
```

contains:

```text
customer-101 → Delhi
customer-101 → Mumbai
customer-101 → Bangalore
customer-101 → Hyderabad
```

If you use normal time-based deletion:

```text
Old records → deleted
```

you could eventually lose the current state entirely.

---

# 5.2 What Compaction Solves

Compaction answers:

> **Can I remove obsolete versions of a keyed record while retaining the latest state?**

Example:

```text
customer-101 → Delhi
customer-101 → Mumbai
customer-101 → Bangalore
customer-101 → Hyderabad
```

Eventually:

```text
customer-101 → Hyderabad
```

Kafka's log compaction is specifically designed to retain the latest value for each key in a partition. ([Apache Kafka][2])

---

# 5.3 Easy Analogy

Imagine an employee's address history:

```text
Employee 101
│
├── Delhi
├── Mumbai
├── Bangalore
└── Hyderabad
```

If your system only needs:

```text
Current address
```

you don't need every old address in the final state snapshot.

Compaction is like cleaning the filing cabinet so only the latest version for each employee remains.

---

# 5.4 Architecture

```text
Producer
   │
   │ key + value
   ▼
Topic
   │
   ▼
Partition
   │
   ├── Segment
   ├── Segment
   └── Segment
          │
          ▼
      Log Cleaner
          │
          ▼
      Find duplicate keys
          │
          ▼
      Keep latest value
          │
          ▼
      Cleaned segments
```

Compaction runs as a background process and doesn't simply happen at the instant a duplicate key arrives. ([Apache Kafka][2])

---

# 5.5 Very Important: Compaction Does NOT Mean "One Record Per Key Immediately"

Suppose:

```text
customer-101 → A
customer-101 → B
customer-101 → C
```

Immediately after producing:

```text
A
B
C
```

you may still see:

```text
A
B
C
```

Compaction is asynchronous.

Later, eligible segments are cleaned.

Eventually:

```text
C
```

may remain as the latest value.

---

# 5.6 Ordering Is Not Changed

Compaction does not reorder remaining records.

Kafka's design documentation states that compaction removes records but does not reorder the remaining messages, and offsets remain unchanged. ([Apache Kafka][2])

So:

```text
Before:

Offset 10 → A
Offset 11 → B
Offset 12 → C
Offset 13 → D
```

After compaction:

```text
Offset 12 → C
Offset 13 → D
```

Offsets don't become:

```text
0
1
```

They retain their original positions.

---

# 5.7 Tombstones

Compacted topics also support deletion.

Suppose:

```text
customer-101 → Hyderabad
```

Now the producer sends:

```text
customer-101 → null
```

This is a **tombstone**.

Conceptually:

```text
customer-101 → Hyderabad
              ↓
customer-101 → null
              ↓
        delete marker
```

The tombstone tells compaction:

> This key should eventually be removed.

Kafka retains tombstones for a configurable period before cleaning them. The current documented default for `delete.retention.ms` is 24 hours. ([Apache Kafka][1])

---

# 6. Log Compaction — Practice

Now let's implement it.

---

# 6.1 Create a Compacted Topic

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic customer-state \
  --partitions 3 \
  --replication-factor 1 \
  --config cleanup.policy=compact
```

---

# 6.2 Verify

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  --entity-type topics \
  --entity-name customer-state \
  --describe
```

You should see:

```text
cleanup.policy=compact
```

---

# 6.3 Produce Keyed Records

Run:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic customer-state \
  --property parse.key=true \
  --property key.separator=:
```

Send:

```text
customer-101:Delhi
customer-102:Mumbai
customer-101:Bangalore
customer-103:Pune
customer-101:Hyderabad
```

Logical log:

```text
Offset 0 → customer-101 → Delhi
Offset 1 → customer-102 → Mumbai
Offset 2 → customer-101 → Bangalore
Offset 3 → customer-103 → Pune
Offset 4 → customer-101 → Hyderabad
```

---

# 6.4 What Does Compaction Eventually Aim For?

Conceptually:

```text
customer-101 → Hyderabad
customer-102 → Mumbai
customer-103 → Pune
```

The older versions of `customer-101` become obsolete.

---

# 6.5 Important Configuration Parameters

### `cleanup.policy`

```text
compact
```

Enables compaction.

---

### `min.cleanable.dirty.ratio`

Controls how much of the log must be considered "dirty" before a log becomes eligible for cleaning.

Think:

```text
Clean data
+
Updated/duplicate data
        ↓
Dirty ratio
```

If the ratio is too high:

```text
Less frequent cleaning
```

If lower:

```text
More frequent cleaning
```

Tradeoff:

```text
More cleaning
    ↓
More CPU/I/O

Less cleaning
    ↓
More duplicate data retained
```

---

### `min.compaction.lag.ms`

Minimum time a record remains uncompacted.

Useful when you don't want newly written records to be compacted immediately.

---

### `max.compaction.lag.ms`

Places an upper bound on how long a record can remain ineligible for compaction.

Current Kafka topic configuration documents both of these controls. ([Apache Kafka][1])

---

### `delete.retention.ms`

Controls how long tombstone records are retained.

Current default:

```text
86400000 ms
= 24 hours
```

([Apache Kafka][1])

---

# 6.6 Broker-Side Cleaner Configuration

The broker has log-cleaner settings such as:

```text
log.cleaner.enable
log.cleaner.threads
log.cleaner.min.cleanable.ratio
log.cleaner.io.buffer.size
log.cleaner.dedupe.buffer.size
log.cleaner.io.max.bytes.per.second
```

These affect the cluster's background log-cleaning machinery. ([Apache Kafka][3])

---

# 6.7 Real Case Study — Customer State

Suppose:

```text
customer-state
```

contains 100 million updates.

But there are only:

```text
10 million customers
```

Without compaction:

```text
100M records
```

With compaction, the retained state can eventually approach:

```text
~10M latest keyed records
```

The exact physical result depends on cleaner progress and topic configuration.

This is useful for:

* Customer state
* Device state
* Configuration
* Account state
* User profile state

---

# 7. Unclean Leader Election

Now we're moving from:

```text
Storage
```

to:

```text
Availability vs Data Durability
```

---

# 7.1 Normal Replication

Suppose:

```text
Replication Factor = 3

P0

B1 → Leader
B2 → Follower
B3 → Follower
```

All are in ISR:

```text
ISR = B1,B2,B3
```

---

# 7.2 Leader Failure

B1 fails:

```text
B1 ❌
```

Kafka can elect:

```text
B2 → New Leader
```

because B2 was in ISR.

This is the normal/clean case.

---

# 7.3 Problem Scenario

Now suppose:

```text
P0

B1 → Leader
B2 → Follower
B3 → Follower
```

But:

```text
ISR = B1
```

B2 and B3 are behind.

Now B1 fails:

```text
B1 ❌
```

Kafka doesn't have a fully caught-up ISR follower available.

---

# 7.4 Two Choices

### Choice 1 — Don't elect out-of-sync replica

```text
No suitable ISR leader
       ↓
Partition unavailable
       ↓
Data preserved
```

### Choice 2 — Elect an out-of-sync replica

```text
B2 becomes leader
       ↓
Partition available
       ↓
But B2 may be missing records
       ↓
Potential data loss
```

Choice 2 is:

# Unclean Leader Election

---

# 7.5 Configuration

```text
unclean.leader.election.enable=true
```

allows a non-ISR replica to become leader as a last resort.

Current Kafka documentation warns explicitly that this may cause data loss and documents the default as `false`. ([Apache Kafka][1])

---

# 7.6 Easy Analogy

Imagine:

```text
Server A → latest database copy
Server B → latest database copy
Server C → yesterday's database copy
```

A and B fail.

You can:

### Wait

```text
Service unavailable
but data remains safe
```

or:

### Use C

```text
Service comes back
but yesterday's data may be missing
```

That's:

```text
Availability
     vs
Durability
```

---

# 7.7 Architecture

```text
                 Partition P0
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
         B1         B2         B3
       Leader     Follower    Follower
          │
          └── ISR
              │
              ▼
         Leader Election
              │
        ┌─────┴─────┐
        ▼           ▼
    ISR replica   Non-ISR
       │             │
       ▼             ▼
  Clean election  Unclean election
                      │
                      ▼
               Possible data loss
```

---

# 7.8 Platform Engineer Recommendation

For critical data such as:

```text
Payments
Orders
Financial transactions
Audit events
```

you generally prioritize durability.

So:

```text
unclean.leader.election.enable=false
```

is the safer posture.

For workloads where:

```text
Availability > potential loss
```

you might consider unclean election, but this should be an explicit architectural decision.

---

# 7.9 KRaft Note

Since you've already learned KRaft:

In current Kafka/KRaft documentation, dynamically enabling unclean leader election may wait for the periodic election thread; Kafka documents that `kafka-leader-election.sh --election-type unclean` can trigger an unclean election immediately when needed. ([Apache Kafka][1])

---

# 8. Large Messages in Kafka

Now the final topic.

---

# 8.1 What is a Large Message?

Kafka has limits on the size of records/batches that can be produced and consumed.

Suppose:

```text
Application
   │
   ▼
15 MB JSON
   │
   ▼
Kafka
```

If Kafka's configured limits are smaller:

```text
15 MB
  ↓
Kafka limit = 1 MB
  ↓
❌ Rejected
```

---

# 8.2 Why Is This a Problem?

Large messages increase:

```text
Network traffic
     +
Broker memory pressure
     +
Replication traffic
     +
Consumer memory usage
     +
Latency
     +
Recovery time
```

For example:

```text
1 KB × 1 million messages
```

versus:

```text
10 MB × 1 million messages
```

The second workload is enormous from a network/storage perspective.

---

# 8.3 Architecture

Large-message handling crosses several layers:

```text
             Producer
                │
                ▼
       max.request.size
                │
                ▼
             Broker
                │
                ▼
       message.max.bytes
                │
                ▼
             Topic
                │
                ▼
       max.message.bytes
                │
                ▼
          Replication
                │
                ▼
            Consumer
                │
        ┌───────┴────────┐
        ▼                ▼
 fetch.max.bytes   max.partition.fetch.bytes
```

---

# 8.4 Producer Limit

Producer:

```text
max.request.size
```

Controls the maximum request size the producer will send.

Example:

```text
max.request.size=10485760
```

= 10 MiB.

If your producer cannot construct/send a request large enough:

```text
Large message
     ↓
Producer limit
     ↓
❌ Failure
```

---

# 8.5 Broker Limit

Broker:

```text
message.max.bytes
```

controls the largest record batch the broker accepts.

---

# 8.6 Topic-Level Limit

You can override it per topic:

```text
max.message.bytes
```

Current Kafka 4.3 documentation lists:

```text
max.message.bytes
```

as the largest record-batch size allowed by Kafka for that topic, with a documented default of about 1 MiB. ([Apache Kafka][1])

---

# 8.7 Example

Suppose:

```text
Producer max.request.size = 10 MB
```

but:

```text
Topic max.message.bytes = 1 MB
```

Application sends:

```text
5 MB
```

Flow:

```text
Producer
  │
  │ 5 MB
  ▼
Broker
  │
  │ Topic limit = 1 MB
  ▼
❌ Rejected
```

Increasing only the producer isn't enough.

---

# 8.8 Consumer Side

Suppose Kafka accepts:

```text
5 MB record batch
```

But the consumer has very small fetch limits.

Relevant settings include:

```text
fetch.max.bytes
max.partition.fetch.bytes
```

These control how much data the consumer fetches.

The broker's replica fetch settings also matter for replication. Kafka documents that `replica.fetch.max.bytes` is a per-partition fetch target and that the first record batch may still be returned even if it exceeds the nominal setting so progress can be made. 

---

# 8.9 Producer → Broker → Consumer

For a 5 MB record:

```text
                 5 MB record
                     │
                     ▼
                  Producer
                     │
             max.request.size
                     │
                     ▼
                   Broker
                     │
             message.max.bytes
                     │
                     ▼
                   Topic
                     │
                     ▼
               Replication
                     │
                     ▼
                 Consumer
                     │
        max.partition.fetch.bytes
                     │
                     ▼
                 Application
```

All relevant limits must be compatible.

---

# 8.10 Why Not Simply Increase Everything?

You could configure:

```text
Producer = 50 MB
Broker = 50 MB
Topic = 50 MB
Consumer = 50 MB
```

But that doesn't mean it's a good architecture.

You may create:

```text
High memory usage
       ↓
High network bandwidth
       ↓
High replication cost
       ↓
High latency
       ↓
Slow recovery
```

---

# 8.11 Better Architecture for Large Objects

Suppose you're sending a:

```text
50 MB PDF
```

Instead of:

```text
Application
     │
     ▼
Kafka
     │
     └── 50 MB PDF
```

a better pattern is often:

```text
                 Application
                 /         \
                /           \
               ▼             ▼
        Object Storage      Kafka
             │                │
             │                ▼
        50 MB PDF        Small Event
                              │
                              ▼
                     "document available"
```

Example event:

```json
{
  "orderId": "12345",
  "documentId": "doc-987",
  "storageLocation": "object-storage/path/doc-987"
}
```

Kafka carries the event/metadata.

Object storage carries the large payload.

---

# 8.12 When Large Kafka Messages Are Reasonable

There are legitimate cases where you may increase Kafka message limits.

For example:

```text
5 MB event
```

might be acceptable if:

* Throughput is controlled
* Consumers are designed for it
* Memory is sized appropriately
* Network capacity is sufficient
* Replication impact is understood
* The payload genuinely belongs in the event stream

But don't make:

```text
50 MB
100 MB
500 MB
```

your default design without a strong reason.

---

# 8.13 Platform Engineer Troubleshooting

Suppose application team says:

> "Kafka producer is throwing RecordTooLargeException."

Check:

```text
Producer max.request.size
          ↓
Topic max.message.bytes
          ↓
Broker message.max.bytes
```

Then check:

```text
Consumer fetch limits
          ↓
Replica fetch limits
```

Mental model:

```text
Producer
   ↓
Can producer send it?
   ↓
Broker
   ↓
Will broker accept it?
   ↓
Replication
   ↓
Can replicas fetch it?
   ↓
Consumer
   ↓
Can consumer fetch it?
```

---

# Putting Everything Together

These eight concepts are actually connected.

```text
                    Kafka Topic
                         │
                         ▼
                  Topic Configuration
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
      Segments        Cleanup       Message Size
          │              │              │
          │         ┌────┴────┐         │
          │         ▼         ▼         │
          │       Delete    Compact     │
          │         │         │         │
          ▼         ▼         ▼         ▼
       Indexes   Retention  Latest    Producer/
                              Key      Broker/
                                       Consumer
```

And replication is another important branch:

```text
                    Partition
                       │
                       ▼
                    Leader
                       │
                       ▼
                      ISR
                       │
                Leader failure
                       │
              ┌────────┴────────┐
              ▼                 ▼
         ISR replica        Non-ISR replica
              │                 │
              ▼                 ▼
        Clean election     Unclean election
                                  │
                                  ▼
                           Possible data loss
```

---

# A Real Production Case Study

Imagine you're running an e-commerce Kafka cluster.

You have:

```text
orders
payments
customer-state
application-logs
```

## `orders`

```text
cleanup.policy=delete
retention.ms=7 days
```

Why?

Orders are event data and the business only needs a replay window of 7 days.

---

## `payments`

```text
cleanup.policy=delete
retention.ms=30 days
```

Why?

Longer replay window.

---

## `customer-state`

```text
cleanup.policy=compact
```

Messages:

```text
customer-101 → Delhi
customer-101 → Mumbai
customer-101 → Bangalore
```

Eventually:

```text
customer-101 → Bangalore
```

Why?

You care about latest state.

---

## `application-logs`

```text
cleanup.policy=delete
retention.ms=3 days
segment.bytes=...
```

Why?

Logs are high volume and should not fill the Kafka disks indefinitely.

---

## Large events

Application tries:

```text
25 MB JSON
```

Platform team checks:

```text
Producer
   ↓
max.request.size

Broker
   ↓
message.max.bytes

Topic
   ↓
max.message.bytes

Consumer
   ↓
fetch limits
```

If the event is unnecessarily large:

```text
Move payload → object storage
Kafka → metadata/event
```

---

## Broker failure

Suppose:

```text
orders-P0

B1 → Leader
B2 → ISR
B3 → ISR
```

B1 fails.

Kafka can elect:

```text
B2
```

No problem.

But if:

```text
ISR = B1
```

and B1 fails:

```text
No ISR replica available
```

With:

```text
unclean.leader.election.enable=false
```

Kafka prioritizes avoiding data loss over immediately making the partition available.

---

# Platform Engineer Decision Table

| Requirement                           | Configuration/concept                          |
| ------------------------------------- | ---------------------------------------------- |
| Limit how long data remains           | `retention.ms`                                 |
| Limit partition storage               | `retention.bytes`                              |
| Control segment size                  | `segment.bytes`                                |
| Force periodic segment rolling        | `segment.ms`                                   |
| Locate offsets efficiently            | Offset index                                   |
| Remove old historical segments        | `cleanup.policy=delete`                        |
| Keep latest value per key             | `cleanup.policy=compact`                       |
| Do both                               | `cleanup.policy=delete,compact`                |
| Delete a key from compacted state     | Tombstone                                      |
| Control tombstone retention           | `delete.retention.ms`                          |
| Control compaction eligibility        | `min.cleanable.dirty.ratio`                    |
| Delay compaction                      | `min.compaction.lag.ms`                        |
| Bound compaction delay                | `max.compaction.lag.ms`                        |
| Avoid data loss during leader failure | `unclean.leader.election.enable=false`         |
| Allow non-ISR leader as last resort   | `unclean.leader.election.enable=true`          |
| Allow larger producer requests        | `max.request.size`                             |
| Allow larger broker record batches    | `message.max.bytes`                            |
| Per-topic message limit               | `max.message.bytes`                            |
| Consumer fetch size                   | `fetch.max.bytes`, `max.partition.fetch.bytes` |

---

# 🔥 The Most Important Things to Remember

### 1. Segments

```text
Partition
   ↓
Many segments
   ↓
Each segment has log + indexes
```

### 2. Delete

```text
cleanup.policy=delete
       ↓
Retention
       ↓
Old segments become eligible
       ↓
Deleted
```

### 3. Compact

```text
cleanup.policy=compact
       ↓
Same key appears multiple times
       ↓
Older versions become obsolete
       ↓
Latest value retained
```

### 4. Tombstone

```text
key + null
   ↓
Delete marker
   ↓
Eventually key removed by compaction
```

### 5. Unclean Election

```text
Leader fails
   ↓
ISR available?
   ├── YES → clean election
   │
   └── NO
        │
        ├── unclean=false → preserve data, partition may remain unavailable
        │
        └── unclean=true → restore availability, possible data loss
```

### 6. Large Messages

```text
Producer
   ↓
max.request.size
   ↓
Broker
   ↓
message.max.bytes
   ↓
Topic
   ↓
max.message.bytes
   ↓
Consumer
   ↓
fetch limits
```

### 7. Topic Configuration

```text
Broker Default
      ↓
Topic Override
      ↓
Specific topic behaves differently
```

This is the **Senior Platform Engineer mental model** I would retain from this section:

> **Topic configuration controls the lifecycle and operational behavior of Kafka data: segments determine how the log is physically organized, retention/cleanup determines what can be removed, compaction determines which keyed state is retained, leader-election policy determines the availability-vs-durability tradeoff, and message-size settings determine how large records can safely flow through the producer → broker → replica → consumer path.** ([Apache Kafka][1])

[1]: https://kafka.apache.org/43/configuration/topic-configs/?utm_source=chatgpt.com "Topic Configs | Apache Kafka"
[2]: https://kafka.apache.org/090/design/design/?utm_source=chatgpt.com "Design | Apache Kafka"
[3]: https://kafka.apache.org/43/configuration/broker-configs/?utm_source=chatgpt.com "Broker Configs | Apache Kafka"
