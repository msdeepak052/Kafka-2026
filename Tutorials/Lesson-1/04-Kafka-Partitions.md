# 4. Kafka Partitions

**Partitions are probably the most important Kafka concept after topics.**
As a Senior Platform Engineer, you need to understand partitions deeply because they directly affect **scalability, throughput, ordering, replication, consumer parallelism, broker utilization, and storage**.

---

# 1. What is a Kafka Partition?

* A **partition is an ordered, append-only log inside a Kafka topic**.
* Every Kafka topic is made up of one or more partitions.
* Kafka actually stores records **inside partitions**, not directly inside the topic.
* Each partition has its own:

  * Records
  * Offsets
  * Leader
  * Replicas
  * Log segments

### Simple definition

> **Partition = The unit in Kafka where records are actually stored and ordered.**

For example:

```text
Topic: payments

        payments
           │
     ┌─────┼─────┐
     ▼     ▼     ▼
    P0     P1    P2
```

---

# 2. Easy Analogy 📚

Imagine a **large library**.

The library has a section called:

```text
PAYMENTS
```

Instead of putting every book into one giant shelf, you have multiple shelves:

```text
Payments Section

Shelf 0
Shelf 1
Shelf 2
Shelf 3
```

Think:

```text
Library       → Kafka Cluster
Section       → Topic
Shelf         → Partition
Book          → Kafka Record
Book position → Offset
```

Why multiple shelves?

Because multiple people can work with different shelves simultaneously.

That's exactly what partitions enable in Kafka:

> **Parallelism.**

---

# 3. Topic → Partitions → Records

Suppose:

```text
Topic: orders
Partitions: 3
```

Kafka could look like:

```text
orders
│
├── Partition 0
│    ├── Offset 0 → Order A
│    ├── Offset 1 → Order B
│    └── Offset 2 → Order C
│
├── Partition 1
│    ├── Offset 0 → Order D
│    ├── Offset 1 → Order E
│    └── Offset 2 → Order F
│
└── Partition 2
     ├── Offset 0 → Order G
     ├── Offset 1 → Order H
     └── Offset 2 → Order I
```

### Important:

There is **no single global offset** for the topic.

Instead:

```text
P0 → 0,1,2...
P1 → 0,1,2...
P2 → 0,1,2...
```

Offsets belong to individual partitions.

---

# 4. Why Does Kafka Need Partitions?

There are **four major reasons**.

### 1. Scalability

### 2. Parallelism

### 3. Ordering

### 4. Distribution across brokers

These are extremely important for interviews.

---

# 5. Partition → Scalability

Imagine you have:

```text
Topic: orders
1 partition
```

Everything goes through one partition:

```text
Producer
   │
   ▼
  P0
   │
   ▼
Broker
```

There is a limit to how much throughput that partition/broker can handle.

Now create:

```text
4 partitions
```

```text
             orders
                │
      ┌─────────┼─────────┐
      ▼         ▼         ▼
     P0        P1        P2        P3
```

Kafka can distribute the workload.

```text
P0 → Broker 1
P1 → Broker 2
P2 → Broker 3
P3 → Broker 1
```

Now multiple brokers can participate in serving the topic.

---

# 6. Partition → Parallelism

This is one of the **most important concepts**.

Suppose:

```text
Topic: orders
Partitions = 3
```

Consumer group:

```text
order-service

Consumer 1
Consumer 2
Consumer 3
```

Kafka can assign:

```text
P0 → Consumer 1
P1 → Consumer 2
P2 → Consumer 3
```

So processing happens in parallel.

```text
             orders
          ┌────┼────┐
          ▼    ▼    ▼
         P0   P1   P2
          │    │    │
          ▼    ▼    ▼
         C1   C2   C3
```

Instead of:

```text
P0
 │
 ▼
C1
 │
 ▼
One-by-one
```

you get:

```text
P0 → C1
P1 → C2
P2 → C3
```

### Key principle

> **Partitions determine the maximum parallelism of a consumer group.**

---

# 7. Very Important: Consumers vs Partitions

Suppose:

```text
Partitions = 3
Consumers = 3
```

Perfect:

```text
P0 → C1
P1 → C2
P2 → C3
```

---

### What if Consumers > Partitions?

```text
Partitions = 3

Consumers:
C1
C2
C3
C4
C5
```

Kafka can only assign:

```text
P0 → C1
P1 → C2
P2 → C3

C4 → idle
C5 → idle
```

So:

> **Adding consumers beyond the number of partitions does not increase parallelism for that consumer group.**

This is a **very common interview question**.

---

# 8. What if Partitions > Consumers?

Suppose:

```text
Partitions = 6
Consumers = 2
```

Kafka can assign multiple partitions to each consumer:

```text
C1 → P0, P1, P2

C2 → P3, P4, P5
```

So:

```text
6 partitions
      +
2 consumers
      ↓
Each consumer handles multiple partitions
```

---

# 9. Partition → Ordering

This is another extremely important concept.

Kafka guarantees ordering **within a partition**.

Example:

```text
P0

Offset 0 → Order Created
Offset 1 → Payment Completed
Offset 2 → Order Shipped
Offset 3 → Order Delivered
```

Kafka preserves:

```text
Created
   ↓
Payment
   ↓
Shipped
   ↓
Delivered
```

---

# 10. Kafka Does NOT Guarantee Global Ordering

Suppose:

```text
P0:
A → B → C

P1:
X → Y → Z
```

Kafka guarantees:

```text
A < B < C

X < Y < Z
```

But it doesn't guarantee:

```text
A < X < B < Y < C < Z
```

There is no global ordering across partitions.

### Interview answer

> **Kafka provides ordering within a partition, not across the entire topic.**

---

# 11. Partition Key

Now comes an extremely important concept.

When a producer sends a message, Kafka needs to determine:

> **Which partition should receive this record?**

The producer can provide a **key**.

Example:

```text
Customer ID = 101
```

Kafka can use the key to determine the partition.

Conceptually:

```text
hash(key) % number_of_partitions
```

For example:

```text
customer-101 → P0
customer-102 → P2
customer-103 → P1
```

---

# 12. Why Is the Key Important?

Suppose you have:

```text
Customer 101
```

and events:

```text
Order Created
Payment Completed
Order Shipped
Order Delivered
```

You want them in order.

If all events use:

```text
key = customer-101
```

Kafka can consistently route them to the same partition:

```text
customer-101
     │
     ▼
    P0
     │
     ├── Order Created
     ├── Payment Completed
     ├── Order Shipped
     └── Order Delivered
```

Now ordering for that customer is maintained.

---

# 13. Hot Partition 🔥

But partition keys can cause problems.

Suppose 80% of traffic belongs to:

```text
customer-999
```

And the key is:

```text
customer ID
```

Then:

```text
customer-999 → P0
```

You might get:

```text
P0 → 80% traffic 🔥🔥🔥
P1 → 7%
P2 → 7%
P3 → 6%
```

This is a **hot partition**.

Even though you have four partitions, one partition becomes the bottleneck.

### Platform Engineer thinking

Don't just ask:

> "How many partitions do we have?"

Ask:

> **"How evenly is traffic distributed across partitions?"**

---

# 14. Partition Distribution Across Brokers

Suppose:

```text
Kafka Cluster

Broker 1
Broker 2
Broker 3
```

Topic:

```text
orders
```

Partitions:

```text
P0
P1
P2
P3
P4
P5
```

Kafka can distribute them:

```text
Broker 1
├── P0
└── P3

Broker 2
├── P1
└── P4

Broker 3
├── P2
└── P5
```

Now workload is distributed across brokers.

---

# 15. Partitions + Replication

Partitions aren't necessarily stored only once.

Suppose:

```text
Partitions = 3
Replication Factor = 3
```

You might have:

```text
P0:
Broker 1 → Leader
Broker 2 → Replica
Broker 3 → Replica

P1:
Broker 2 → Leader
Broker 3 → Replica
Broker 1 → Replica

P2:
Broker 3 → Leader
Broker 1 → Replica
Broker 2 → Replica
```

So there are:

```text
3 partitions × 3 replicas
= 9 partition copies
```

This provides fault tolerance.

---

# 16. Partition Leader

Every partition has a **leader replica**.

Example:

```text
P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

Producer requests for P0 go to the leader.

Consumers also fetch data from the appropriate leader under normal operation.

```text
Producer
   │
   ▼
Broker 1
   │
   ▼
P0 Leader
```

Followers replicate the data.

---

# 17. What Happens If the Leader Broker Dies?

Suppose:

```text
P0

Broker 1 → Leader ❌
Broker 2 → Follower
Broker 3 → Follower
```

Kafka can elect an eligible in-sync replica:

```text
P0

Broker 2 → New Leader
Broker 3 → Follower
```

Then clients refresh metadata and continue operating.

This is where **ISR** becomes important.

---

# 18. Partition and ISR

ISR = **In-Sync Replicas**

Example:

```text
P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower

ISR:
[1,2,3]
```

If Broker 3 falls significantly behind:

```text
P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Lagging
```

ISR might become:

```text
[1,2]
```

Now:

```text
Broker 3
   ↓
Out of ISR
   ↓
Under-replicated partition
```

This is a major production monitoring signal.

---

# 19. Partition Storage

A partition is an **append-only log**.

Example:

```text
Partition 0

Offset
────────────────────────
0 → Order A
1 → Order B
2 → Order C
3 → Order D
4 → Order E
────────────────────────
```

New records are appended:

```text
5 → Order F
6 → Order G
```

Kafka generally doesn't randomly modify old records like a database row.

That's why Kafka is called a **distributed commit log**.

---

# 20. Partition → Log Segments

A partition can become huge.

Kafka therefore divides the partition log into **segments**.

Conceptually:

```text
Partition 0

├── Segment 00000000000000000000
├── Segment 00000000000000000010
├── Segment 00000000000000000020
└── Segment 00000000000000000030
```

Each segment contains records.

Why?

* Easier retention
* Easier deletion
* Better storage management
* Log rolling
* Efficient recovery

We'll cover **log segments** separately because they're important for Kafka storage administration.

---

# 21. How Many Partitions Should We Create?

This is a **Senior Platform Engineer decision**.

Don't simply say:

> "Let's create 100 partitions because more is better."

More partitions have costs.

### More partitions can mean:

* More metadata
* More files
* More replication traffic
* More leader management
* More recovery work
* More rebalancing work
* More resource consumption
* Potentially longer recovery/reassignment operations

But too few partitions can limit:

* Throughput
* Consumer parallelism
* Scalability

So you need a balance.

---

# 22. Example Partition Sizing Decision

Suppose an application expects:

```text
Normal traffic = 50 MB/s
Peak traffic = 200 MB/s
```

And based on testing, you determine:

```text
1 partition ≈ 50 MB/s
```

You might need at least:

```text
200 / 50 = 4 partitions
```

But you wouldn't blindly stop at exactly 4.

You'd consider:

* Future growth
* Consumer parallelism
* Broker capacity
* Replication
* Ordering
* Key distribution
* Recovery requirements

You might provision more partitions based on the architecture and expected growth.

### Important:

> **Partition count should be based on throughput + parallelism + growth + operational constraints.**

---

# 23. Can We Increase Partitions?

Yes.

Example:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders \
  --partitions 6
```

If currently:

```text
P0
P1
P2
```

you can increase to:

```text
P0
P1
P2
P3
P4
P5
```

### But...

You **cannot simply reduce** the partition count of an existing topic.

So partition count is an important **design decision**.

---

# 24. Why Increasing Partitions Can Be Dangerous

Suppose your application uses:

```text
key = customerId
```

Initially:

```text
3 partitions
```

Partition selection conceptually uses:

```text
hash(customerId) % 3
```

Later you increase to:

```text
6 partitions
```

Now:

```text
hash(customerId) % 6
```

Some keys may map to different partitions.

That can affect ordering assumptions.

Therefore:

> **Never increase partitions blindly when application-level ordering depends on partitioning.**

---

# 25. Producer → Partition Flow

Let's see the complete flow.

```text
                     Producer
                        │
                        │
                  Record + Key
                        │
                        ▼
                 Partitioning Logic
                        │
            ┌───────────┼───────────┐
            ▼           ▼           ▼
           P0          P1          P2
            │           │           │
            ▼           ▼           ▼
        Broker 1     Broker 2     Broker 3
```

The producer's partitioner decides where the record goes.

Depending on the producer configuration and whether a key is provided, Kafka may use different partition selection behavior.

---

# 26. Consumer → Partition Flow

Consumer group:

```text
orders
│
├── P0 ──────► Consumer 1
├── P1 ──────► Consumer 2
└── P2 ──────► Consumer 3
```

If Consumer 2 dies:

```text
Consumer 2 ❌
```

Kafka performs a **rebalance**.

Another consumer can take over P1:

```text
P1 ──────► Consumer 1
```

or another available consumer.

This is why partition assignment and consumer groups are closely related.

---

# 27. Partition Count Determines Consumer Parallelism

This is worth memorizing:

```text
Partitions = 6

Consumers = 1
→ 1 consumer handles 6 partitions

Consumers = 3
→ ~2 partitions per consumer

Consumers = 6
→ 1 partition per consumer

Consumers = 10
→ 4 consumers idle
```

Therefore:

> **Maximum active consumers in a consumer group is effectively bounded by the number of partitions.**

---

# 28. Real Production Scenario

Imagine:

```text
Payment Service
```

receives:

```text
100,000 events/sec
```

You create:

```text
payments
Partitions = 1
```

Now everything goes through:

```text
P0
```

You can't get much parallelism from consumers.

You increase to:

```text
20 partitions
```

Now:

```text
P0  → Consumer 1
P1  → Consumer 2
P2  → Consumer 3
...
P19 → Consumer 20
```

You can scale the consumer group horizontally.

```text
             payments
                 │
       ┌─────────┼─────────┐
       ▼         ▼         ▼
      P0        P1        P2 ... P19
       │         │         │
       ▼         ▼         ▼
      C1        C2        C3 ... C20
```

Much higher parallel processing capacity.

---

# 29. But More Partitions ≠ Automatically More Throughput

This is a **senior-level point**.

Suppose:

```text
100 partitions
```

but:

```text
1 broker
```

with:

```text
CPU = 95%
Disk = 95%
Network = saturated
```

Adding partitions won't magically fix the bottleneck.

Similarly:

```text
20 partitions
20 consumers
```

won't help if:

```text
Database = bottleneck
```

So always think about the **entire pipeline**:

```text
Producer
   ↓
Kafka
   ↓
Partitions
   ↓
Consumers
   ↓
Downstream service
   ↓
Database
```

---

# 30. Partition-Level Monitoring

As a Platform Engineer, important metrics include:

### Partition health

* Offline partitions
* Under-replicated partitions
* Preferred replica imbalance
* Leader imbalance

### Traffic

* Bytes in
* Bytes out
* Messages/sec

### Storage

* Partition size
* Log segment size
* Disk utilization

### Consumer side

* Consumer lag per partition

Example:

```text
payments

P0 → Lag: 100
P1 → Lag: 120
P2 → Lag: 95
P3 → Lag: 1,500,000 🔥
```

This immediately tells you:

> **P3 may be a hot partition or its consumer may be unhealthy.**

---

# 31. Important Troubleshooting Scenario

### Problem

Application says:

> "Kafka is slow."

Don't immediately blame Kafka.

Check:

```text
Producer
   ↓
Partition distribution
   ↓
Broker load
   ↓
Partition leader distribution
   ↓
Disk I/O
   ↓
Network
   ↓
Consumer lag
   ↓
Consumer processing
```

Suppose you find:

```text
P0 → 90% traffic
P1 → 3%
P2 → 3%
P3 → 4%
```

Then the likely issue is:

> **Uneven partition-key distribution → hot partition.**

---

# 32. Key Senior Platform Engineer Questions

When someone asks you to create a topic, ask:

### Partitioning

* How much throughput?
* Peak throughput?
* Expected growth?
* How many consumers?
* Required parallelism?

### Ordering

* Do we need ordering?
* Global ordering or per-key ordering?

### Key

* What is the partition key?
* Is the key distribution uniform?
* Could one key become extremely hot?

### Infrastructure

* How many brokers?
* Broker capacity?
* Disk capacity?
* Network capacity?

### Reliability

* Replication factor?
* `min.insync.replicas`?
* Failure requirements?

### Operations

* Retention?
* Reassignment?
* Monitoring?
* Disaster recovery?

---

# 33. Interview Questions

### Q1. What is a partition?

> A partition is an ordered, append-only log within a Kafka topic. It is the fundamental unit of storage, ordering, and parallelism in Kafka.

---

### Q2. Why does Kafka use partitions?

> Partitions allow Kafka to distribute data across brokers and enable parallel writes and reads, which provides scalability and higher throughput.

---

### Q3. Does Kafka guarantee ordering?

> Yes, but only within an individual partition. Kafka does not guarantee ordering across multiple partitions.

---

### Q4. What happens if consumers are greater than partitions?

> Some consumers remain idle because a partition can be assigned to only one consumer within a consumer group at a time.

---

### Q5. Can we reduce partitions after creating a topic?

> No, Kafka doesn't support simply reducing the partition count of an existing topic. Increasing partitions is supported, but it should be planned carefully because it can affect partition-key mapping and ordering assumptions.

---

### Q6. What causes a hot partition?

Common causes:

* Poor partition-key distribution
* Highly skewed keys
* One customer/account generating huge traffic
* Bad partitioning strategy

---

# 34. The Most Important Mental Model 🔥

Remember this:

```text
                         TOPIC
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
            P0            P1            P2
             │             │             │
             ▼             ▼             ▼
        Ordered Log   Ordered Log   Ordered Log
             │             │             │
             ▼             ▼             ▼
          Broker        Broker        Broker
             │             │             │
             └────── Replication ────────┘
                           │
                           ▼
                    Consumer Group
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
            C1            C2            C3
```

### Lock these 7 points into memory:

1. **Topic = logical stream**
2. **Partition = actual ordered log**
3. **Offset = position inside a partition**
4. **Partition = unit of parallelism**
5. **Ordering = guaranteed within partition**
6. **Consumer parallelism ≤ partition count**
7. **Partition key determines distribution and can create hot partitions**

### 🔥 Senior Platform Engineer one-liner

> **"When designing Kafka partitions, I primarily consider throughput, consumer parallelism, ordering requirements, partition-key distribution, broker capacity, replication, and future growth. I also monitor for hot partitions, leader imbalance, under-replication, and consumer lag."**

This is the foundation for the next major concepts: **partition leaders → replicas → ISR → replication factor → consumer groups → offsets → consumer lag → rebalancing**.
