# Kafka Cluster Size

For a **Senior Platform Engineer**, don't think of Kafka cluster size as simply:

> "How many brokers do I need?"

Think of it as:

> **How many brokers are required to handle traffic, storage, replication, failures, and future growth?**

---

## 1. What is Kafka Cluster Size?

A Kafka cluster consists of multiple **brokers** working together.

```text
                 Kafka Cluster
                      │
       ┌──────────────┼──────────────┐
       │              │              │
    Broker-1       Broker-2       Broker-3
       │              │              │
    Partitions      Partitions      Partitions
```

Example:

```text
Topic: orders
Partitions: 6
Replication Factor: 3
```

The partitions and their replicas are distributed across the brokers.

---

# 2. Why Do We Need Multiple Brokers?

A single broker:

```text
Producer
   │
   ▼
Broker-1
```

has a major problem:

```text
Broker-1 ❌
   ↓
Kafka unavailable
```

With 3 brokers:

```text
             Kafka Cluster

        ┌────────┼────────┐
        ▼        ▼        ▼
       B1       B2       B3
```

If B1 fails:

```text
B1 ❌

B2 ✅
B3 ✅

Kafka can continue serving
```

This is one of the major reasons Kafka clusters are normally deployed with multiple brokers.

<img width="1617" height="916" alt="image" src="https://github.com/user-attachments/assets/96f0372a-68c3-41c2-8833-b6b88ba220f7" />

<img width="1617" height="916" alt="image" src="https://github.com/user-attachments/assets/6bb625cd-fdb0-4287-93b5-5891e41524e4" />

<img width="1617" height="916" alt="image" src="https://github.com/user-attachments/assets/4adc2008-cd96-496a-adc4-91b713a87194" />

---

# 3. Analogy

Imagine a **warehouse company**.

### One warehouse

```text
Warehouse-1
```

All products are there.

If it burns down:

```text
❌ Everything unavailable
```

### Three warehouses

```text
Warehouse-1
Warehouse-2
Warehouse-3
```

Products are replicated across warehouses.

If one warehouse goes down:

```text
Warehouse-1 ❌
Warehouse-2 ✅
Warehouse-3 ✅
```

Business continues.

Kafka brokers are like those warehouses, while **partition replicas** are like copies of inventory.

---

# 4. What Actually Determines Cluster Size?

There are several major factors:

```text
Kafka Cluster Size
       │
       ├── Traffic
       ├── Storage
       ├── Partitions
       ├── Replication
       ├── Network
       ├── CPU
       ├── Memory
       ├── Failure tolerance
       └── Future growth
```

Don't size Kafka based on CPU alone.

---

# 5. Factor 1 — Throughput

Suppose your application produces:

```text
1 GB/sec
```

and one broker can safely handle approximately:

```text
500 MB/sec
```

Then:

```text
1 GB/sec
───────
500 MB/sec

≈ 2 brokers
```

But you shouldn't deploy exactly 2.

Why?

Because if one fails:

```text
B1 ❌
B2 must handle everything
```

and your cluster has no spare capacity.

So you might choose:

```text
3 brokers
```

with headroom.

---

# 6. Factor 2 — Storage

Suppose:

```text
Incoming data = 500 GB/day
Retention = 7 days
```

Raw storage:

```text
500 GB × 7
= 3.5 TB
```

Now replication factor:

```text
RF = 3
```

Approximate total storage required:

```text
3.5 TB × 3
= 10.5 TB
```

Then add operational headroom.

You **should not plan to fill disks to 100%**.

Example:

```text
Required replicated data ≈ 10.5 TB

+ headroom
+ replication/recovery overhead
+ operational safety margin
```

So you might provision significantly more than 10.5 TB.

---

# 7. Factor 3 — Replication Factor

Suppose:

```text
Replication Factor = 3
```

and:

```text
3 brokers
```

Example:

```text
Partition P0

B1 → Leader
B2 → Replica
B3 → Replica
```

If B1 fails:

```text
B1 ❌

B2 → can become leader
B3 → replica
```

Therefore:

```text
Replication Factor 3
        +
3 brokers
        ↓
Can tolerate a broker failure
```

---

# 8. Important Rule

For a normal production Kafka cluster:

```text
Number of brokers >= Replication Factor
```

For example:

```text
RF = 3

Minimum sensible cluster:
3 brokers
```

You cannot have:

```text
2 brokers
RF = 3
```

because you cannot place three distinct replicas on only two brokers.

---

# 9. But 3 Brokers Doesn't Mean "Always Enough"

This is a common interview mistake.

Someone might say:

> "Kafka needs 3 brokers."

Not necessarily.

Three is a common **starting point for HA**, but actual sizing depends on workload.

Example:

### Small workload

```text
100 MB/sec
1 TB storage
```

3 brokers may be sufficient.

### Large workload

```text
5 GB/sec
100 TB storage
```

You may need:

```text
10+
20+
30+
```

brokers depending on hardware and workload characteristics.

---

# 10. Factor 4 — Partitions

Partitions are distributed across brokers.

Example:

```text
Topic: orders
Partitions: 6
RF: 3

B1 → P0 P3
B2 → P1 P4
B3 → P2 P5
```

With replicas:

```text
P0 → B1 B2 B3
P1 → B2 B3 B1
...
```

More partitions mean more:

```text
metadata
network connections
file handles
replica management
recovery work
```

So don't create thousands of unnecessary partitions just because you have many brokers.

---

# 11. Example — Choosing Cluster Size

Suppose you have:

```text
Production application

Incoming data:
500 MB/sec

Retention:
7 days

Replication:
3

Peak traffic:
2 × average
```

Average:

```text
500 MB/sec
```

Peak:

```text
1 GB/sec
```

Suppose testing shows one broker should safely handle:

```text
300 MB/sec
```

Then peak capacity:

```text
1 GB/sec
────────
300 MB/sec

≈ 4 brokers
```

But:

```text
4 brokers
```

doesn't give you much room for a broker failure.

If one fails:

```text
3 brokers remaining
```

and:

```text
1 GB/sec / 3
≈ 333 MB/sec
```

which is above the tested 300 MB/sec target.

So you might choose:

```text
5 brokers
```

Now after one broker fails:

```text
4 brokers remain
```

and:

```text
1 GB/sec / 4
= 250 MB/sec
```

which gives you better headroom.

This is the correct way to reason about cluster size.

---

# 12. Factor 5 — Failure Tolerance

Ask:

> **How many brokers should we be able to lose while continuing normal operations?**

Suppose:

```text
Requirement:
Survive 1 broker failure
```

A common production design:

```text
3+ brokers
```

If:

```text
Requirement:
Survive 2 broker failures
```

you generally need more capacity and an appropriate replication/quorum design.

For example:

```text
5 brokers
```

allows you to maintain a majority of brokers after losing two, depending on the relevant Kafka quorum/controller configuration.

---

# 13. Failure Domain Matters

Don't do:

```text
AWS AZ-A

B1
B2
B3
B4
B5
```

If AZ-A disappears:

```text
B1 ❌
B2 ❌
B3 ❌
B4 ❌
B5 ❌
```

Instead:

```text
             AWS Region

       ┌───────┼───────┐
       │       │       │
      AZ-A    AZ-B    AZ-C
       │       │       │
      B1      B2      B3
      B4      B5
```

The exact placement depends on workload and rack/AZ-awareness configuration.

---

# 14. `broker.rack`

Kafka supports rack awareness.

Example:

```properties
broker.rack=az-a
```

Broker 2:

```properties
broker.rack=az-b
```

Broker 3:

```properties
broker.rack=az-c
```

Then Kafka can use rack information when placing replicas.

Architecture:

```text
       Topic Partition
             │
       ┌─────┼─────┐
       ▼     ▼     ▼
      AZ-A  AZ-B  AZ-C
       │     │     │
      B1    B2    B3
```

This reduces the risk of placing all replicas in the same failure domain.

---

# 15. Factor 6 — Network

Kafka is heavily network-oriented.

Traffic includes:

```text
Producer
   ↓
Broker

Broker
   ↓
Consumer

Leader
   ↔
Follower replicas
```

With replication factor 3, replication traffic can become substantial.

Example:

```text
Producer traffic
      ↓
100 MB/sec

Replication
      ↓
additional broker-to-broker traffic
```

Therefore:

> **Don't size Kafka only from producer throughput.**

Consider:

```text
Client traffic
+
Replication traffic
+
Consumer traffic
+
Recovery traffic
```

---

# 16. Factor 7 — Disk

Kafka is fundamentally a disk-backed streaming system.

Suppose:

```text
Disk throughput = 500 MB/sec
```

but your workload needs:

```text
1 GB/sec
```

That broker may become the bottleneck.

Also consider:

```text
IOPS
throughput
latency
disk capacity
disk utilization
```

For AWS, this means selecting appropriate EBS characteristics rather than simply saying:

> "Give every broker 1 TB."

---

# 17. Factor 8 — CPU

CPU becomes important for:

```text
compression
TLS
message processing
network processing
replica management
request handling
```

For example:

```text
Heavy compression
       ↓
CPU ↑
```

or:

```text
TLS encryption
       ↓
CPU ↑
```

So CPU requirements depend on workload and configuration.

---

# 18. Factor 9 — Memory

Kafka uses memory for:

```text
Page cache
JVM heap
Buffers
Network processing
```

One important Kafka concept:

> **Don't allocate all available RAM to the JVM heap.**

The operating system page cache is very important for Kafka's performance.

Conceptually:

```text
EC2 RAM
 │
 ├── JVM Heap
 │
 └── OS Page Cache
          │
          └── Kafka log files
```

So:

```text
More RAM
≠
Give all RAM to Kafka heap
```

---

# 19. Cluster Size Is Also About Headroom

Suppose you calculate:

```text
Required = 3 brokers
```

Don't automatically deploy exactly 3 if the workload is near capacity.

Think:

```text
Required capacity
       +
Failure capacity
       +
Growth capacity
```

Example:

```text
Current workload → 2 brokers worth
Failure tolerance → +1
Growth → +1

Cluster = 4 brokers
```

---

# 20. Real Platform Engineer Sizing Approach

When someone asks:

> **"How many Kafka brokers should we deploy?"**

Don't answer:

> "3."

Ask:

### Workload

```text
What is ingress MB/sec?
What is egress MB/sec?
Peak traffic?
Average traffic?
```

### Storage

```text
How much data/day?
Retention?
Replication factor?
Compression?
```

### Reliability

```text
How many broker failures must we survive?
AZ failure?
```

### Performance

```text
Disk throughput?
Disk latency?
Network bandwidth?
CPU?
Memory?
```

### Growth

```text
Expected traffic growth?
6 months?
1 year?
```

---

# 21. A Simple Sizing Formula

A useful **starting model** is:

```text
Required brokers
=
max(
  throughput requirement,
  storage requirement,
  failure requirement
)
```

Then add:

```text
headroom
+
growth
```

For throughput:

```text
Brokers required
=
Peak cluster throughput
/
Safe throughput per broker
```

Then round **up**.

Example:

```text
Peak = 1,200 MB/sec

Safe broker capacity = 300 MB/sec

1200 / 300
= 4 brokers
```

Then add failure tolerance:

```text
4 + 1
= 5 brokers
```

Then validate storage.

---

# 22. Example: Complete Production Scenario

Imagine an e-commerce platform.

```text
Orders
Payments
Inventory
Shipping
```

Kafka workload:

```text
Average ingress = 300 MB/sec
Peak ingress    = 900 MB/sec

Retention = 7 days

RF = 3

Requirement:
survive 1 broker failure
```

### Step 1 — Throughput

Suppose benchmark says:

```text
Safe broker throughput = 300 MB/sec
```

Then:

```text
900 / 300
= 3 brokers
```

But that's **without failure**.

### Step 2 — Failure

We want one broker failure:

```text
3 + 1
= 4 brokers
```

But after losing one:

```text
3 brokers remain
```

Each would need to handle approximately:

```text
900 / 3
= 300 MB/sec
```

That's already the safe limit.

So 4 may technically work but leaves little operational margin.

### Step 3 — Headroom

Choose:

```text
5 brokers
```

After one failure:

```text
4 brokers remain
```

Load:

```text
900 / 4
= 225 MB/sec
```

Now we have better headroom.

---

# 23. Architecture for That Example

```text
                     Kafka Cluster

          ┌──────────┼──────────┼──────────┐
          │          │          │          │
         B1         B2         B3         B4         B5
          │          │          │          │          │
       AZ-A       AZ-B       AZ-C       AZ-A       AZ-B
          │          │          │          │          │
          └──────────┴──────────┴──────────┴──────────┘
                             │
                        Topic: orders
                             │
                   ┌─────────┼─────────┐
                   │         │         │
                  P0        P1        P2 ...
```

With:

```text
RF = 3
```

Kafka distributes replicas across brokers/failure domains according to its replica placement rules and rack-awareness configuration.

---

# 24. What Happens When a Broker Dies?

Suppose:

```text
B2 ❌
```

Before:

```text
P0
Leader → B1
Replica → B2
Replica → B3
```

After B2 failure:

```text
P0
Leader → B1
Replica → B3
Missing replica → B2 ❌
```

Kafka can create a new replica on another eligible broker.

Example:

```text
B4
 ↓
new replica
```

Now:

```text
P0

B1 → Leader
B3 → Replica
B4 → Replica
```

This is why having spare broker capacity matters.

---

# 25. Under-Replication After Failure

Immediately after a broker fails:

```text
RF = 3

Available replicas = 2
```

Kafka reports the partition as:

```text
Under Replicated
```

Conceptually:

```text
Expected:
3 replicas

Current:
2 replicas

Missing:
1 replica
```

Once Kafka successfully creates the replacement replica:

```text
3 replicas
```

and the partition becomes fully replicated again.

---

# 26. Why You Don't Want a Cluster Running at 90–100%

Suppose:

```text
5 brokers
```

and all are already near:

```text
90% CPU
90% disk
90% network
```

Then:

```text
Broker failure
      ↓
Load moves to remaining brokers
      ↓
Capacity exhausted
      ↓
Performance degradation
```

A healthy cluster needs **operational headroom**.

The exact target depends on workload and benchmarking; don't treat a single utilization percentage as a universal Kafka rule.

---

# 27. Cluster Size vs Partition Count

Don't confuse:

```text
Cluster size
```

with:

```text
Partition count
```

Example:

```text
5 Brokers

Topic A → 12 partitions
Topic B → 24 partitions
Topic C → 6 partitions
```

Total:

```text
42 partitions
```

The cluster has:

```text
5 brokers
```

The number of partitions is:

```text
42
```

These are different sizing dimensions.

---

# 28. Very Important: More Brokers ≠ Automatically More Performance

Suppose:

```text
3 brokers → 5 brokers
```

You may get more:

```text
CPU
Disk
Network
partition placement capacity
```

But simply adding brokers does not magically make an existing topic's workload scale infinitely.

Your workload also depends on:

```text
Partition count
Producer parallelism
Consumer parallelism
Partition distribution
Leader distribution
```

Example:

```text
Topic
  │
  └── only 1 partition
```

Adding:

```text
10 brokers
```

doesn't make that single partition process data through all 10 brokers simultaneously.

This is why **cluster size and partition design must be considered together**.

---

# 29. Senior Platform Engineer Mental Model

When sizing:

```text
                 Kafka Cluster Size
                         │
       ┌─────────────────┼──────────────────┐
       │                 │                  │
   Throughput         Storage            Availability
       │                 │                  │
   MB/sec             GB/day             failures
   Peak               retention           AZ
       │                 │                  │
       └─────────────────┼──────────────────┘
                         │
                      Brokers
                         │
                + headroom/growth
```

---

# 30. Interview Answer

If asked:

> **"How do you decide Kafka cluster size?"**

A strong answer:

> "I don't size Kafka purely by broker count. I first calculate peak ingress and egress throughput, retention and replicated storage requirements, partition count, network and disk capacity, and the required failure tolerance. I benchmark the safe throughput and storage capacity of the selected broker instance type, then determine the minimum broker count. After that I add capacity for broker/AZ failures and future growth, and use rack/AZ awareness so replicas are distributed across failure domains."

Then give an example:

> "For example, if peak traffic is 900 MB/s and a benchmarked broker safely handles 300 MB/s, the throughput requirement is 3 brokers. If I need to survive one broker failure, I need additional capacity. If losing one broker would push the remaining brokers to their limit, I'd choose 5 rather than 4 to maintain operational headroom."

That is a much better Senior Platform Engineer answer than:

> **"Kafka production needs 3 brokers."**
