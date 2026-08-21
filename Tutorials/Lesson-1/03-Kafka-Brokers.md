# 3. Kafka Brokers

For a **Senior Platform Engineer**, brokers are one of the most important Kafka concepts because brokers are the actual Kafka servers you operate, scale, monitor, secure, and troubleshoot.

---

# 1. What is a Kafka Broker?

* A **Kafka broker is a Kafka server**.
* It is responsible for:

  * Receiving messages from producers.
  * Storing messages on disk.
  * Serving messages to consumers.
  * Managing partition replicas.
  * Handling replication.
  * Participating in leader elections.
  * Communicating with other brokers.
* A Kafka cluster consists of **multiple brokers**.

### Simple definition

> **Broker = A Kafka server that stores and serves partition data and participates in the Kafka cluster.**

---

# 2. Easy Analogy 🏢

Imagine a large **warehouse company**.

You have:

```text
Warehouse Company
│
├── Warehouse 1
├── Warehouse 2
└── Warehouse 3
```

Each warehouse stores some products.

In Kafka:

```text
Kafka Cluster
│
├── Broker 1
├── Broker 2
└── Broker 3
```

Each broker stores some **partition replicas**.

So:

> **Kafka Cluster = Company**
> **Broker = Warehouse**
> **Topic = Product category**
> **Partition = Individual storage section**

---

# 3. Basic Kafka Architecture

```text
                         Kafka Cluster
                ┌────────────────────────────┐
                │                            │
Producer ──────►│  Broker 1                  │
                │   ├── Topic A - P0         │
                │   └── Topic B - P1         │
                │                            │
                │  Broker 2                  │
                │   ├── Topic A - P1         │
                │   └── Topic B - P0         │
                │                            │
                │  Broker 3                  │
                │   ├── Topic A - P2         │
                │   └── Topic B - P2         │
                │                            │
                └────────────┬───────────────┘
                             │
                             ▼
                          Consumer
```

The important thing is:

**A topic is NOT a broker.**

A topic's partitions can be distributed across multiple brokers.

---

# 4. Broker vs Topic vs Partition

This distinction is extremely important.

Suppose:

```text
Topic: payments
Partitions: 3
Brokers: 3
Replication Factor: 3
```

You could have:

```text
                 payments
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼
      P0           P1           P2
       │            │            │
       ▼            ▼            ▼
    Broker 1     Broker 2     Broker 3
```

But because replication factor = 3:

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

Therefore:

> **A broker can contain partitions from many different topics.**

---

# 5. Why Do We Need Multiple Brokers?

There are three major reasons:

### 1. Scalability

Instead of one machine handling everything:

```text
              Kafka
                │
                ▼
             Broker 1
             100% load
```

we distribute workload:

```text
              Kafka Cluster
          ┌──────┼──────┐
          ▼      ▼      ▼
       Broker1 Broker2 Broker3
         33%     33%     34%
```

---

### 2. High Availability

If there is only one broker:

```text
Broker 1 ❌
   ↓
Kafka unavailable
```

With multiple brokers and replication:

```text
Broker 1 ❌

Broker 2 ✓
Broker 3 ✓

Data still available
```

---

### 3. Fault Tolerance

Partitions can have replicas on different brokers.

```text
P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

If Broker 1 fails:

```text
Broker 1 ❌

Broker 2 → New Leader
Broker 3 → Follower
```

---

# 6. What Does a Broker Actually Do?

Think about a producer sending:

```text
Order #1001
```

The flow is roughly:

```text
Producer
   │
   │ Send record
   ▼
Kafka Broker
   │
   ▼
Partition Leader
   │
   ▼
Write to log
   │
   ▼
Replicate to followers
```

Later:

```text
Consumer
   │
   ▼
Partition Leader
   │
   ▼
Read record
```

So a broker sits in the middle of Kafka's data path.

---

# 7. Broker and Partition Leader

This is a **very important concept**.

A partition has a leader.

Example:

```text
Topic: orders

P0
Leader → Broker 1

P1
Leader → Broker 2

P2
Leader → Broker 3
```

Producers and consumers generally interact with the **leader replica** for a partition.

So:

```text
Producer
   │
   ▼
Broker 1
   │
   ▼
P0 Leader
```

and:

```text
Consumer
   │
   ▼
Broker 1
   │
   ▼
P0 Leader
```

Followers replicate the leader's data.

---

# 8. Broker ID

Each broker in a Kafka cluster needs an identity.

Example:

```text
Broker 1
Broker 2
Broker 3
```

Historically this was commonly represented using:

```properties
broker.id=1
```

Modern Kafka deployments can also use other node identity configurations depending on the cluster mode.

For your learning, remember:

> **Every broker needs a unique identity within the Kafka cluster.**

---

# 9. Broker Storage

This is particularly important from your **Platform Engineer perspective**.

Kafka brokers store partition data on disk.

Example:

```text
Broker 1
│
├── /kafka-data/
│    ├── payments-0/
│    ├── orders-1/
│    ├── users-0/
│    └── notifications-2/
```

Inside those directories are Kafka's **log segments**.

Conceptually:

```text
Partition P0
│
├── Segment 1
├── Segment 2
├── Segment 3
└── Segment 4
```

Kafka doesn't keep the entire topic in RAM.

It relies heavily on:

* Disk
* OS page cache
* Sequential I/O
* Network

---

# 10. Broker Disk Is Extremely Important

As a Platform Engineer, monitor:

```text
Broker
│
├── Disk utilization
├── Disk IOPS
├── Disk throughput
├── Network throughput
├── CPU
├── JVM memory
└── Request latency
```

One of the most dangerous situations is:

```text
Broker disk
████████████████████ 95%
```

Because Kafka is fundamentally a **storage-heavy distributed system**.

---

# 11. Broker Communication

Kafka brokers communicate with each other.

For example:

```text
Broker 1
   │
   ├────────► Broker 2
   │
   └────────► Broker 3
```

This communication is required for things such as:

* Replica synchronization
* Metadata
* Leader elections
* Controller operations
* Cluster coordination
* Partition management

---

# 12. Controller

In a Kafka cluster, one broker has the **controller role**.

Conceptually:

```text
Kafka Cluster

Broker 1 → Controller
Broker 2
Broker 3
```

The controller coordinates cluster-level operations such as:

* Partition leadership changes
* Leader elections
* Broker membership changes
* Some administrative state changes

### Important modern Kafka point

Kafka has evolved from the older **ZooKeeper-based architecture** to **KRaft**.

In modern Kafka deployments:

```text
Kafka
  │
  └── KRaft
       ├── Controller role
       └── Broker role
```

KRaft removes the dependency on ZooKeeper.

For modern Kafka administration, you should definitely understand **KRaft**, because it is now the direction for current Kafka deployments.

---

# 13. Broker Failure Scenario

<img width="1844" height="1013" alt="image" src="https://github.com/user-attachments/assets/56b2d858-b0ca-45ac-97a6-5340faee7c77" />


Suppose:

```text
Kafka Cluster

Broker 1
Broker 2
Broker 3
```

And:

```text
Topic: payments
Replication Factor = 3
```

For P0:

```text
P0

Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

Now:

```text
Broker 1 💥
```

Kafka detects that the broker is unavailable.

If an eligible in-sync replica exists:

```text
Broker 2 → New Leader
Broker 3 → Follower
```

Traffic can continue.

This is one of the fundamental reasons Kafka uses **replication**.

---

# 14. What Happens If There Is No Replica?

Suppose:

```text
Replication Factor = 1
```

Then:

```text
P0
 │
 └── Broker 1
```

Broker 1 dies:

```text
Broker 1 ❌

P0 ❌
```

The partition becomes unavailable.

This is why production Kafka topics generally need an appropriate replication factor.

---

# 15. Broker vs Replication Factor

Don't confuse these.

### Brokers

Physical/logical Kafka servers:

```text
Broker 1
Broker 2
Broker 3
```

### Replication factor

Number of copies of each partition:

```text
RF = 3

P0:
 ├── Broker 1
 ├── Broker 2
 └── Broker 3
```

You could have:

```text
10 brokers
Replication Factor = 3
```

You don't need every partition to have a replica on every broker.

---

# 16. Broker Load Balancing

Suppose:

```text
Broker 1 → 90% CPU
Broker 2 → 30%
Broker 3 → 20%
```

This is unhealthy.

Why?

Possibilities include:

* Uneven partition distribution
* Uneven leader distribution
* Hot partition
* Uneven traffic
* Large partitions
* Producer/consumer imbalance

As Platform Engineer, you'd investigate:

```text
Broker utilization
      ↓
Partition distribution
      ↓
Leader distribution
      ↓
Traffic per partition
      ↓
Hot partitions
```

---

# 17. Hot Broker vs Hot Partition

These are different.

### Hot broker

```text
Broker 1 → 90%
Broker 2 → 40%
Broker 3 → 35%
```

Could happen because Broker 1 owns many busy partition leaders.

### Hot partition

```text
P0 → 100 MB/s
P1 → 10 MB/s
P2 → 8 MB/s
```

Even if brokers look reasonably balanced, P0 itself can become a bottleneck.

This often relates to **partition-key design**.

---

# 18. Important Broker Configuration Categories

As a Senior Platform Engineer, you'll encounter configurations around:

### Networking

* `listeners`
* `advertised.listeners`
* network threads
* socket/request settings

### Storage

* `log.dirs`
* retention-related settings
* log segment settings

### Replication

* replica fetcher settings
* replication-related configurations
* ISR behavior

### Performance

* network threads
* I/O threads
* request queues
* socket buffers

### Availability

* replication factor
* minimum in-sync replicas
* leader election behavior

Don't try to memorize every broker configuration.

Understand **what category of problem each configuration solves**.

---

# 19. `listeners` vs `advertised.listeners`

This is a very important real-world Platform Engineer topic.

Suppose Kafka broker is running at:

```text
10.0.1.10:9092
```

Kafka needs to tell clients:

> "This is how you should connect to me."

That's where **advertised listeners** become important.

Conceptually:

```text
Client
  │
  ▼
Bootstrap Server
  │
  ▼
Kafka metadata
  │
  ▼
"Connect to Broker 2 at this address"
  │
  ▼
Broker 2
```

If `advertised.listeners` is incorrectly configured, clients may:

* Connect to bootstrap broker successfully
* Receive metadata
* Then fail to connect to the actual broker

This is a **very common Kafka networking issue**.

Especially important with:

* Kubernetes
* EKS
* Docker
* NAT
* Load balancers
* Private/public networks
* Amazon MSK

We'll cover this in depth when we reach Kafka networking.

---

# 20. Broker Monitoring

For production Kafka, monitor at least:

### Infrastructure

```text
CPU
Memory
Disk
Network
JVM
```

### Kafka

```text
UnderReplicatedPartitions
OfflinePartitionsCount
ActiveControllerCount
LeaderCount
PartitionCount
RequestLatency
BytesIn
BytesOut
```

### Consumer side

```text
Consumer Lag
```

### Particularly critical 🚨

```text
Offline partitions > 0
```

means some partitions have no available leader.

And:

```text
Under-replicated partitions > 0
```

means replicas are not fully caught up with their leaders.

These are major production alerts.

---

# 21. Senior Platform Engineer Scenario

### Situation

You receive an alert:

> `UnderReplicatedPartitions = 20`

You shouldn't immediately restart Kafka.

Think:

```text
Under-replicated partitions
          ↓
Which broker?
          ↓
Is broker healthy?
          ↓
CPU?
Memory?
Disk?
Network?
          ↓
Is disk full?
          ↓
Is network saturated?
          ↓
Replica fetch lag?
          ↓
Broker overloaded?
          ↓
Any broker restart/failure?
```

Potential cause:

```text
Broker 3
   ↓
Disk I/O extremely high
   ↓
Follower can't fetch data fast enough
   ↓
Replica falls behind
   ↓
Removed from ISR
   ↓
UnderReplicatedPartitions increases
```

That's the kind of **operational thinking** expected from a Senior Platform Engineer.

---

# 22. Broker Scaling

Suppose:

```text
Kafka Cluster

Broker 1
Broker 2
Broker 3
```

Traffic grows significantly.

You may add:

```text
Broker 4
Broker 5
```

But an important point:

> **Adding brokers doesn't automatically mean existing partitions are perfectly redistributed.**

You may need partition reassignment/rebalancing.

For example:

```text
Before:

B1 → 40 partitions
B2 → 40 partitions
B3 → 40 partitions

Add B4

B1 → 40
B2 → 40
B3 → 40
B4 → 0
```

You need appropriate partition reassignment so that the new broker actually receives workload.

---

# 23. Kafka Broker vs Traditional Server

A Kafka broker isn't simply:

> "A VM running Kafka."

It participates in a **distributed system**.

It has responsibilities around:

```text
Storage
   +
Networking
   +
Replication
   +
Partition leadership
   +
Cluster coordination
   +
Fault tolerance
```

This is why Kafka administration requires understanding both:

* **Linux/infrastructure**
* **Distributed systems**

---

# 24. AWS MSK Perspective

In Amazon MSK:

```text
                Amazon MSK Cluster
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
       Broker 1     Broker 2     Broker 3
          │            │            │
          ▼            ▼            ▼
        EBS          EBS          EBS
```

AWS manages much of the underlying infrastructure and Kafka service lifecycle, but you still need to understand:

* Broker count
* Broker type/size
* Storage
* Networking
* Subnets
* Security groups
* Encryption
* Kafka configurations
* Partitions
* Replication
* Monitoring
* Scaling

So understanding brokers is directly relevant to **MSK administration**.

---

# 25. Interview Questions You Should Be Able to Answer

### Q1. What is a Kafka broker?

> A Kafka broker is a Kafka server responsible for storing partition replicas, handling producer and consumer requests, and participating in replication and cluster coordination.

### Q2. Why do we need multiple brokers?

> For horizontal scalability, fault tolerance, high availability, and distributing partition storage and traffic.

### Q3. Can one broker contain partitions from multiple topics?

**Yes.**

```text
Broker 1
├── orders-P0
├── payments-P2
├── users-P1
└── notifications-P0
```

### Q4. Can a broker be a leader for some partitions and follower for others?

**Absolutely.**

```text
Broker 1
├── orders-P0 → Leader
├── payments-P1 → Follower
└── users-P2 → Leader
```

This is normal.

### Q5. What happens if a broker fails?

* Its partitions become unavailable **unless replicas exist on other brokers**.
* Kafka can elect an eligible in-sync replica as the new leader.
* Clients refresh metadata and continue using the new leader.

### Q6. Does adding a broker automatically redistribute all existing data?

**No.**

Partition reassignment/rebalancing is required.

---

# 26. The Mental Model 🔥

Remember this hierarchy:

```text
                     KAFKA CLUSTER
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
       BROKER 1          BROKER 2         BROKER 3
          │                │                │
       ┌──┴──┐          ┌──┴──┐          ┌──┴──┐
       ▼     ▼          ▼     ▼          ▼     ▼
      P0     P1         P1     P2         P2     P0
       │                │                │
       └─────── Replicas / Leaders ──────┘
```

### Lock these relationships into your mind:

> **Cluster = collection of brokers**

> **Broker = Kafka server**

> **Topic = logical stream**

> **Partition = unit of parallelism/storage**

> **Replica = copy of a partition**

> **Leader = replica handling requests for that partition**

> **Follower = replica copying the leader**

> **ISR = replicas sufficiently caught up with the leader**

And from a **Senior Platform Engineer** perspective:

> **Broker health → partition health → replication health → storage/network health → consumer impact**

That chain is what you'll repeatedly use when troubleshooting Kafka in production.
