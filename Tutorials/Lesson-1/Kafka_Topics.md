# 1. Kafka Topics

## 1. What is a Kafka Topic?

* A **Kafka Topic** is a logical name/category where Kafka stores and organizes messages (records).
* Producers **write records to a topic**.
* Consumers **read records from a topic**.
* A topic itself does **not store data in a single place**.
* A topic is divided into **partitions**.
* Partitions are distributed across Kafka brokers.

### Simple definition

> **Topic = A logical stream/category of messages in Apache Kafka.**

For example:

```text
Topic: payment-events

Messages:
  Payment 101 successful
  Payment 102 failed
  Payment 103 successful
  Payment 104 pending
```

Another topic could be:

```text
Topic: order-events

Messages:
  Order 1001 created
  Order 1002 shipped
  Order 1003 cancelled
```

---

# 2. Easy Analogy

Think of Kafka as a **large post office**.

### Kafka Broker

* Think of each **broker as a post-office building**.

### Topic

* A **topic is like a specific department/mailbox category**.

For example:

```text
Post Office
│
├── Payments Department
├── Orders Department
├── Notifications Department
└── User Department
```

In Kafka:

```text
Kafka Cluster
│
├── payment-events
├── order-events
├── notification-events
└── user-events
```

But there is an important difference:

### Topic ≠ physical storage location

A topic is a **logical concept**.

Its actual data is stored inside its **partitions**, and those partitions are distributed across brokers.

---

# 3. Topic Architecture

A typical Kafka setup looks like this:

```text
                         Kafka Cluster
              ┌─────────────────────────────┐
              │                             │
Producer ────►│       Topic: orders         │
              │                             │
              │   Partition 0              │
              │   ────────────             │
              │   P0 ───────────────►      │
              │                             │
              │   Partition 1              │
              │   ────────────             │
              │   P1 ───────────────►      │
              │                             │
              │   Partition 2              │
              │   ────────────             │
              │   P2 ───────────────►      │
              │                             │
              └─────────────────────────────┘
                              │
                              ▼
                         Consumer
```

More realistically, partitions are spread across brokers:

```text
                    Topic: orders
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
          Partition   Partition   Partition
             0           1           2
             │           │           │
             ▼           ▼           ▼
          Broker 1    Broker 2    Broker 3
```

If replication is configured:

```text
                    Topic: orders

              Partition 0
             /            \
            ▼              ▼
       Broker 1         Broker 2
        Leader           Follower


              Partition 1
             /            \
            ▼              ▼
       Broker 2         Broker 3
        Leader           Follower


              Partition 2
             /            \
            ▼              ▼
       Broker 3         Broker 1
        Leader           Follower
```

So as a Platform Engineer, remember:

```text
Topic
  ↓
Partitions
  ↓
Partition replicas
  ↓
Brokers
  ↓
Physical storage
```

---

# 4. Why Do We Need Topics?

Without topics, all Kafka applications would write into one common stream.

Imagine:

```text
Payment
Order
Login
Notification
Shipment
Payment
Order
```

It would become very difficult for applications to consume only the data they need.

Topics provide logical separation:

```text
payment-events
order-events
login-events
notification-events
shipment-events
```

Now applications can subscribe to only the relevant topic.

---

# 5. Real-World Example

Imagine an e-commerce platform.

Different events are generated:

```text
Order Created
Payment Successful
Payment Failed
Order Shipped
Order Delivered
Email Required
SMS Required
```

You might create:

```text
orders
payments
shipping
notifications
```

Architecture:

```text
                    E-Commerce Application
                           │
              ┌────────────┼─────────────┐
              ▼            ▼             ▼
           orders       payments      shipping
              │            │             │
              ▼            ▼             ▼
          Consumers     Consumers     Consumers
              │            │             │
              ▼            ▼             ▼
           Order DB     Payment DB    Shipping DB
```

This gives you **logical separation of workloads**.

---

# 6. Topic → Partition Relationship

This is one of the **most important Kafka concepts**.

A topic can have multiple partitions.

Example:

```text
Topic: payments

Partition 0
Partition 1
Partition 2
Partition 3
```

So:

```text
payments
   │
   ├── P0
   ├── P1
   ├── P2
   └── P3
```

### Why partitions?

Primarily for:

* **Parallelism**
* **Scalability**
* **Throughput**
* **Distribution across brokers**

Instead of having one server handle everything:

```text
             payments
                 │
                 ▼
              Broker 1
                 │
            Everything
```

we can distribute:

```text
             payments
                 │
       ┌─────────┼─────────┐
       ▼         ▼         ▼
      P0        P1        P2
       │         │         │
       ▼         ▼         ▼
    Broker1   Broker2   Broker3
```

This allows Kafka to scale horizontally.

---

# 7. Are Messages Stored Directly in a Topic?

Technically, **no**.

Think of it like:

```text
Topic
  │
  └── Partitions
        │
        └── Log
              │
              ├── Record
              ├── Record
              ├── Record
              └── Record
```

Kafka stores records in **partition logs**.

For example:

```text
Topic: payments

Partition 0:

Offset
  0    Payment A
  1    Payment B
  2    Payment C
  3    Payment D
```

The **offset belongs to the partition**.

Therefore:

```text
P0 → offset 0,1,2,3...

P1 → offset 0,1,2,3...

P2 → offset 0,1,2,3...
```

There is **no single global offset across the entire topic**.

This is an important interview point.

---

# 8. Topic and Offset

Suppose:

```text
payments
│
├── P0
│    ├── 0 → Payment A
│    ├── 1 → Payment B
│    └── 2 → Payment C
│
├── P1
│    ├── 0 → Payment D
│    ├── 1 → Payment E
│    └── 2 → Payment F
│
└── P2
     ├── 0 → Payment G
     ├── 1 → Payment H
     └── 2 → Payment I
```

Notice:

```text
P0 → offset 0
P1 → offset 0
P2 → offset 0
```

That's completely normal.

Offsets are **partition-specific**.

---

# 9. Creating a Topic

Using Kafka CLI:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic payments \
  --partitions 3 \
  --replication-factor 3
```

This means:

```text
Topic              = payments
Partitions         = 3
Replication Factor = 3
```

Architecture:

```text
payments
│
├── P0 → 3 replicas
├── P1 → 3 replicas
└── P2 → 3 replicas
```

---

# 10. Listing Topics

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Example:

```text
__consumer_offsets
orders
payments
notifications
```

### Senior Platform Engineer point

`__consumer_offsets` is an important Kafka internal topic.

It stores **consumer group offsets**.

You'll encounter this topic frequently when troubleshooting consumer groups.

---

# 11. Describe a Topic

One of the **most important commands for Kafka administration**:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic payments
```

You may see something like:

```text
Topic: payments
PartitionCount: 3
ReplicationFactor: 3

Partition: 0
Leader: 1
Replicas: 1,2,3
Isr: 1,2,3

Partition: 1
Leader: 2
Replicas: 2,3,1
Isr: 2,3,1

Partition: 2
Leader: 3
Replicas: 3,1,2
Isr: 3,1,2
```

This tells you critical information.

### Partition

```text
Partition: 0
```

### Leader

```text
Leader: 1
```

Broker 1 is currently handling requests for that partition.

### Replicas

```text
Replicas: 1,2,3
```

Copies of the partition exist on brokers 1, 2 and 3.

### ISR

```text
ISR: 1,2,3
```

All replicas are currently in sync.

---

# 12. Topic Replication

Suppose:

```text
Replication Factor = 3
```

For:

```text
Partition 0
```

you could have:

```text
Broker 1 → Leader
Broker 2 → Follower
Broker 3 → Follower
```

If Broker 1 fails:

```text
Broker 1 ❌

Broker 2 → becomes Leader
Broker 3 → Follower
```

This is why replication provides **fault tolerance**.

### Analogy

Think of a company's important document.

Instead of keeping one copy:

```text
Document
   ↓
One person
```

you keep:

```text
Document
   ├── Person A
   ├── Person B
   └── Person C
```

If Person A is unavailable, another copy is available.

---

# 13. Topic Naming

Good topic naming is very important in large environments.

Examples:

```text
prod.orders.created
prod.orders.payment
prod.orders.shipped
prod.notifications.email
```

or:

```text
payments.v1
orders.v1
user-events.v1
```

As a Platform Engineer, you may establish organizational standards around:

* Environment
* Application/domain
* Event type
* Version
* Region, if necessary

Example:

```text
prod.payment.transaction.v1
```

But avoid blindly creating extremely complicated naming schemes.

---

# 14. Topic Configuration

Topics have configurations that affect their behavior.

Important ones include:

### `retention.ms`

How long records are retained.

Example:

```text
retention.ms=604800000
```

≈ 7 days.

---

### `retention.bytes`

Maximum retained log size per partition before old records are removed, subject to Kafka's retention behavior.

---

### `cleanup.policy`

Very important.

Common values:

```text
delete
compact
```

or:

```text
delete,compact
```

### `delete`

Old records are eventually deleted based on retention settings.

Good for:

```text
application-events
logs
metrics/events
```

### `compact`

Kafka retains the latest record for each key over time.

Useful for state-like topics such as:

```text
user-profile
account-state
configuration
```

We'll go deeply into **log compaction** when you reach that topic.

---

# 15. Can We Increase Partitions?

Yes.

Example:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic payments \
  --partitions 6
```

You can increase:

```text
3 → 6
```

But you **cannot normally decrease partitions** through the standard Kafka topic alteration mechanism.

### Very important Platform Engineer consideration

Increasing partitions can affect **message ordering**.

Suppose records are partitioned based on key:

```text
customer-101 → P0
customer-102 → P1
```

If the partition count changes, the partition assignment for keys can change depending on the partitioning scheme.

Therefore:

> **Do not casually increase partition count in production without understanding the application's ordering and partitioning requirements.**

---

# 16. Ordering and Topics

Kafka guarantees ordering **within a partition**.

Example:

```text
P0:

1 → Order Created
2 → Payment Successful
3 → Order Shipped
```

Kafka preserves:

```text
1 → 2 → 3
```

within that partition.

But across partitions:

```text
P0 → A → B → C

P1 → X → Y → Z
```

Kafka does **not** provide a single global ordering:

```text
A X B Y C Z
```

There is no guaranteed global order.

### Interview statement

> "Kafka guarantees ordering within a partition, not across the entire topic."

---

# 17. Topic + Producer + Consumer

Complete flow:

```text
                   Producer
                      │
                      │
                      ▼
                ┌───────────┐
                │   Topic   │
                │  payments │
                └─────┬─────┘
                      │
              ┌───────┼───────┐
              ▼       ▼       ▼
             P0      P1      P2
              │       │       │
              └───────┼───────┘
                      │
                      ▼
                   Consumer
```

With multiple brokers:

```text
                    Producer
                       │
                       ▼
                 Topic: payments
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
      P0              P1              P2
        │              │              │
        ▼              ▼              ▼
    Broker-1       Broker-2       Broker-3
        │              │              │
        └──────────────┼──────────────┘
                       ▼
                  Consumer Group
```

---

# 18. Topics from Senior Platform Engineer Perspective

This is where you should focus most heavily.

When someone asks you to create a production topic, don't think only:

> "Create a topic with 3 partitions."

Think:

```text
                    Production Topic
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
   Partition Count   Replication Factor   Retention
        │                  │                  │
        ▼                  ▼                  ▼
   Throughput          HA / Failure       Storage cost
        │                  │                  │
        └──────────────────┼──────────────────┘
                           ▼
                    Cleanup Policy
                           │
                           ▼
                    Ordering needs
                           │
                           ▼
                    Partition Key
                           │
                           ▼
                    Consumer Load
```

You need to consider:

* **Number of partitions**
* **Replication factor**
* **Broker capacity**
* **Expected throughput**
* **Message size**
* **Retention period**
* **Storage requirements**
* **Cleanup policy**
* **Ordering requirements**
* **Partition key**
* **Consumer parallelism**
* **Availability requirements**
* **Security/ACLs**
* **Monitoring**
* **Disaster recovery**
* **Cross-region requirements**

---

# 19. Production Scenario

Imagine your application team says:

> "We need a new `payments` Kafka topic."

As a Senior Platform Engineer, ask:

### Capacity

* How many messages/sec?
* Average message size?
* Peak messages/sec?

### Retention

* Do you need 1 day?
* 7 days?
* 30 days?

### Ordering

* Does payment ordering matter?
* Per customer?
* Per transaction?

### Availability

* What replication factor is required?
* What happens if a broker fails?

### Consumers

* How many consumers?
* How much parallel processing is required?

### Storage

If:

```text
100 MB/sec
```

and retention is:

```text
7 days
```

roughly:

```text
100 MB × 60 × 60 × 24 × 7
```

≈ **60.5 TB of raw data**

before accounting for replication and other storage overheads.

With:

```text
Replication Factor = 3
```

the storage requirement becomes roughly:

```text
60.5 TB × 3
≈ 181.5 TB
```

So a seemingly simple request:

> "Create a Kafka topic."

can actually become a **capacity-planning decision**.

---

# 20. Important Kafka Topic Facts to Remember

| Concept              | Remember                            |
| -------------------- | ----------------------------------- |
| Topic                | Logical stream/category             |
| Partition            | Unit of parallelism and ordering    |
| Broker               | Stores partition replicas           |
| Record               | Individual message/event            |
| Offset               | Position within a partition         |
| Leader               | Replica handling partition requests |
| Follower             | Replica copying leader data         |
| ISR                  | In-sync replicas                    |
| Replication Factor   | Number of replicas                  |
| Retention            | How long/data much Kafka keeps      |
| Cleanup Policy       | Delete/compact behavior             |
| Ordering             | Guaranteed within partition         |
| Partition count      | Important for scalability           |
| Consumer parallelism | Tied strongly to partitions         |

---

# 21. The Mental Model You Should Remember

Don't memorize Kafka as:

```text
Producer → Topic → Consumer
```

For Senior Platform Engineer interviews, build this mental model:

```text
                         KAFKA CLUSTER
                              │
                         ┌────┴────┐
                         │  TOPIC  │
                         │ payments│
                         └────┬────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
             P0              P1              P2
              │               │               │
         ┌────┴────┐     ┌────┴────┐     ┌────┴────┐
         │         │     │         │     │         │
       Leader   Follower Leader Follower Leader Follower
         │         │     │         │     │         │
         └─────────┘     └─────────┘     └─────────┘
              │               │               │
              ▼               ▼               ▼
           Broker 1        Broker 2        Broker 3
                              │
                              ▼
                       Consumer Groups
```

### The key chain:

> **Topic → Partitions → Replicas → Brokers → Storage**

And from the application side:

> **Producer → Partition → Broker → Consumer Group → Consumer**

That mental model will make the next Kafka topics—**partitions, replication, ISR, leaders, consumer groups, offsets, retention, log segments, rebalancing, etc.**—much easier to understand.
