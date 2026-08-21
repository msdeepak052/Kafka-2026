# Common Doubts 

These four questions are **very important fundamentals**, and they connect directly to what you've already learned about **partitions, ordering, consumers, offsets and topics**.

I'll keep the explanation at your current level and avoid jumping into later Kafka concepts.

---

# 1. Why is ordering guaranteed in a partition but NOT across partitions?

## First: What does "ordering" mean?

Suppose a topic has **one partition**:

```text
orders-P0

Offset 0 → Order Created
Offset 1 → Payment Completed
Offset 2 → Order Shipped
Offset 3 → Order Delivered
```

Kafka maintains the order:

```text
0 → 1 → 2 → 3
```

So the consumer sees:

```text
Created
   ↓
Payment Completed
   ↓
Shipped
   ↓
Delivered
```

### Why?

Because a partition is an **ordered append-only log**.

New records are appended at increasing offsets.

---

# Now suppose we have 3 partitions

```text
orders

P0:
Offset 0 → A
Offset 1 → B
Offset 2 → C

P1:
Offset 0 → D
Offset 1 → E
Offset 2 → F

P2:
Offset 0 → G
Offset 1 → H
Offset 2 → I
```

Each partition has its **own independent ordering**.

```text
P0: A → B → C
P1: D → E → F
P2: G → H → I
```

But Kafka does NOT have one global sequence like:

```text
A → D → G → B → E → H → C → F → I
```

There is no single global offset across the topic.

---

## Easy analogy 🚗🚗🚗

Imagine a toll plaza with 3 lanes:

```text
             Toll Plaza

Lane 1       Lane 2       Lane 3
  ↓            ↓            ↓
Car A        Car D        Car G
Car B        Car E        Car H
Car C        Car F        Car I
```

Inside each lane:

```text
A → B → C
D → E → F
G → H → I
```

The order within each lane is clear.

But can you say:

> "Was Car B before or after Car E?"

Not from the lane information alone.

That's exactly the Kafka partition concept.

---

# Real Example — Bank Transactions 💳

Suppose a customer performs:

```text
Transaction 1 → Deposit ₹10,000
Transaction 2 → Withdraw ₹2,000
Transaction 3 → Withdraw ₹1,000
```

If all three events go to:

```text
P0
```

then:

```text
P0

Offset 100 → Deposit ₹10,000
Offset 101 → Withdraw ₹2,000
Offset 102 → Withdraw ₹1,000
```

The order is preserved.

But if events for different customers are distributed:

```text
P0:
Customer A → Transaction 1
Customer A → Transaction 2

P1:
Customer B → Transaction 1
Customer B → Transaction 2
```

Kafka guarantees ordering **inside P0 and inside P1**, not between P0 and P1.

---

# Why doesn't Kafka provide global ordering?

Because partitions exist partly to provide **scalability and parallelism**.

If Kafka had to maintain one global order across every partition, it would need to coordinate all partitions.

Instead:

```text
                    Topic
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
         P0          P1          P2
          │           │           │
       Ordered     Ordered     Ordered
       sequence    sequence    sequence
```

This allows them to operate independently.

### 🔥 Remember

> **Ordering is guaranteed within a partition, not across partitions.**

---

# 2. Can multiple consumers read from the same partition?

## Yes — but there is an important distinction.

It depends on whether the consumers belong to the **same consumer group or different consumer groups**.

---

## Case 1 — Same Consumer Group ❌

Suppose:

```text
Topic: orders

P0
P1
P2
```

And you have:

```text
Consumer Group: payment-group

C1
C2
C3
```

Kafka assigns partitions to consumers.

For example:

```text
P0 → C1
P1 → C2
P2 → C3
```

You do **not** normally have:

```text
P0 → C1
P0 → C2
```

at the same time within the same consumer group.

### Why?

Because a consumer group is intended to distribute partition processing among its consumers.

Think of it like dividing work:

```text
3 partitions
3 consumers

P0 → C1
P1 → C2
P2 → C3
```

---

## What if we have 4 consumers and only 3 partitions?

```text
P0
P1
P2
```

Consumers:

```text
C1
C2
C3
C4
```

Only three consumers can actively own a partition:

```text
P0 → C1
P1 → C2
P2 → C3

C4 → No partition
```

So:

> **Within a consumer group, the maximum useful consumer parallelism is limited by the number of partitions.**

---

# Case 2 — Different Consumer Groups ✅

Now suppose:

```text
payment-group
inventory-group
notification-group
```

Each group has a consumer.

All three groups can read the same partition:

```text
                  P0
             ┌────┼────┐
             ▼    ▼    ▼
          Payment Inventory Notification
           Group    Group      Group
```

So:

```text
P0
 │
 ├── payment-group
 │
 ├── inventory-group
 │
 └── notification-group
```

### Why?

Because each consumer group maintains its **own consumption progress**.

For example:

```text
P0:

Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
```

Payment:

```text
payment-group
     ↓
reads 0,1,2
```

Inventory:

```text
inventory-group
     ↓
reads 0,1,2
```

Notification:

```text
notification-group
     ↓
reads 0,1,2
```

The same events can therefore be consumed independently by different applications.

---

# Real Example 🛒

Order event:

```json
{
  "orderId": "ORD-1001",
  "status": "CREATED"
}
```

It enters:

```text
order-events / P0
```

Now:

```text
                    P0
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼
 payment-group inventory-group notification-group
       │            │            │
       ▼            ▼            ▼
   Process       Reserve      Send email
   payment       inventory
```

So **yes**, multiple consumers can effectively read the same partition **when they belong to different consumer groups**.

### 🔥 Remember

```text
Same Group:
One partition → one consumer at a time

Different Groups:
Same partition → each group can consume it
```

---

# 3. How Many Partitions Should We Choose When Creating a Topic?

This is a **very important Platform Engineer question**.

Suppose you need to create:

```text
order-events
```

How many partitions?

```text
1?
3?
10?
50?
100?
```

There is **no universal number**.

You choose based primarily on:

* Expected throughput
* Required consumer parallelism
* Number of consumers you may need
* Ordering requirements
* Future growth

---

# First Understand What Partitions Give You

Suppose:

```text
Topic
 │
 ├── P0
 ├── P1
 └── P2
```

You can process them in parallel.

```text
P0 ──► Consumer 1
P1 ──► Consumer 2
P2 ──► Consumer 3
```

So more partitions can provide more **parallelism**.

---

# Example

Suppose your application produces:

```text
10,000 events/second
```

And one consumer can comfortably process:

```text
2,000 events/second
```

Roughly:

```text
10,000 / 2,000 = 5
```

You'd need around **5 partitions** to support that level of consumer parallelism, assuming the workload is reasonably balanced.

You might choose:

```text
6 partitions
```

to leave some room for growth.

### But don't blindly calculate:

```text
10,000 / 2,000 = 5
→ ALWAYS choose 5
```

Real production sizing requires testing and considering the producer, consumer, broker capacity, record size, workload pattern, and growth.

---

# Another Important Factor — Consumer Count

Suppose:

```text
Partitions = 3
```

You have:

```text
C1
C2
C3
```

Great:

```text
P0 → C1
P1 → C2
P2 → C3
```

Now suppose you want:

```text
C1
C2
C3
C4
C5
C6
```

But still:

```text
P0
P1
P2
```

You cannot get six-way partition-level parallelism.

You only have three partitions.

---

# So Think Like This

```text
Partitions
     ↓
Available parallel work units
     ↓
Consumer parallelism
```

For example:

```text
6 partitions

       ↓

Up to 6 consumers
can actively process
different partitions
within one consumer group
```

---

# Another Important Factor — Ordering

Suppose all events for an order must remain ordered:

```text
ORD-1001 CREATED
ORD-1001 PAID
ORD-1001 SHIPPED
```

You can use:

```text
key = orderId
```

so these related events can go to the same partition.

Therefore, partition count isn't just:

> "More is always better."

You also need to think about how your data is distributed across partitions.

---

# Can We Increase Partitions Later?

Yes, Kafka allows increasing the partition count of a topic.

For example:

```text
Before:

P0
P1
P2
```

Later:

```text
P0
P1
P2
P3
P4
P5
```

But **partition count is an important design decision** because changing it can affect how keyed records are distributed across partitions.

So you shouldn't think:

> "I'll just start with 1 and increase it whenever I want."

Especially when partition keys and ordering matter.

### Platform Engineer mindset:

> **Choose enough partitions for expected throughput and parallelism, with reasonable headroom, rather than arbitrarily creating a huge number.**

---

# Real Example

Suppose your order system expects:

```text
Current:
5,000 events/sec

Expected after 1 year:
15,000 events/sec
```

And testing shows:

```text
1 partition ≈ 2,000 events/sec
```

You might initially consider:

```text
5,000 / 2,000 ≈ 3
```

But because you expect growth, you might choose something like:

```text
6 partitions
```

rather than exactly 3.

This is an **illustrative sizing example**, not a universal Kafka formula.

---

# 4. When Are Events Deleted From Kafka?

This is another very important concept.

The first thing to remember:

> **Kafka does NOT normally delete an event just because a consumer has read it.**

This is a common misconception.

---

# Example

Suppose:

```text
orders / P0

Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
```

Consumer reads:

```text
A
B
C
```

Does Kafka immediately delete them?

```text
❌ No
```

They can remain in Kafka according to the topic's retention policy.

---

# Easy Analogy 📚

Think of a library.

You borrow a book:

```text
Consumer → reads book
```

The library doesn't destroy the book immediately after you read it.

The book remains available according to the library's rules.

Similarly:

```text
Kafka
  │
  ├── Event A
  ├── Event B
  └── Event C

Consumer reads them
        ↓
Events can still remain
```

---

# How Does Kafka Decide When to Remove Data?

Kafka primarily uses **retention policies**.

Two important retention concepts at this stage are:

### 1. Time-based retention

Example:

```text
retention = 7 days
```

Meaning approximately:

> Kafka retains the records for the configured retention period, subject to Kafka's log-segment cleanup behavior.

After the data becomes eligible for deletion, Kafka's cleanup process removes the relevant old log segments.

Conceptually:

```text
Day 1
Event A

Day 2
Event B

...

Day 7+
Event A becomes eligible for cleanup
```

---

# 2. Size-based retention

You can also configure a maximum amount of log data to retain.

Example:

```text
retention size = 100 GB
```

When the retained log exceeds the configured limit, older data becomes eligible for cleanup.

Conceptually:

```text
Kafka Topic

100 GB
│
├── New data
├── New data
├── New data
└── Old data → cleanup
```

---

# Important: Kafka Doesn't Delete Based on Consumption

Suppose:

```text
Consumer A → reads Offset 100
Consumer B → reads Offset 100
```

Kafka doesn't say:

> "Both consumers have read offset 100, so delete it."

❌ That's not the basic Kafka retention model.

Retention is configured independently of whether a consumer has read the event.

---

# Real Example

Suppose:

```text
Topic:
order-events

Retention:
7 days
```

Monday:

```text
ORD-1001 → Offset 100
```

Tuesday:

```text
ORD-1002 → Offset 101
```

Wednesday:

```text
ORD-1003 → Offset 102
```

The consumer reads everything.

But Kafka can still retain those events.

As data ages beyond the configured retention period, old data becomes eligible for cleanup.

---

# What Does "Delete" Actually Mean?

Kafka stores records in **log segments**.

You shouldn't imagine:

```text
Offset 100 → DELETE
Offset 101 → DELETE
Offset 102 → DELETE
```

one by one immediately.

Instead, Kafka's log is organized into segments, and old segments can be removed when their records are eligible for retention cleanup.

Conceptually:

```text
Partition P0

Segment 1
├── Offset 0
├── Offset 1
└── Offset 2

Segment 2
├── Offset 3
├── Offset 4
└── Offset 5

Segment 3
├── Offset 6
├── Offset 7
└── Offset 8
```

Older segments can eventually be removed according to retention.

We can go deeper into **log segments and retention internals** when that topic comes later.

---

# Put All 4 Questions Together 🔥

These four questions actually connect beautifully:

```text
                 KAFKA TOPIC
                     │
             ┌───────┼───────┐
             ▼       ▼       ▼
            P0      P1      P2
             │       │       │
          Ordered  Ordered  Ordered
             │       │       │
             └───────┼───────┘
                     │
              No global order
```

### Multiple consumers

```text
P0
 │
 ├── payment-group
 ├── inventory-group
 └── notification-group
```

### Partitions determine parallelism

```text
3 partitions
     ↓
Up to 3 active partition owners
within one consumer group
```

### Data deletion

```text
Consumer reads event
       ↓
Event is NOT automatically deleted
       ↓
Retention policy
       ↓
Event becomes eligible for cleanup
```

---

# 🎯 Senior Platform Engineer Interview Answers

### Q1. Why is ordering guaranteed within a partition but not across partitions?

> "Kafka partitions are ordered append-only logs, so records within a partition have a well-defined offset sequence. Different partitions maintain independent sequences, so Kafka doesn't provide a global ordering across partitions. This allows partitions to operate independently and provide scalability and parallelism."

---

### Q2. Can multiple consumers read from the same partition?

> "Yes, consumers from different consumer groups can independently read the same partition. However, within a single consumer group, a partition is assigned to only one consumer at a time, so multiple consumers in the same group don't simultaneously process the same partition."

---

### Q3. How do you decide the number of partitions?

> "I'd consider expected producer throughput, consumer processing throughput, required consumer parallelism, ordering requirements, record size, and expected growth. For example, if a consumer can process around 2,000 events per second and we need 10,000 events per second, we'd need roughly five partitions for that workload, with additional capacity considered for growth and operational requirements. I'd validate the sizing through performance testing rather than relying on a fixed formula."

---

### Q4. When are Kafka events deleted?

> "Kafka doesn't normally delete records after a consumer reads them. Records are retained according to the topic's retention configuration, primarily based on time and/or size. Once data becomes eligible for cleanup, Kafka removes the relevant old log segments. Consumer consumption itself doesn't normally trigger deletion."

---

## 🔥 Four Things to Remember

```text
1️⃣ PARTITION
   → Ordering exists within it

2️⃣ CONSUMER GROUP
   → One consumer owns a partition at a time

3️⃣ PARTITION COUNT
   → Determines available partition-level parallelism

4️⃣ RETENTION
   → Controls how long/how much data Kafka keeps
   → NOT based simply on whether consumers read it
```

And one particularly important mental model:

> **Partitions give Kafka scalability. Replication gives Kafka fault tolerance. Consumer groups give independent consumption and parallel processing. Retention determines how long Kafka keeps the data.**
