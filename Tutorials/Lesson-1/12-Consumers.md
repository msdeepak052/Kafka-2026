# 11. Kafka Consumer

Now we move one step further in the sequence:

```text
Kafka Topic
    ↓
Partitions
    ↓
Ordering
    ↓
Offset
    ↓
Consumer Groups
    ↓
👉 Kafka Consumer
```

We'll focus specifically on **what a Kafka Consumer is and how it reads data**, without jumping into future Kafka concepts.

---

# 1. What is a Kafka Consumer?

* A **Kafka Consumer** is an application/process that reads records from Kafka topics.
* It connects to a Kafka cluster and requests records from partitions.
* A consumer doesn't receive records simply because they exist.
* It **pulls/requests records** from Kafka.
* A consumer keeps track of its position using **offsets**.
* A consumer can belong to a **consumer group**.

### Simple definition

> **Kafka Consumer = An application/process that reads records from Kafka topic partitions.**

---
<img width="1844" height="1013" alt="image" src="https://github.com/user-attachments/assets/8081c5d3-1414-4f60-923e-32364da3a7e2" />



---

# 2. Easy Analogy 📦

Imagine Kafka as a warehouse:

```text
Kafka Warehouse
│
├── Section 0
├── Section 1
└── Section 2
```

These are:

```text
P0
P1
P2
```

A worker comes to the warehouse and says:

> "Give me the next items from Section 0."

That worker is the **Kafka Consumer**.

```text
Kafka Partition
      │
      │ records
      ▼
  Consumer
      │
      ▼
Application processing
```

---

# 3. Basic Architecture

```text
                  Kafka Cluster
                       │
                  Topic: orders
                       │
             ┌─────────┼─────────┐
             ▼         ▼         ▼
            P0        P1        P2
             │         │         │
             │         │         │
             ▼         ▼         ▼
            C1        C2        C3
             │         │         │
             └─────────┼─────────┘
                       ▼
                 Application
```

Here:

```text
P0 → C1
P1 → C2
P2 → C3
```

The consumers are reading records from their assigned partitions.

---

# 4. What Does a Consumer Actually Do?

Conceptually, a consumer follows this process:

```text
1. Connect to Kafka
       ↓
2. Identify the topic
       ↓
3. Read assigned partition
       ↓
4. Request records
       ↓
5. Receive records
       ↓
6. Process records
       ↓
7. Continue from its position/offset
```

Example:

```text
Partition P0

Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
Offset 3 → Order D
```

Consumer starts reading:

```text
C1
 │
 ├── reads A
 ├── reads B
 ├── reads C
 └── reads D
```

---

# 5. Consumer Uses Offset

This connects directly to the topic we just covered.

Suppose:

```text
P0

0 → A
1 → B
2 → C
3 → D
4 → E
```

Consumer has read:

```text
A
B
C
```

Its position is around:

```text
0 → 1 → 2
          ↑
       consumer
```

The offset tells the consumer **where it is in the partition**.

So a consumer doesn't have to start from the beginning every time.

---

# 6. Consumer Reads From a Partition

Suppose:

```text
Topic: payments

P0
P1
P2
```

A consumer can be assigned:

```text
C1 → P0
```

Then:

```text
P0:

Offset 0 → Payment A
Offset 1 → Payment B
Offset 2 → Payment C
Offset 3 → Payment D
```

C1 reads:

```text
A → B → C → D
```

Because the records are in the ordered partition log.

---

# 7. Consumer and Ordering

We already learned:

> **Kafka guarantees ordering within a partition.**

Therefore, if a consumer reads:

```text
P0:

0 → A
1 → B
2 → C
3 → D
```

it reads the partition's records in that sequence.

```text
A
 ↓
B
 ↓
C
 ↓
D
```

But if there are multiple partitions:

```text
P0 → A → B → C
P1 → X → Y → Z
```

the consumer doesn't get a single global ordering between P0 and P1.

That's because the ordering guarantee belongs to the **partition**, not the entire topic.

---

# 8. One Consumer Can Read Multiple Partitions

Suppose:

```text
Topic:
orders

Partitions:
P0
P1
P2
```

Only one consumer:

```text
C1
```

C1 can be responsible for:

```text
C1
├── P0
├── P1
└── P2
```

Conceptually:

```text
P0 ──┐
     │
P1 ──┼──► C1
     │
P2 ──┘
```

So:

> **A consumer is not necessarily equal to one partition.**

A consumer can handle multiple partitions.

---

# 9. Multiple Consumers

Suppose:

```text
Topic
│
├── P0
├── P1
└── P2
```

and:

```text
C1
C2
C3
```

within the same consumer group.

They can work like:

```text
P0 → C1
P1 → C2
P2 → C3
```

This allows parallel processing.

---

# 10. Consumer Group Connection

A consumer can belong to a consumer group.

Example:

```text
Consumer 1
Consumer 2
Consumer 3

group.id = payment-service
```

They form:

```text
          payment-service
          Consumer Group
                │
       ┌────────┼────────┐
       ▼        ▼        ▼
      C1       C2       C3
```

The group collectively consumes the topic's partitions.

---

# 11. Same Consumer vs Different Consumer Groups

Suppose:

```text
Topic: orders
```

### Same group

```text
order-group

C1
C2
C3
```

They share the workload:

```text
P0 → C1
P1 → C2
P2 → C3
```

### Different groups

```text
payment-group
inventory-group
```

Both groups can consume the same topic independently:

```text
                    orders
                       │
              ┌────────┴────────┐
              ▼                 ▼
        payment-group      inventory-group
              │                 │
              ▼                 ▼
        Payment App       Inventory App
```

---

# 12. Real-World Example

Imagine an e-commerce application.

Kafka topic:

```text
order-events
```

Event:

```json
{
  "orderId": 1001,
  "event": "ORDER_CREATED"
}
```

Consumer applications could be:

```text
Order Consumer
Payment Consumer
Inventory Consumer
```

For example:

```text
                    order-events
                         │
            ┌────────────┼────────────┐
            ▼            ▼            ▼
         Payment      Inventory     Analytics
         Consumer      Consumer      Consumer
```

Each consumer reads events relevant to its application.

---

# 13. Consumer Pull Model

An important characteristic of Kafka consumers:

> **Consumers pull records from Kafka.**

Conceptually:

```text
Consumer
   │
   │ "Give me records"
   ▼
Kafka
   │
   │ records
   ▼
Consumer
```

Rather than Kafka continuously pushing every message directly into the application.

This pull-based model allows the consumer to control how quickly it reads data.

For now, just remember:

```text
Consumer → requests → Kafka
Kafka → returns → records
```

---

# 14. Consumer Doesn't Delete the Record

This is important because we learned about offsets.

Suppose:

```text
P0:

0 → A
1 → B
2 → C
```

Consumer reads:

```text
A
B
C
```

The records aren't simply removed because the consumer read them.

Think:

```text
Record
  │
  ├── Consumer reads it
  │
  └── Consumer position moves forward
```

The record remains part of the Kafka partition according to Kafka's storage rules.

For now, just remember:

> **Reading a record and deleting a record are different things.**

---

# 15. Consumer Restart

Suppose:

```text
P0:

0 → A
1 → B
2 → C
3 → D
```

Consumer reads:

```text
A
B
C
```

Then the consumer stops.

When it starts again, Kafka can use the consumer's stored progress/offset information to determine where it should continue.

Conceptually:

```text
A → B → C → D
          ↑
       progress
```

So the consumer doesn't necessarily have to start from:

```text
0
```

every time.

---

# 16. Consumer Example With Offsets

Let's put everything together.

```text
Topic: orders

P0:
0 → Order A
1 → Order B
2 → Order C

P1:
0 → Order D
1 → Order E
2 → Order F
```

Consumer group:

```text
order-service
```

Consumers:

```text
C1
C2
```

Assignment:

```text
P0 → C1
P1 → C2
```

C1:

```text
0 → A
1 → B
2 → C
```

C2:

```text
0 → D
1 → E
2 → F
```

So:

```text
             orders
                │
        ┌───────┴───────┐
        ▼               ▼
       P0              P1
        │               │
        ▼               ▼
       C1              C2
        │               │
        ▼               ▼
     Order App       Order App
```

---

# 17. What Makes a Consumer Different From a Consumer Group?

This distinction is important.

### Consumer

One application/process:

```text
C1
```

### Consumer Group

A logical collection of consumers:

```text
payment-group
│
├── C1
├── C2
└── C3
```

Think:

```text
Consumer = Worker 👷

Consumer Group = Team 👷👷👷
```

---

# 18. Senior Platform Engineer Perspective

When someone says:

> "The Kafka consumer isn't working."

Your first basic questions should be:

```text
Which topic?
     ↓
Which partition?
     ↓
Which consumer?
     ↓
Which consumer group?
     ↓
Where is the consumer's offset?
```

Example:

```text
Topic:
payment-events

Group:
payment-service

Consumer:
payment-consumer-1

Partition:
P2

Position:
Offset 500
```

This gives you a clear picture of what the consumer is doing.

---

# 19. Practical Kafka Consumer Command

Kafka provides a console consumer for testing:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Conceptually:

```text
Kafka Topic
    │
    ▼
orders
    │
    ▼
Console Consumer
    │
    ▼
Terminal
```

If records are published to `orders`, the consumer can display them.

For example:

```text
Order 1001 created
Order 1002 created
Order 1003 created
```

---

# 20. Consumer With a Group ID

You can also specify a consumer group:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service
```

Now the consumer belongs to:

```text
group.id = order-service
```

If you start another consumer with the same group:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service
```

they become members of the same consumer group.

---

# 21. Senior-Level Mental Model

At this stage, connect everything you've learned:

```text
                         KAFKA
                           │
                        Topic
                           │
                ┌──────────┼──────────┐
                ▼          ▼          ▼
               P0         P1         P2
                │          │          │
                ▼          ▼          ▼
               C1         C2         C3
                │          │          │
                └──────────┼──────────┘
                           ▼
                    Consumer Group
```

And each partition has:

```text
Partition
   │
   ├── Ordered records
   │
   └── Offsets
```

So:

> **Consumer reads records → from partitions → in partition order → using offsets to track its position → as part of a consumer group when configured with a group ID.**

---

# 22. 🔥 What You Should Remember

### Kafka Consumer

> Application/process that reads records from Kafka.

### Consumer + Partition

> Consumer reads records from its assigned partition(s).

### Consumer + Offset

> Offset represents the consumer's position in a partition.

### Consumer + Ordering

> Records from a partition are read according to that partition's ordering.

### Consumer + Consumer Group

> Consumers with the same group ID work together and share partitions.

### Multiple Groups

> Different consumer groups can independently consume the same topic.

---

## Final Picture

```text
                     Kafka Topic
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
            P0          P1          P2
             │           │           │
        Ordered       Ordered      Ordered
         Records       Records      Records
             │           │           │
             ▼           ▼           ▼
            C1          C2          C3
             │           │           │
             └───────────┼───────────┘
                         ▼
                  Consumer Group
```

### The one sentence to remember:

> **A Kafka Consumer is a process that reads records from one or more assigned partitions, follows the partition's ordering, and uses offsets to keep track of its position; multiple consumers with the same group ID work together as a consumer group.**

---

# 12. How Do Consumers Consume Events From Kafka?

This is a natural continuation of **Kafka Consumer**.

We already know:

```text
Topic
  ↓
Partitions
  ↓
Records
  ↓
Offsets
  ↓
Consumer
  ↓
Consumer Group
```

Now let's focus on the actual **flow of consumption**:

> **How does a consumer get an event from Kafka?**

---

# 1. The Most Important Point

Kafka consumers use a **pull model**.

That means:

```text
Consumer
   │
   │ "Give me records"
   ▼
Kafka Broker
   │
   │ records
   ▼
Consumer
```

Kafka does **not normally push every event into the consumer**.

Instead:

> **The consumer asks Kafka for records, and Kafka returns available records.**

---

# 2. Easy Analogy 📚

Imagine a library.

You are sitting at a desk.

You don't have a librarian constantly pushing books toward you.

Instead, you say:

> "Give me the next 5 books from shelf 2."

The librarian gives you those books.

Then you say:

> "Give me the next 5."

That's similar to Kafka.

```text
Consumer
   │
   │ Request records
   ▼
Kafka
   │
   │ Return records
   ▼
Consumer
```

---

# 3. Complete Flow

Suppose we have:

```text
Topic: orders
```

with:

```text
P0
```

and records:

```text
P0

Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
Offset 3 → Order D
```

Consumer:

```text
C1
```

The flow is:

```text
C1
 │
 │ 1. Request records
 ▼
Kafka Broker
 │
 │ 2. Find records from partition
 ▼
P0
 │
 │ 3. Return records
 ▼
C1
 │
 │ 4. Process records
 ▼
Application
```

---

# 4. Step-by-Step

## Step 1 — Consumer connects to Kafka

```text
Consumer
   │
   ▼
Kafka Broker
```

The consumer establishes communication with the Kafka cluster.

---

## Step 2 — Consumer knows which topic it wants

For example:

```text
orders
```

Conceptually:

```text
Consumer
   │
   │ "I want records from orders"
   ▼
Kafka
```

---

## Step 3 — Consumer is associated with partition(s)

Suppose:

```text
orders

P0
P1
P2
```

The consumer might be reading:

```text
C1 → P0
```

So C1 is interested in records from P0.

---

# 5. Step 4 — Consumer Requests Records

Suppose P0 contains:

```text
P0

Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
Offset 3 → Order D
```

The consumer asks Kafka for records from a particular position.

Conceptually:

```text
C1
 │
 │ "Give me records from P0"
 │ "starting around offset 0"
 ▼
Broker
```

Kafka looks at P0.

---

# 6. Step 5 — Kafka Returns Records

Kafka responds:

```text
Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
```

So:

```text
Kafka
  │
  │ A, B, C
  ▼
Consumer
```

The consumer receives the records.

---

# 7. Step 6 — Consumer Processes the Records

Now the application processes:

```text
A
↓
B
↓
C
```

For example:

```text
Order A → Update database
Order B → Update database
Order C → Update database
```

The exact application processing depends on the application.

---

# 8. Step 7 — Consumer Continues

Suppose the next available record is:

```text
Offset 3 → Order D
```

The consumer can request more:

```text
Consumer
   │
   │ "Give me more records"
   ▼
Kafka
   │
   ▼
Order D
```

So the overall process continues:

```text
Request
   ↓
Receive records
   ↓
Process
   ↓
Request more
   ↓
Receive
   ↓
Process
   ↓
...
```

---

# 9. Important: Consumer Doesn't Ask for Just One Event Every Time

Kafka is designed for efficient data transfer.

A consumer can receive **multiple records in a fetch**.

For example:

```text
Consumer
   │
   │ Request
   ▼
Kafka
   │
   ├── Event A
   ├── Event B
   ├── Event C
   ├── Event D
   └── Event E
        │
        ▼
    Consumer
```

This avoids doing a network request for every single record.

---

# 10. Multiple Partitions

Now let's make it slightly bigger.

Topic:

```text
orders
```

```text
P0:
0 → A
1 → B
2 → C

P1:
0 → D
1 → E
2 → F
```

Consumer group:

```text
order-group
```

Consumers:

```text
C1
C2
```

Assignment:

```text
P0 → C1
P1 → C2
```

Now:

```text
             orders
                │
        ┌───────┴───────┐
        ▼               ▼
       P0              P1
        │               │
        ▼               ▼
       C1              C2
```

C1 requests records from P0:

```text
C1 → Kafka → A,B,C
```

C2 requests records from P1:

```text
C2 → Kafka → D,E,F
```

They can consume in parallel.

---

# 11. What Does the Offset Do Here?

This is where our previous topic becomes important.

Suppose:

```text
P0

0 → A
1 → B
2 → C
3 → D
```

Consumer has progressed through:

```text
0
1
2
```

Now it knows its position around:

```text
3
```

So its next request can effectively ask for records starting from that position.

```text
P0

0 → A ✓
1 → B ✓
2 → C ✓
3 → D   ← next
```

Therefore:

> **Offset tells the consumer where it is in the partition's sequence.**

---

# 12. Consumer Does Not Search the Entire Topic

This is important.

Suppose:

```text
orders
│
├── P0
├── P1
└── P2
```

If C1 is working with P0:

```text
C1 → P0
```

it reads from **P0**, not randomly from the entire topic.

Remember:

> **Kafka stores records in partitions, and consumers consume from partitions.**

---

# 13. Ordering During Consumption

Suppose P0 contains:

```text
P0

0 → A
1 → B
2 → C
3 → D
```

The consumer reads:

```text
A → B → C → D
```

because the partition is ordered.

But suppose:

```text
P0 → A → B → C
P1 → X → Y → Z
```

You don't get a global sequence such as:

```text
A → X → B → Y → C → Z
```

because P0 and P1 are separate ordered streams.

---

# 14. Pull Model vs Push Model

### Kafka Consumer

```text
Consumer
   │
   │ REQUEST
   ▼
Kafka
   │
   │ RESPONSE
   ▼
Consumer
```

### Push-style system

Conceptually:

```text
Server
   │
   │ PUSH
   ▼
Consumer
```

Kafka's pull model gives the consumer control over how it fetches records.

For now, the important thing is simply:

> **Consumer asks → Kafka responds.**

---

# 15. Real-World Example — Order Processing

Suppose an e-commerce application generates:

```text
Order 1001
Order 1002
Order 1003
Order 1004
```

Kafka:

```text
orders

P0:
0 → Order 1001
1 → Order 1002
2 → Order 1003
3 → Order 1004
```

Consumer:

```text
Order Service Consumer
```

Flow:

```text
                 Kafka
                   │
              orders / P0
                   │
                   │ records
                   ▼
             Order Consumer
                   │
                   ▼
             Order Processing
                   │
                   ▼
              Application
```

The consumer repeatedly fetches records and processes them.

---

# 16. Multiple Consumers Example

Suppose:

```text
Topic: orders
Partitions: 4
```

```text
P0
P1
P2
P3
```

Consumer group:

```text
order-service
```

Consumers:

```text
C1
C2
C3
C4
```

Possible assignment:

```text
P0 → C1
P1 → C2
P2 → C3
P3 → C4
```

Consumption happens like:

```text
C1 ──fetch──► P0
C2 ──fetch──► P1
C3 ──fetch──► P2
C4 ──fetch──► P3
```

So Kafka can process multiple partitions concurrently.

---

# 17. The Whole Process in One Diagram 🔥

```text
                         KAFKA CLUSTER
                              │
                         Topic: orders
                              │
                 ┌────────────┼────────────┐
                 ▼            ▼            ▼
                P0           P1           P2
                 │            │            │
              A B C        D E F        G H I
                 │            │            │
                 ▼            ▼            ▼
                C1           C2           C3
                 │            │            │
                 │ FETCH      │ FETCH      │ FETCH
                 └────────────┼────────────┘
                              ▼
                         Application
```

The key flow is:

```text
Consumer
   │
   │ FETCH
   ▼
Kafka Broker
   │
   ▼
Partition
   │
   ▼
Records
   │
   ▼
Consumer
   │
   ▼
Process
```

---

# 18. Senior Platform Engineer Perspective

When you're troubleshooting a consumer, think through this chain:

```text
Consumer
   ↓
Consumer Group
   ↓
Partition
   ↓
Offset
   ↓
Fetch records
   ↓
Application processes records
```

At this stage, the important questions are:

* Which **topic** is the consumer reading?
* Which **partition(s)** is it reading?
* Which **consumer group** does it belong to?
* What **offset** is it reading from?
* Is Kafka returning records?
* Is the application processing those records?

---

# 19. Interview Answer 🎯

If an interviewer asks:

> **"How does a Kafka consumer consume messages?"**

A good answer is:

> "Kafka consumers use a pull-based model. A consumer connects to the Kafka cluster, reads records from its assigned topic partitions, and fetches records from a specific position in the partition using offsets. Kafka returns records, usually in batches, and the consumer processes them and continues fetching subsequent records. Multiple consumers in the same consumer group can consume different partitions in parallel, while different consumer groups can independently consume the same topic."

---

# 20. 🔥 Remember This Flow

```text
        Consumer
            │
            │ 1. Connect
            ▼
       Kafka Broker
            │
            │ 2. Request records
            ▼
        Partition
            │
            │ 3. Read from offset
            ▼
         Records
            │
            │ 4. Return
            ▼
        Consumer
            │
            │ 5. Process
            ▼
       Application
            │
            │ 6. Continue
            └──────────────► Fetch more
```

### The one-line mental model:

> **Kafka Consumer = Connect → fetch records from a partition starting at its position → process records → continue fetching.**

And this stays completely within the concepts you've covered so far: **topics, partitions, ordering, offsets, consumers, and consumer groups.**
