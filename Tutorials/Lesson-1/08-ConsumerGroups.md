# 8. Kafka Consumer Groups

Now we are moving to the **next concept after Offset**.

We already know:

```text
Kafka
  ↓
Topic
  ↓
Partitions
  ↓
Records
  ↓
Ordering
  ↓
Offsets
```

Now we introduce:

```text
Consumer Groups
```

I’ll keep this explanation **only around Consumer Groups** and the concepts you've already covered.

---

# 1. What is a Consumer Group?

* A **Consumer Group** is a group of Kafka consumers that work together to consume records from a topic.
* Kafka treats all consumers having the same **group ID** as one logical group.
* Within a consumer group:

  * Partitions are distributed among the consumers.
  * A partition is consumed by **one consumer within that group at a time**.
* Different consumer groups can consume the same topic independently.

### Simple definition

> **Consumer Group = A set of consumers working together to consume the partitions of a Kafka topic.**

<img width="1227" height="803" alt="image" src="https://github.com/user-attachments/assets/b5dbebe1-82c4-454b-887d-cdbf5b36c7c8" />

<img width="1844" height="1013" alt="image" src="https://github.com/user-attachments/assets/0cafcd2a-bc10-4044-80e1-c7325d7b9f80" />



---

# 2. Easy Analogy 🏭

Imagine you have a warehouse with **4 sections**:

```text
Warehouse
│
├── Section A
├── Section B
├── Section C
└── Section D
```

You have 4 workers:

```text
Worker 1
Worker 2
Worker 3
Worker 4
```

Instead of all workers processing every section, each worker handles a section:

```text
Section A → Worker 1
Section B → Worker 2
Section C → Worker 3
Section D → Worker 4
```

This is similar to:

```text
Kafka Topic
│
├── Partition 0 → Consumer 1
├── Partition 1 → Consumer 2
├── Partition 2 → Consumer 3
└── Partition 3 → Consumer 4
```

The **workers together form the Consumer Group**.

---

# 3. Basic Architecture

Suppose we have:

```text
Topic: orders
Partitions: 3
```

And one consumer group:

```text
Group ID: order-service
```

Architecture:

```text
                     Kafka
                       │
                 Topic: orders
                       │
             ┌─────────┼─────────┐
             ▼         ▼         ▼
            P0        P1        P2
             │         │         │
             ▼         ▼         ▼
            C1        C2        C3
             │         │         │
             └─────────┼─────────┘
                       │
                Consumer Group
                 "order-service"
```

Here:

```text
P0 → C1
P1 → C2
P2 → C3
```

All three consumers belong to:

```text
order-service
```

---

# 4. Why Do We Need Consumer Groups?

The biggest reason is **parallel processing**.

Suppose you have:

```text
Topic
│
├── P0
├── P1
├── P2
└── P3
```

One consumer:

```text
C1 → P0,P1,P2,P3
```

Everything is processed by one consumer.

Now create a consumer group with 4 consumers:

```text
P0 → C1
P1 → C2
P2 → C3
P3 → C4
```

Now four consumers can process records in parallel.

### Therefore:

> **Partitions provide the parallel units, and Consumer Groups allow consumers to process those partitions in parallel.**

---

# 5. Consumer Group and Partitions

This relationship is extremely important.

Suppose:

```text
Partitions = 4
Consumers = 2
```

Kafka can distribute:

```text
P0 ──► C1
P1 ──► C1

P2 ──► C2
P3 ──► C2
```

So:

```text
C1 → 2 partitions
C2 → 2 partitions
```

Each consumer can handle multiple partitions.

---

# 6. What If Consumers = Partitions?

Suppose:

```text
Partitions = 4
Consumers = 4
```

You can have:

```text
P0 → C1
P1 → C2
P2 → C3
P3 → C4
```

This is a straightforward parallel setup.

---

# 7. What If Consumers > Partitions?

This is very important.

Suppose:

```text
Partitions = 3
Consumers = 5
```

You cannot assign every consumer a partition.

You might have:

```text
P0 → C1
P1 → C2
P2 → C3

C4 → No partition
C5 → No partition
```

So C4 and C5 are idle.

### Important rule 🔥

> **Within a consumer group, the number of actively consuming consumers cannot exceed the number of partitions.**

---

# 8. Why Can't Two Consumers in the Same Group Read the Same Partition?

Suppose:

```text
Topic
│
└── P0
```

Consumer Group:

```text
C1
C2
```

Kafka does **not** normally do:

```text
P0 → C1
P0 → C2
```

Instead:

```text
P0 → C1
C2 → idle
```

This allows the partition's ordered stream to be processed by one consumer within that group.

Remember our previous topic:

> **Ordering is guaranteed within a partition.**

Consumer groups work with that partition model.

---

# 9. Multiple Consumer Groups

This is where Consumer Groups become especially powerful.

Suppose:

```text
Topic: orders
```

You have three applications:

```text
Payment Service
Inventory Service
Analytics Service
```

Create three consumer groups:

```text
payment-group
inventory-group
analytics-group
```

Architecture:

```text
                         orders
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
       payment-group  inventory-group  analytics-group
             │             │             │
             ▼             ▼             ▼
        Payment App    Inventory App   Analytics App
```

Each group can consume the same topic independently.

---

# 10. Why Multiple Groups?

Imagine an order event:

```text
Order #1001 Created
```

Three applications are interested:

```text
Payment Service
Inventory Service
Analytics Service
```

We don't want the payment application to consume the event and make it unavailable to inventory.

Instead:

```text
                    orders
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
     Payment       Inventory      Analytics
      Group          Group          Group
```

Each group has its **own consumption progress**.

This is one of the fundamental ideas behind Kafka's event-streaming model.

---

# 11. Same Topic, Different Groups

Suppose:

```text
Topic: orders
```

has:

```text
P0
P1
P2
```

### Group 1

```text
payment-group

P0 → Consumer A
P1 → Consumer B
P2 → Consumer C
```

### Group 2

```text
inventory-group

P0 → Consumer D
P1 → Consumer E
P2 → Consumer F
```

So the same partitions can be consumed by different groups independently.

```text
                    orders
                       │
             ┌─────────┴─────────┐
             ▼                   ▼
       payment-group       inventory-group
             │                   │
          P0 P1 P2            P0 P1 P2
```

---

# 12. Consumer Group + Offset

This connects directly to what you just learned about **offsets**.

Suppose:

```text
Topic: orders
Partition: P0
```

contains:

```text
Offset 0 → A
Offset 1 → B
Offset 2 → C
Offset 3 → D
```

Now:

```text
payment-group
```

has processed:

```text
0
1
2
```

while:

```text
analytics-group
```

has only processed:

```text
0
1
```

Their progress can be different:

```text
P0:

0     1     2     3
│     │     │     │
└─────┴─────┴─────┴────── Records

payment-group
              ↑
          further ahead

analytics-group
        ↑
     behind
```

### Key idea

> **Consumer group maintains its own progress through the partitions it consumes.**

This is why the same topic can be consumed independently by different applications.

---

# 13. Consumer Group ID

Every consumer belongs to a group using a **group ID**.

Example:

```text
group.id=payment-service
```

Another application:

```text
group.id=inventory-service
```

These are two separate consumer groups.

Think:

```text
group.id
   ↓
Logical identity of the consumer group
```

---

# 14. Same Group ID vs Different Group ID

This is a common interview question.

### Same Group ID

```text
C1 → group = payment
C2 → group = payment
C3 → group = payment
```

They work together.

```text
       payment-group
       /      |      \
      C1      C2      C3
```

Partitions are distributed among them.

---

### Different Group IDs

```text
C1 → group = payment
C2 → group = inventory
```

They are independent consumers.

```text
payment-group
      │
      ▼
   Topic

inventory-group
      │
      ▼
   Topic
```

Both can consume the same events independently.

---

# 15. Consumer Group Example

Let's use an e-commerce system.

Topic:

```text
order-events
```

Partitions:

```text
P0
P1
P2
```

### Payment application

```text
payment-group

Payment Consumer 1 → P0
Payment Consumer 2 → P1
Payment Consumer 3 → P2
```

### Inventory application

```text
inventory-group

Inventory Consumer 1 → P0
Inventory Consumer 2 → P1
Inventory Consumer 3 → P2
```

### Analytics application

```text
analytics-group

Analytics Consumer 1 → P0
Analytics Consumer 2 → P1
Analytics Consumer 3 → P2
```

So:

```text
                         order-events
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
    payment-group      inventory-group      analytics-group
          │                   │                   │
      P0 P1 P2             P0 P1 P2             P0 P1 P2
```

---

# 16. Consumer Group = Competing Consumers

When multiple consumers belong to the **same group**, they are essentially **competing to process the partitions**.

Example:

```text
Topic
│
├── P0 ──► C1
├── P1 ──► C2
├── P2 ──► C3
└── P3 ──► C4
```

All consumers belong to:

```text
payment-group
```

The objective is:

> **Process the workload collectively.**

This resembles a traditional **message queue worker model** that we discussed earlier.

---

# 17. Consumer Group = Independent Subscribers

When consumers use **different group IDs**, they behave more like independent subscribers to an event stream.

```text
                    Kafka Topic
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
       Group A        Group B        Group C
          │             │             │
       Payment       Inventory      Analytics
```

So Kafka gives us both behaviors:

```text
Same Group ID
     ↓
Work sharing / parallel processing


Different Group IDs
     ↓
Independent consumption
```

---

# 18. Important Relationship

You should now have this mental model:

```text
                     Kafka Topic
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
             P0         P1         P2
              │          │          │
              └──────────┼──────────┘
                         │
                  Consumer Group
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
             C1         C2         C3
```

And:

```text
Partitions
    ↓
Provide parallel units

Consumer Group
    ↓
Uses those partitions collectively

Consumers
    ↓
Process assigned partitions
```

---

# 19. Senior Platform Engineer Perspective

When you see a production Kafka consumer group, think about:

```text
Consumer Group
      │
      ├── Group ID
      ├── Consumers
      ├── Topic
      ├── Partitions
      └── Offset progress
```

Questions you'd ask:

* How many partitions does the topic have?
* How many consumers are in the group?
* Is the workload distributed evenly?
* Is the number of consumers appropriate for the partition count?
* Which application owns this consumer group?
* Where is each consumer's progress?
* Is the group processing the topic as expected?

We're intentionally **not going into the later operational concepts yet**; those will come when you reach them in your sequence.

---

# 20. Production Example

Imagine your company has:

```text
Topic: payment-events
Partitions: 6
```

Your payment service has:

```text
Consumer Group:
payment-service

Consumers:
C1
C2
C3
```

Kafka can distribute:

```text
P0 → C1
P1 → C1

P2 → C2
P3 → C2

P4 → C3
P5 → C3
```

Therefore:

```text
6 partitions
      ↓
3 consumers
      ↓
2 partitions per consumer
```

The three consumers collectively process the topic.

---

# 21. Scaling Consumers

Suppose you have:

```text
6 partitions
3 consumers
```

Then:

```text
C1 → P0 P1
C2 → P2 P3
C3 → P4 P5
```

You add another consumer:

```text
C4
```

Now Kafka can redistribute the partitions so you may get approximately:

```text
C1 → P0 P1
C2 → P2
C3 → P3 P4
C4 → P5
```

The exact assignment depends on the partition assignment strategy.

For now, the important concept is simply:

> **Adding consumers can increase parallelism only while there are available partitions to distribute.**

---

# 22. The Most Important Rule 🔥

Memorize this:

```text
                 PARTITIONS
                     │
                     ▼
             Maximum parallelism
                     │
                     ▼
              CONSUMER GROUP
                     │
                     ▼
                CONSUMERS
```

For one consumer group:

```text
Partitions > Consumers
→ Consumers handle multiple partitions


Partitions = Consumers
→ Each consumer can handle one partition


Partitions < Consumers
→ Some consumers are idle
```

---

# 23. Interview Questions

### Q1. What is a Kafka Consumer Group?

> A Consumer Group is a logical group of Kafka consumers that work together to consume partitions of a topic. Within a group, partitions are distributed among consumers so that the workload can be processed in parallel.

---

### Q2. Can two consumers in the same group consume the same partition?

> No. A partition is assigned to only one active consumer within a consumer group at a time.

---

### Q3. Can two different consumer groups consume the same partition?

> Yes. Different consumer groups independently consume the same topic and maintain their own progress.

---

### Q4. What happens if there are more consumers than partitions?

> Some consumers remain idle because there aren't enough partitions to assign one to each consumer.

---

### Q5. Why are consumer groups important?

> They provide parallel processing of partitions and allow multiple independent applications to consume the same Kafka topic using different group IDs.

---

# 24. Final Mental Model 🧠

Keep this picture in your mind:

```text
                       Kafka Topic
                           │
                ┌──────────┼──────────┐
                ▼          ▼          ▼
               P0         P1         P2
                │          │          │
                └──────────┼──────────┘
                           │
                    ┌──────┴──────┐
                    │ Consumer    │
                    │   Group     │
                    └──────┬──────┘
                           │
                ┌──────────┼──────────┐
                ▼          ▼          ▼
               C1         C2         C3
```

### Remember these 5 lines:

> **Topic contains partitions.**

> **Partitions contain ordered records.**

> **Consumers read partitions.**

> **Consumers with the same group ID work together.**

> **Different group IDs consume independently.**

That's all you need for **Consumer Groups at this stage**. We can build the next concept on top of this without jumping ahead.
