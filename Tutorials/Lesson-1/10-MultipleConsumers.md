# 10. How Multiple Consumers Consume Events in Kafka?

This follows directly from what we just learned about **Consumer Groups**.

We already know:

```text
Topic
  ↓
Partitions
  ↓
Consumers
  ↓
Consumer Group
```

Now let's understand **exactly how multiple consumers consume events**.

---

# 1. First Important Rule 🔥

There are **two different scenarios**:

### Scenario A — Multiple consumers in the SAME consumer group

➡️ They **share the partitions**.

### Scenario B — Consumers in DIFFERENT consumer groups

➡️ Each group can consume the **same events independently**.

This distinction is extremely important.

---

# 2. Scenario A — Multiple Consumers in Same Group

Suppose we have:

```text
Topic: orders

Partitions:
P0
P1
P2
P3
```

And:

```text
Consumer Group: order-service

Consumers:
C1
C2
C3
C4
```

Kafka distributes the partitions:

```text
                 orders
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
       P0          P1          P2          P3
        │           │           │           │
        ▼           ▼           ▼           ▼
       C1          C2          C3          C4
                 order-service
                 Consumer Group
```

So:

```text
P0 → C1
P1 → C2
P2 → C3
P3 → C4
```

All four consumers are **working together**.

---

# 3. Easy Analogy 👷

Imagine a warehouse has 4 sections:

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

Instead of every worker processing every section:

```text
❌ Worker 1 → A B C D
❌ Worker 2 → A B C D
❌ Worker 3 → A B C D
❌ Worker 4 → A B C D
```

you divide the work:

```text
Worker 1 → Section A
Worker 2 → Section B
Worker 3 → Section C
Worker 4 → Section D
```

That's how consumers in the **same consumer group** work.

---

# 4. Why Does Kafka Do This?

Because it allows **parallel processing**.

Suppose there are 4 partitions:

```text
P0 → 1,000 events
P1 → 1,000 events
P2 → 1,000 events
P3 → 1,000 events
```

With one consumer:

```text
C1
 │
 ├── P0
 ├── P1
 ├── P2
 └── P3
```

One consumer handles everything.

With four consumers:

```text
P0 → C1
P1 → C2
P2 → C3
P3 → C4
```

Four consumers can process in parallel.

---

# 5. What If There Are More Partitions Than Consumers?

Example:

```text
Partitions = 6
Consumers = 3
```

Kafka can distribute:

```text
P0 ──┐
P1 ──┼──► C1
     
P2 ──┐
P3 ──┼──► C2

P4 ──┐
P5 ──┼──► C3
```

So:

```text
C1 → P0, P1
C2 → P2, P3
C3 → P4, P5
```

Each consumer handles multiple partitions.

---

# 6. What If There Are More Consumers Than Partitions?

Example:

```text
Partitions = 3
Consumers = 5
```

Kafka has only 3 partitions to distribute:

```text
P0 → C1
P1 → C2
P2 → C3

C4 → idle
C5 → idle
```

So:

> **Consumers cannot create additional parallelism if there aren't additional partitions.**

This is one of the most important relationships:

```text
Partitions
    ↓
Available parallel work
    ↓
Consumers
```

---

# 7. Scenario B — Different Consumer Groups

Now let's change the architecture.

Topic:

```text
orders
```

Partitions:

```text
P0
P1
P2
```

We have:

```text
Payment Group
Inventory Group
Analytics Group
```

Architecture:

```text
                         orders
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
        payment-group inventory-group analytics-group
              │            │            │
             P0           P0           P0
             P1           P1           P1
             P2           P2           P2
```

Now **each consumer group gets the events independently**.

---

# 8. Real Example

Suppose an event is:

```text
Order #1001 Created
```

Three systems need it:

### Payment Service

```text
payment-group
```

uses the event to process payment.

### Inventory Service

```text
inventory-group
```

uses the event to reserve stock.

### Analytics Service

```text
analytics-group
```

uses the event for analytics.

So:

```text
                    Order #1001
                         │
                         ▼
                    Kafka Topic
                    order-events
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
     Payment Group   Inventory Group  Analytics Group
          │              │              │
          ▼              ▼              ▼
       Payment        Inventory       Analytics
```

The event is effectively available to **all three groups**.

---

# 9. Same Group vs Different Groups

This is the most important comparison.

|                                             | Same Consumer Group            | Different Consumer Groups          |
| ------------------------------------------- | ------------------------------ | ---------------------------------- |
| Purpose                                     | Share workload                 | Independent applications           |
| Partition assignment                        | Shared                         | Each group gets its own assignment |
| Same event processed by multiple consumers? | Generally no, within the group | Yes, across groups                 |
| Parallel processing                         | Yes                            | Yes                                |
| Example                                     | 4 payment workers              | Payment + Inventory + Analytics    |

### Remember:

```text
SAME GROUP
    ↓
Share the work


DIFFERENT GROUPS
    ↓
Each gets the work independently
```

---

# 10. Example With 3 Partitions

Let's make this very clear.

Topic:

```text
orders
```

```text
P0:
A B C

P1:
D E F

P2:
G H I
```

## Group 1

```text
payment-group

C1 → P0
C2 → P1
C3 → P2
```

So:

```text
C1 gets A B C
C2 gets D E F
C3 gets G H I
```

---

## Group 2

```text
inventory-group

C4 → P0
C5 → P1
C6 → P2
```

So:

```text
C4 gets A B C
C5 gets D E F
C6 gets G H I
```

Therefore:

```text
                orders
                   │
          ┌────────┴────────┐
          ▼                 ▼
   payment-group      inventory-group
      │   │   │          │   │   │
      ▼   ▼   ▼          ▼   ▼   ▼
     P0  P1  P2          P0  P1  P2
```

**Both groups consume the same topic independently.**

---

# 11. Why This Is Powerful

Imagine your company has:

```text
Order Service
Payment Service
Inventory Service
Notification Service
Analytics Service
```

Instead of creating separate topics for every application:

```text
orders-for-payment
orders-for-inventory
orders-for-notification
orders-for-analytics
```

you can have:

```text
order-events
```

and use different consumer groups:

```text
order-events
     │
     ├── payment-group
     ├── inventory-group
     ├── notification-group
     └── analytics-group
```

This is one of the fundamental patterns that makes Kafka useful for **event streaming**.

---

# 12. Ordering Still Works

Remember our previous topic:

> **Ordering is guaranteed within a partition.**

Suppose:

```text
P0:

Offset 0 → Order Created
Offset 1 → Payment Started
Offset 2 → Payment Completed
```

If:

```text
payment-group
```

is consuming P0:

```text
C1 → P0
```

it reads the partition's records in their partition order:

```text
0 → 1 → 2
```

Another group:

```text
analytics-group
```

can independently consume P0:

```text
C2 → P0
```

and also see:

```text
0 → 1 → 2
```

So different groups have their own independent consumption of the partition.

---

# 13. Multiple Consumers Don't Mean Multiple Copies Inside the Same Group

This is a common misunderstanding.

Suppose:

```text
P0 → Event A
```

and:

```text
C1
C2
C3
```

all belong to:

```text
payment-group
```

You don't get:

```text
❌ C1 → Event A
❌ C2 → Event A
❌ C3 → Event A
```

Instead, one consumer in that group handles that partition:

```text
✅ P0 → C1 → Event A
```

But if you have:

```text
payment-group
inventory-group
analytics-group
```

then the event can be consumed by each group:

```text
P0 → payment-group
P0 → inventory-group
P0 → analytics-group
```

---

# 14. Senior Platform Engineer Perspective

When designing a Kafka application, one of the first questions is:

> **"Should these consumers share the workload or should each application independently receive the events?"**

### If they should share workload:

Use the **same group ID**.

```text
payment-service-1
payment-service-2
payment-service-3

group.id = payment-service
```

They collectively process the topic.

### If they should independently receive events:

Use **different group IDs**.

```text
payment-service
inventory-service
analytics-service
```

Each gets its own consumption of the topic.

---

# 15. Production Example

Imagine:

```text
Topic:
payment-events

Partitions:
6
```

Payment application needs 3 instances:

```text
payment-group

C1
C2
C3
```

Possible assignment:

```text
P0 → C1
P1 → C1

P2 → C2
P3 → C2

P4 → C3
P5 → C3
```

Now your analytics application also needs the same payment events.

Create:

```text
analytics-group
```

with 2 consumers:

```text
A1
A2
```

Possible assignment:

```text
P0 → A1
P1 → A1
P2 → A1

P3 → A2
P4 → A2
P5 → A2
```

Now:

```text
                  payment-events
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
       payment-group          analytics-group
        C1 C2 C3                A1 A2
```

Both applications independently process the same topic.

---

# 16. The Core Mental Model 🔥

Keep this diagram in your mind:

```text
                         KAFKA TOPIC
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
                   P0        P1        P2
                    │         │         │
          ┌─────────┴─────────┴─────────┴─────────┐
          │                                       │
          ▼                                       ▼
   SAME CONSUMER GROUP                    DIFFERENT GROUPS
          │                                       │
     Share P0/P1/P2                       Each group gets
          │                               its own P0/P1/P2
       C1 C2 C3                                  │
                                                 ▼
                                  Payment / Inventory / Analytics
```

### 🔥 Memorize these 4 rules:

1. **One partition → one active consumer within a consumer group.**
2. **Multiple partitions → multiple consumers can work in parallel.**
3. **Consumers > partitions → some consumers are idle.**
4. **Different consumer groups → each group can consume the same events independently.**

That is the core of **how multiple consumers consume events in Kafka**, without jumping into the later Kafka topics.
