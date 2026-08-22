# Kafka Consumers in Groups — CLI with `kafka-console-consumer.sh`

This is the practical CLI version of **Kafka Consumer Groups** using:

```bash
kafka-console-consumer.sh
```

The key idea is:

> **Multiple console consumers using the same `--group` form one Consumer Group, and Kafka distributes partitions of the topic among them.**

---

# 1. Basic Command

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service
```

Here:

```text
--bootstrap-server localhost:9092
        ↓
Kafka broker to initially connect to

--topic orders
        ↓
Topic we want to consume

--group order-service
        ↓
Consumer Group ID
```

---

# 2. Architecture

Suppose `orders` has **6 partitions**:

```text
                         Topic: orders
                              │
          ┌────────┬─────────┼─────────┬────────┬────────┐
          ▼        ▼         ▼         ▼        ▼        ▼
         P0       P1        P2        P3       P4       P5
          │        │         │         │        │        │
          └────────┴─────────┼─────────┴────────┴────────┘
                             │
                             ▼
                  Consumer Group
                  "order-service"
                             │
                ┌────────────┼────────────┐
                ▼            ▼            ▼
               C1           C2           C3
```

Possible assignment:

```text
C1 → P0, P1
C2 → P2, P3
C3 → P4, P5
```

Each consumer is running:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service
```

---

# 3. The Most Important Rule

## Same `--group` → Consumers share the work

```text
Group: order-service

C1 ──► P0
   └──► P1

C2 ──► P2
   └──► P3

C3 ──► P4
   └──► P5
```

They are **not each reading every partition**.

They are working together as one logical consumer application.

---

# 4. Why Use Multiple Consumers?

Imagine:

```text
orders
│
├── P0
├── P1
├── P2
├── P3
├── P4
└── P5
```

With only one consumer:

```text
C1 → P0 P1 P2 P3 P4 P5
```

If processing becomes heavy, add consumers:

```text
C1 → P0 P1
C2 → P2 P3
C3 → P4 P5
```

### Analogy

Think of a warehouse:

```text
6 loading lanes
│
├── Lane 1
├── Lane 2
├── Lane 3
├── Lane 4
├── Lane 5
└── Lane 6
```

Instead of:

```text
Worker 1 → all 6 lanes
```

you have:

```text
Worker 1 → Lane 1,2
Worker 2 → Lane 3,4
Worker 3 → Lane 5,6
```

Kafka does something similar with partitions and consumers.

---

# 5. One Partition → One Consumer in a Group

This is a very important rule:

```text
P0 → C1 ✅
P0 → C2 ❌
```

Within **one consumer group**, a partition can be assigned to **at most one consumer at a time**.

Why?

Because Kafka uses partitions to provide parallel processing while preventing multiple consumers in the same group from independently processing the same partition at the same time.

---

# 6. Example: 3 Partitions, 3 Consumers

Topic:

```text
orders
│
├── P0
├── P1
└── P2
```

Consumers:

```text
order-service
│
├── C1
├── C2
└── C3
```

Possible assignment:

```text
C1 → P0
C2 → P1
C3 → P2
```

Perfect utilization.

---

# 7. Example: 3 Partitions, 5 Consumers

Now start five consumers:

```text
order-service
│
├── C1
├── C2
├── C3
├── C4
└── C5
```

But there are only:

```text
P0
P1
P2
```

Therefore:

```text
C1 → P0
C2 → P1
C3 → P2

C4 → idle
C5 → idle
```

### Important:

> **You cannot get more parallel consumption than the number of partitions for a topic within a consumer group.**

---

# 8. Example: 6 Partitions, 2 Consumers

```text
orders
│
├── P0
├── P1
├── P2
├── P3
├── P4
└── P5
```

Two consumers:

```text
C1
C2
```

Kafka can assign:

```text
C1 → P0 P1 P2
C2 → P3 P4 P5
```

Therefore:

> **One consumer can consume multiple partitions.**

---

# 9. Starting the Consumers

### Terminal 1

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service
```

### Terminal 2

Run the **same command**:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service
```

### Terminal 3

Again:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service
```

Now you have:

```text
Consumer-1
Consumer-2
Consumer-3
       │
       ▼
Same group ID
"order-service"
```

Kafka treats them as members of the same group.

---

# 10. What Happens When a Consumer Joins?

Initially:

```text
6 partitions

C1 → P0 P1 P2 P3 P4 P5
```

Now C2 joins:

```text
C1 → P0 P1 P2
C2 → P3 P4 P5
```

C3 joins:

```text
C1 → P0 P1
C2 → P2 P3
C3 → P4 P5
```

The partition assignments are adjusted when group membership changes.

This redistribution is called a **rebalance**.

For now, remember only:

```text
Consumer joins/leaves
        ↓
Group membership changes
        ↓
Partitions may be redistributed
```

---

# 11. What Happens If a Consumer Dies?

Before:

```text
C1 → P0 P1
C2 → P2 P3
C3 → P4 P5
```

Suppose:

```text
C2 💥
```

Kafka can redistribute C2's partitions among the remaining consumers.

For example:

```text
C1 → P0 P1 P2
C3 → P3 P4 P5
```

The important point:

> Kafka doesn't permanently leave those partitions without a consumer just because one consumer failed.

---

# 12. Same Topic + Different Groups

This is extremely important.

Suppose:

```text
Topic: orders
```

You have:

```text
Group A = order-service
Group B = analytics-service
```

Architecture:

```text
                     orders
                        │
             ┌──────────┴──────────┐
             ▼                     ▼
       order-service         analytics-service
          Group A                 Group B
             │                       │
          C1 C2 C3                C1 C2
```

Both groups can consume the same topic independently.

---

# 13. Same Group vs Different Groups

### Same group

```text
orders
  │
  ▼
Group A
 ├── C1
 ├── C2
 └── C3
```

➡️ Consumers **share the work**.

### Different groups

```text
             orders
             /    \
            /      \
       Group A    Group B
```

➡️ Both groups can consume the records independently.

---

# 14. Real-World Example

Suppose your company has:

```text
orders
```

Three applications need the events:

```text
Inventory Service
Payment Service
Analytics Service
```

You don't put all their consumers into one group.

Instead:

```text
orders
  │
  ├──────────────┬──────────────┐
  ▼              ▼              ▼
Inventory      Payment       Analytics
Group          Group          Group
```

Why?

Because each application needs its **own independent consumption** of the order events.

---

# 15. CLI Example With a Real Application

### Inventory consumers

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group inventory-service
```

### Payment consumers

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group payment-service
```

They are different groups:

```text
inventory-service
        │
       C1

payment-service
        │
       C1
```

Both can consume from:

```text
orders
```

independently.

---

# 16. `--from-beginning` With a Group

You can also use:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service \
  --from-beginning
```

This tells the consumer to consume available records starting from the beginning when the group's position has not already been established for the relevant partitions.

For example:

```text
orders-P0

0 → Order-1
1 → Order-2
2 → Order-3
```

A new group can start reading those existing records.

---

# 17. Consumer Group and Offsets

This connects directly to the **Offset** topic you already covered.

Suppose:

```text
orders-P0

0 → Order-1
1 → Order-2
2 → Order-3
3 → Order-4
4 → Order-5
```

Group:

```text
order-service
```

may currently have consumed through:

```text
offset 3
```

Another group:

```text
analytics-service
```

could be at:

```text
offset 1
```

So:

```text
P0
│
├── 0
├── 1       ← analytics position
├── 2
├── 3       ← order-service position
└── 4
```

The groups maintain independent consumption positions.

---

# 18. Very Important Mental Model

Don't think:

```text
Topic → Consumers
```

Think:

```text
Topic
  │
  ▼
Partitions
  │
  ▼
Consumer Group
  │
  ▼
Consumers
```

Example:

```text
orders
│
├── P0 ──────► C1
├── P1 ──────► C1
├── P2 ──────► C2
├── P3 ──────► C2
├── P4 ──────► C3
└── P5 ──────► C3
```

---

# 19. Useful CLI Command

After starting your consumers, you can inspect the group using:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

You'll eventually use this to see things such as:

```text
GROUP
TOPIC
PARTITION
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

For now, the important connection is:

```text
console-consumer
       ↓
--group order-service
       ↓
Consumer Group
       ↓
Partition assignment
```

---

# 20. Final Architecture

```text
                         Kafka Topic
                        "orders"
                            │
             ┌──────────────┼──────────────┐
             ▼              ▼              ▼
            P0             P1             P2
             │              │              │
             └──────────────┼──────────────┘
                            │
                            ▼
                    Consumer Group
                    "order-service"
                            │
                ┌───────────┼───────────┐
                ▼           ▼           ▼
               C1          C2          C3
                │           │           │
              P0,P1       P2,P3       P4,P5
```

With more partitions:

```text
Topic
│
├── P0 ──► C1
├── P1 ──► C1
├── P2 ──► C2
├── P3 ──► C2
├── P4 ──► C3
└── P5 ──► C3
```

### 🔥 Remember these 4 rules

1. **Same `--group` → consumers share the partitions.**
2. **Different groups → independently consume the same topic.**
3. **One partition can be assigned to at most one consumer within a group.**
4. **Consumers > partitions → some consumers remain idle.**

### Interview answer

> **A Kafka consumer group is a logical group of consumers identified by the same group ID. Kafka distributes a topic's partitions among the consumers in that group, allowing horizontal scaling. A partition is assigned to at most one consumer within the group, while different consumer groups can independently consume the same topic.**
