# Kafka Consumer Groups CLI

The CLI for managing and inspecting Kafka consumer groups is:

```bash
kafka-consumer-groups.sh
```

This is an important CLI for a **Senior Platform Engineer**, because it lets you inspect what a consumer group is doing and troubleshoot its consumption.

---

## 1. What does `kafka-consumer-groups.sh` do?

It is used to:

* List consumer groups
* Describe a consumer group
* See which partitions are assigned to consumers
* See consumer offsets
* See lag
* Inspect the state of a consumer group
* Perform certain consumer-group administrative operations

Basic structure:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  <operation>
```

---

# 2. Architecture

```text
                 Platform Engineer
                         │
                         ▼
             kafka-consumer-groups.sh
                         │
                         │ --bootstrap-server
                         ▼
                  Kafka Broker
                         │
                         ▼
                 Consumer Group
                  "order-service"
                         │
             ┌───────────┼───────────┐
             ▼           ▼           ▼
            C1          C2          C3
             │           │           │
             ▼           ▼           ▼
            P0          P1          P2
```

The CLI connects to Kafka through the **broker**.

---

# 3. Easy Analogy

Imagine a company has a delivery team:

```text
Delivery Team: order-service

Worker 1
Worker 2
Worker 3
```

A manager wants to know:

* Which worker is working?
* Which area is each worker handling?
* How many deliveries have they completed?
* How many are pending?

That's essentially what:

```bash
kafka-consumer-groups.sh
```

helps you inspect.

---

# 4. List Consumer Groups

To see consumer groups:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Example:

```text
order-service
payment-service
analytics-service
```

You can think of this as:

```text
Kafka Cluster
│
├── order-service
├── payment-service
└── analytics-service
```

---

# 5. Describe a Consumer Group

This is probably the **most important command** you'll use.

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

You may see output similar to:

```text
GROUP          TOPIC    PARTITION   CURRENT-OFFSET   LOG-END-OFFSET   LAG
order-service  orders   0           120              125              5
order-service  orders   1           210              210              0
order-service  orders   2           98               110              12
```

---

# 6. Understand the Output

This is extremely important.

### GROUP

```text
order-service
```

The consumer group being inspected.

---

### TOPIC

```text
orders
```

The topic being consumed.

---

### PARTITION

```text
0
```

The specific partition.

For example:

```text
orders-P0
```

---

### CURRENT-OFFSET

```text
120
```

The group's current committed position for that partition.

Think:

```text
Consumer Group
      ↓
"I'm currently around offset 120"
```

---

### LOG-END-OFFSET

```text
125
```

The current end position of the partition log.

Conceptually:

```text
Partition

120 ← consumer position
121
122
123
124
125 ← log end
```

---

### LAG

```text
5
```

Conceptually:

```text
Log End Offset
       -
Consumer Offset
       =
Lag
```

Example:

```text
125 - 120 = 5
```

So the consumer group is behind by approximately **5 records** for that partition.

---

# 7. Consumer Lag

This is one of the most important things you'll use this CLI for.

Example:

```text
PARTITION    CURRENT    LOG-END    LAG
P0           120        125        5
P1           210        210        0
P2           98         110        12
```

You immediately see:

```text
P0 → 5 behind
P1 → 0 behind
P2 → 12 behind
```

Therefore:

```text
P2
 ↓
Highest lag
 ↓
Potential area to investigate
```

---

# 8. Real-World Example

Suppose your application team says:

> "The order processing application is slow."

You run:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server kafka-01:9092 \
  --describe \
  --group order-service
```

Output:

```text
GROUP          TOPIC    PARTITION   CURRENT-OFFSET   LOG-END-OFFSET   LAG
order-service  orders   0           10000            10000            0
order-service  orders   1           9000             10000            1000
order-service  orders   2           9990             10000            10
```

You immediately notice:

```text
P0 → 0 lag
P1 → 1000 lag  ⚠️
P2 → 10 lag
```

This tells you that **P1 deserves investigation**.

The CLI therefore becomes a practical troubleshooting tool.

---

# 9. Consumer Group + Partition Assignment

Suppose:

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

Consumer group:

```text
order-service
│
├── C1
├── C2
└── C3
```

Possible assignment:

```text
C1 → P0,P1
C2 → P2,P3
C3 → P4,P5
```

The consumer group CLI helps you inspect the group's state and partition consumption.

---

# 10. Why This CLI Matters for Platform Engineering

Think of a production incident:

```text
Application
     │
     ▼
Kafka Consumer
     │
     ▼
"Messages are delayed"
```

Your first investigation may involve:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server kafka-01:9092 \
  --describe \
  --group order-service
```

You can determine:

```text
Consumer Group
       ↓
Topic
       ↓
Partitions
       ↓
Current Offset
       ↓
Log End Offset
       ↓
Lag
```

---

# 11. List Groups

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Example:

```text
order-service
payment-service
analytics-service
```

---

# 12. Describe Group

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

This is the command you should remember first.

---

# 13. Describe Group State

You can also inspect the group state:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service \
  --state
```

This can provide information such as the group's state and number of members.

Conceptually:

```text
Group: order-service

State: Stable
Members: 3
```

The exact output depends on the Kafka version.

---

# 14. Why "Stable" Matters

A healthy active consumer group will commonly be in:

```text
Stable
```

Conceptually:

```text
Consumers
   │
   ▼
Assignments
   │
   ▼
Stable
```

If group membership or assignments are changing, you may see other group states.

For now, remember:

> **Stable generally means the group has completed its current assignment and is operating normally.**

---

# 15. Important Relationship With Offsets

You've already learned:

```text
Partition
   ↓
Offset
```

Now add:

```text
Consumer Group
       ↓
Committed Offset
       ↓
Partition
```

Example:

```text
orders-P0

0
1
2
3
4
5
6
7
8
9
```

Suppose:

```text
order-service → committed offset 7
```

Another group:

```text
analytics-service → committed offset 4
```

They can have completely different positions on the same partition.

---

# 16. Same Topic, Different Consumer Groups

```text
                     orders
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
       order-service        analytics-service
           Group A                Group B
              │                     │
              ▼                     ▼
        Group offsets         Group offsets
```

For example:

```text
orders-P0

order-service      → 100
analytics-service  → 80
```

That's perfectly normal.

Each group tracks its own consumption position.

---

# 17. Important Commands for Now

Keep these in your notes:

### List groups

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Describe group

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

### Describe group state

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service \
  --state
```

---

# 18. Complete Mental Model

```text
                        Kafka Cluster
                              │
                              ▼
                       Consumer Group
                       "order-service"
                              │
                 ┌────────────┼────────────┐
                 ▼            ▼            ▼
                C1           C2           C3
                 │            │            │
                 ▼            ▼            ▼
                P0           P1           P2
                 │            │            │
                 ▼            ▼            ▼
          Current Offset  Current Offset  Current Offset
                 │            │            │
                 └────────────┼────────────┘
                              ▼
                         Group CLI
                              │
                              ▼
              kafka-consumer-groups.sh
```

And when you run:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

you're essentially asking Kafka:

> **"Show me what this consumer group is currently doing and how far behind it is."**

---

## 🔥 Remember These 5 Things

1. **`kafka-consumer-groups.sh` → manage/inspect consumer groups**
2. **`--list` → see consumer groups**
3. **`--describe --group <name>` → inspect a group**
4. **Current Offset vs Log End Offset → understand consumption progress**
5. **Lag → tells you how far the group is behind**

### Most important Platform Engineer command:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server kafka-01:9092 \
  --describe \
  --group order-service
```

This is the command you'll repeatedly use when troubleshooting **consumer groups and lag**.
