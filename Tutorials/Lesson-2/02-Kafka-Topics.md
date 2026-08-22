Absolutely. I’ll keep the **Kafka Topics CLI** focused and chronological, without jumping into configurations, ACLs, KRaft internals, consumer groups, etc.

# Kafka Topics CLI

## 1. What is `kafka-topics.sh`?

`kafka-topics.sh` is Kafka's CLI tool for performing basic **topic administration**.

You use it to:

* List topics
* Create topics
* Describe topics
* Alter topics
* Delete topics

Basic structure:

```bash
kafka-topics.sh \
  --bootstrap-server <broker>:9092 \
  <operation>
```

---

# 2. Architecture

```text
                  Ubuntu / Admin Machine
                         │
                         ▼
                 kafka-topics.sh
                         │
                         │ --bootstrap-server
                         ▼
                  Kafka Broker
                         │
                         ▼
                  Kafka Cluster
                         │
                         ▼
                    Topic Metadata
```

For example:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

The CLI connects to Kafka through the **broker endpoint** you provide with `--bootstrap-server`.

---

# 3. List Topics

To see all topics:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Example output:

```text
orders
payments
notifications
```

### Analogy

Think of a Kafka cluster as a library.

```text
Library
│
├── Orders
├── Payments
└── Notifications
```

`--list` means:

> "Show me all the books in this library."

---

# 4. Create a Topic

Basic command:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders
```

However, in practice you'll normally specify partitions and replication:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3
```

This means:

```text
Topic: orders

Partitions = 3
Replication Factor = 3
```

---

# 5. What Does This Create?

Conceptually:

```text
orders
│
├── Partition 0
├── Partition 1
└── Partition 2
```

With replication factor 3:

```text
                 orders
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
       P0          P1          P2
      / | \       / | \       / | \
     B1 B2 B3    B2 B3 B1    B3 B1 B2
```

Each partition has three replicas.

---

# 6. Important: Topic ≠ Partition

Don't confuse them.

```text
Topic
  │
  ├── Partition 0
  ├── Partition 1
  └── Partition 2
```

Topic is the **logical stream**.

Partition is the **ordered log inside that topic**.

You've already covered this, so the CLI is simply how you administer it.

---

# 7. Describe a Topic

This is one of the most important commands for a Platform Engineer.

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

Example output:

```text
Topic: orders
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

---

# 8. Understand the Output

For:

```text
Partition: 0
Leader: 1
Replicas: 1,2,3
Isr: 1,2,3
```

### Partition

```text
Partition: 0
```

The partition number is:

```text
orders-0
```

---

### Leader

```text
Leader: 1
```

Broker `1` is currently the leader for this partition.

```text
orders-P0
     │
     ▼
  Broker 1
```

---

### Replicas

```text
Replicas: 1,2,3
```

Partition P0 has replicas on:

```text
Broker 1
Broker 2
Broker 3
```

---

### ISR

```text
Isr: 1,2,3
```

All three replicas are currently in sync.

So:

```text
Replicas = 3
ISR      = 3
```

---

# 9. Real Example

Imagine your company has an order-processing system.

You create:

```bash
kafka-topics.sh \
  --bootstrap-server kafka-01:9092 \
  --create \
  --topic orders \
  --partitions 6 \
  --replication-factor 3
```

You now have:

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

Each partition has three replicas distributed across brokers.

Why?

Because if one broker fails, replicas of the partition can exist on other brokers.

---

# 10. Check Whether a Topic Exists

You can list topics:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Then look for:

```text
orders
```

Or describe the topic directly:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

---

# 11. Increase Partitions

You can increase the number of partitions.

For example:

```text
orders
│
├── P0
├── P1
└── P2
```

Increase to 6:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders \
  --partitions 6
```

Now:

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

### Important

You can **increase partitions**, but you cannot use this command to reduce them.

```text
3 → 6 ✅
6 → 3 ❌
```

---

# 12. Why Can't We Simply Reduce Partitions?

Because partitions are ordered logs.

Suppose:

```text
P0:
offset 0
offset 1
offset 2
offset 3
```

Kafka cannot simply merge partitions while preserving the existing partition/offset structure and ordering semantics.

So:

> **Partition count can be increased, but not decreased through the normal topic alter operation.**

This is important operationally.

---

# 13. Delete a Topic

Command:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic orders
```

Example:

```text
Topic orders is marked for deletion.
```

After deletion:

```text
orders
  ↓
deleted
```

### ⚠️ Production caution

Deleting a topic is a destructive operation.

Before doing it:

```text
Check topic
    ↓
Confirm environment
    ↓
Confirm application dependency
    ↓
Confirm retention/data requirements
    ↓
Delete
```

Never casually execute:

```bash
--delete
```

against a production topic.

---

# 14. `--create` + `--partitions` + `--replication-factor`

This is a command you should know very well:

```bash
kafka-topics.sh \
  --bootstrap-server kafka-01:9092 \
  --create \
  --topic orders \
  --partitions 6 \
  --replication-factor 3
```

Read it as:

> Connect to Kafka through `kafka-01:9092`, create a topic called `orders`, create 6 partitions, and maintain 3 replicas of each partition.

---

# 15. Most Important Options

For now, focus only on these:

| Option                 | Meaning                           |
| ---------------------- | --------------------------------- |
| `--bootstrap-server`   | Kafka broker endpoint             |
| `--list`               | List topics                       |
| `--create`             | Create topic                      |
| `--describe`           | Show topic details                |
| `--alter`              | Modify supported topic properties |
| `--delete`             | Delete topic                      |
| `--topic`              | Specify topic name                |
| `--partitions`         | Number of partitions              |
| `--replication-factor` | Number of replicas per partition  |

---

# 16. Complete Mini Lab

### Create

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
```

### List

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Expected:

```text
orders
```

### Describe

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

### Increase partitions

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders \
  --partitions 6
```

### Verify

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders
```

You should now see:

```text
PartitionCount: 6
```

---

# 17. Senior Platform Engineer Perspective

When troubleshooting a Kafka topic, your first command will often be:

```bash
kafka-topics.sh \
  --bootstrap-server kafka-01:9092 \
  --describe \
  --topic orders
```

You immediately want to understand:

```text
Topic
  ↓
Partitions
  ↓
Leader
  ↓
Replicas
  ↓
ISR
```

For example:

```text
P0 → Leader 1 → Replicas 1,2,3 → ISR 1,2,3
P1 → Leader 2 → Replicas 2,3,1 → ISR 2,3,1
P2 → Leader 3 → Replicas 3,1,2 → ISR 3,1,2
```

That gives you a quick picture of the topic's **partition and replication state**.

---

# 18. Commands to Remember

For **Kafka Topics CLI**, keep these in your notes:

```bash
# List
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3

# Describe
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic orders

# Increase partitions
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --alter \
  --topic orders \
  --partitions 6

# Delete
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --delete \
  --topic orders
```

### Core mental model

```text
                 kafka-topics.sh
                       │
                       ▼
                --bootstrap-server
                       │
                       ▼
                   Broker
                       │
                       ▼
                    Topic
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
         P0           P1           P2
          │            │            │
       Leader       Leader       Leader
          │            │            │
       Replicas      Replicas      Replicas
          │            │            │
         ISR          ISR          ISR
```

**We'll keep the next topics at this same level and won't jump ahead.**
