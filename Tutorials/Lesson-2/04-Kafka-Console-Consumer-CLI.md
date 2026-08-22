# Kafka Console Consumer CLI

`kafka-console-consumer.sh` is Kafka's built-in CLI tool used to **read/consume records from a Kafka topic**.

For your current sequence, keep the mental model simple:

```text
Kafka Topic
    ↓
Partition
    ↓
Kafka Consumer
    ↓
Console
```

---

## 1. What is `kafka-console-consumer.sh`?

It is a **command-line Kafka consumer** mainly used for:

* Testing whether messages are reaching a topic
* Reading records from a topic
* Troubleshooting
* Verifying producer → Kafka flow
* Learning consumer behavior

Basic syntax:

```bash
kafka-console-consumer.sh \
  --bootstrap-server <broker>:9092 \
  --topic <topic-name>
```

Example:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

---

# 2. Architecture

```text
                 Ubuntu / Admin Machine
                         │
                         ▼
             kafka-console-consumer.sh
                         │
                         │ Consume request
                         ▼
                  Kafka Broker
                         │
                         ▼
                    Topic: orders
                         │
                ┌────────┼────────┐
                ▼        ▼        ▼
               P0       P1       P2
                │        │        │
                └────────┼────────┘
                         ▼
                  Consumer CLI
                         │
                         ▼
                    Terminal
```

The CLI is acting as a **Kafka Consumer**.

---

# 3. Easy Analogy

Think of Kafka as a newspaper distribution system.

```text
Newspaper warehouse
       │
       ▼
   Newspaper
       │
       ▼
     Reader
```

Kafka:

```text
Kafka Topic
    │
    ▼
Kafka Broker
    │
    ▼
Consumer
    │
    ▼
Your Terminal
```

The producer **puts** records into Kafka.

The consumer **reads** those records.

---

# 4. Basic Consumer

Suppose you already have:

```text
orders
```

Run:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

The consumer will wait for new records.

If another terminal produces:

```text
Order-1001
```

the consumer displays:

```text
Order-1001
```

---

# 5. Producer + Consumer Together

### Terminal 1 — Consumer

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

### Terminal 2 — Producer

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Type:

```text
Order-1001
Order-1002
Order-1003
```

Consumer receives:

```text
Order-1001
Order-1002
Order-1003
```

Architecture:

```text
                    Kafka Cluster

Producer CLI
     │
     │ produce
     ▼
  Broker
     │
     ▼
  orders
     │
     ├── P0
     ├── P1
     └── P2
     │
     ▼
Consumer CLI
     │
     ▼
  Terminal
```

---

# 6. Important: Consumer Reads From Partitions

A consumer doesn't read some abstract "topic storage."

A topic contains partitions:

```text
orders
│
├── P0
├── P1
└── P2
```

The consumer reads records from those partitions.

For example:

```text
P0:
Offset 0 → Order-1001
Offset 1 → Order-1004

P1:
Offset 0 → Order-1002

P2:
Offset 0 → Order-1003
```

The consumer reads records from the partitions assigned to it.

---

# 7. `--from-beginning`

This is one of the most important options.

Without it:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

the consumer normally starts from the appropriate current position rather than simply replaying the entire topic history.

If you want to read existing records from the beginning:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning
```

For example, suppose the topic already contains:

```text
Order-1001
Order-1002
Order-1003
```

Running:

```bash
--from-beginning
```

allows you to read those existing records.

---

# 8. Analogy for `--from-beginning`

Imagine a YouTube video.

### Normal behavior

You join the live stream:

```text
───────●──────────────>
       ↑
    You joined
```

You see new content from your current position.

### `--from-beginning`

You say:

> "Start the video from the beginning."

```text
●─────────────────────>
↑
Start here
```

Kafka:

```text
--from-beginning
       ↓
Read available records from the beginning
```

---

# 9. Consumer and Offset

You've already learned **offsets**, so this is where they become practical.

Suppose:

```text
orders-P0

Offset 0 → Order-1001
Offset 1 → Order-1002
Offset 2 → Order-1003
Offset 3 → Order-1004
```

The consumer reads:

```text
Offset 0
   ↓
Offset 1
   ↓
Offset 2
   ↓
Offset 3
```

The offset identifies the record's position within that partition.

---

# 10. Consumer Doesn't Delete the Record

This is very important.

When the consumer reads:

```text
Offset 2 → Order-1003
```

Kafka does **not** normally remove:

```text
Order-1003
```

from the partition just because it was consumed.

The record remains according to Kafka's retention rules.

Think:

```text
Kafka Log
│
├── Record 0
├── Record 1  ← consumer read
├── Record 2
└── Record 3
```

Reading ≠ deleting.

---

# 11. Consumer With a Consumer Group

You already learned Consumer Groups, so here's the CLI connection.

You can specify:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service
```

Architecture:

```text
                  Consumer Group
                 "order-service"
                       │
              ┌────────┼────────┐
              ▼        ▼        ▼
           Consumer  Consumer  Consumer
              │        │        │
              ▼        ▼        ▼
             P0       P1       P2
```

The group allows Kafka to coordinate which consumer reads which partition.

---

# 12. Without `--group`

Example:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

This is useful for simple testing.

You're essentially saying:

> "I want to consume records from this topic."

For repeatable consumer-group behavior, use:

```bash
--group order-service
```

---

# 13. Consumer With a Key

If the producer sent:

```text
customer-101:Order-1001
customer-102:Order-1002
```

the consumer can be configured to display keys as well.

For example:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property print.key=true \
  --property key.separator=:
```

You may see:

```text
customer-101:Order-1001
customer-102:Order-1002
```

Without `print.key=true`, you generally see the value rather than the key.

---

# 14. Read a Limited Number of Records

You can use:

```bash
--max-messages 5
```

Example:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 5
```

Meaning:

> Read up to 5 messages and then exit.

This is very useful during troubleshooting.

Instead of:

```text
Consumer runs forever
```

you can do:

```text
Read 5 records
     ↓
Exit
```

---

# 15. Read From Beginning + Limited Messages

A very useful testing command:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 10
```

Meaning:

```text
orders
   ↓
Start from beginning
   ↓
Read up to 10 records
   ↓
Exit
```

---

# 16. Real Platform Engineer Example

Application team says:

> "Orders are not reaching our Kafka consumer."

You can test the topic directly.

### Step 1 — Check topic

```bash
kafka-topics.sh \
  --bootstrap-server kafka-01:9092 \
  --describe \
  --topic orders
```

### Step 2 — Consume existing records

```bash
kafka-console-consumer.sh \
  --bootstrap-server kafka-01:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 10
```

If you see:

```text
Order-1001
Order-1002
Order-1003
```

you know the topic contains readable records.

Then you can investigate the application consumer separately.

---

# 17. Consumer CLI + Partition Architecture

Suppose:

```text
orders
│
├── P0
│    ├── 0 → Order-1
│    ├── 1 → Order-4
│    └── 2 → Order-7
│
├── P1
│    ├── 0 → Order-2
│    └── 1 → Order-5
│
└── P2
     ├── 0 → Order-3
     └── 1 → Order-6
```

Consumer:

```text
                Consumer
                   │
          ┌────────┼────────┐
          ▼        ▼        ▼
         P0       P1       P2
          │        │        │
          ▼        ▼        ▼
       Records  Records  Records
```

The consumer reads according to its partition assignments.

---

# 18. Consumer CLI and Ordering

You've already learned:

> **Ordering is guaranteed within a partition.**

Therefore:

```text
P0

Offset 0 → A
Offset 1 → B
Offset 2 → C
Offset 3 → D
```

Consumer reads:

```text
A → B → C → D
```

That ordering exists **within P0**.

Don't interpret the topic as one globally ordered sequence when it has multiple partitions.

---

# 19. Useful Consumer Options

For now, focus on these:

| Option               | Purpose                                |
| -------------------- | -------------------------------------- |
| `--bootstrap-server` | Kafka broker endpoint                  |
| `--topic`            | Topic to consume                       |
| `--from-beginning`   | Start from available beginning         |
| `--group`            | Consumer group                         |
| `--max-messages`     | Stop after specified number of records |
| `--property`         | Configure console consumer output      |

---

# 20. Basic Commands to Keep in Your Notes

### Consume new/current records

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

### Read from beginning

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning
```

### Consumer group

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --group order-service
```

### Read only 10 records

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --max-messages 10
```

### Display keys

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property print.key=true \
  --property key.separator=:
```

---

# 21. Producer vs Consumer CLI

Keep this simple:

| Producer                         | Consumer                       |
| -------------------------------- | ------------------------------ |
| `kafka-console-producer.sh`      | `kafka-console-consumer.sh`    |
| Writes records                   | Reads records                  |
| Producer → Kafka                 | Kafka → Consumer               |
| Targets topic                    | Reads topic                    |
| Appends records                  | Reads records                  |
| Does not remove existing records | Reading doesn't delete records |

Architecture:

```text
       PRODUCER                         CONSUMER
          │                                ▲
          │                                │
          ▼                                │
     ┌──────────┐                     ┌──────────┐
     │  Broker  │────────────────────►│ Consumer │
     └────┬─────┘                     └──────────┘
          │
          ▼
        Topic
          │
     ┌────┴────┐
     ▼         ▼
    P0        P1
```

---

# 22. The Complete Mental Model

```text
                  kafka-console-consumer.sh
                              │
                              │
                              ▼
                      --bootstrap-server
                              │
                              ▼
                         Kafka Broker
                              │
                              ▼
                          Topic: orders
                              │
                    ┌─────────┼─────────┐
                    ▼         ▼         ▼
                   P0        P1        P2
                    │         │         │
                    ▼         ▼         ▼
                 Offset    Offset    Offset
                    │         │         │
                    └─────────┼─────────┘
                              ▼
                           Consumer
                              │
                              ▼
                           Terminal
```

### The key things to remember:

* **`kafka-console-consumer.sh` = Kafka CLI consumer**
* It connects using **`--bootstrap-server`**
* It consumes records from a **topic**
* Records physically exist in **partitions**
* `--from-beginning` lets you read existing records from the beginning
* `--group` makes the consumer part of a consumer group
* `--max-messages` is useful for testing
* **Reading a record does not delete it**
* Ordering is maintained **within each partition**
* It is primarily a **testing/troubleshooting tool**, not normally how production applications consume Kafka records.
