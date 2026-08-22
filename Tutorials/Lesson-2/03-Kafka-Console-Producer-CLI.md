# Kafka Console Producer CLI

`kafka-console-producer.sh` is Kafka's built-in CLI tool for **sending records/messages to a Kafka topic**.

For your current learning sequence, focus on:

```text
Producer CLI
   ↓
Kafka Broker
   ↓
Topic
   ↓
Partition
```

---

## 1. What is `kafka-console-producer.sh`?

It is a **command-line Kafka producer** mainly useful for:

* Testing Kafka
* Sending sample messages
* Troubleshooting
* Validating topic connectivity
* Learning producer behavior

Basic syntax:

```bash
kafka-console-producer.sh \
  --bootstrap-server <broker>:9092 \
  --topic <topic-name>
```

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

---

# 2. Architecture

```text
                 Ubuntu / Admin Machine
                         │
                         ▼
             kafka-console-producer.sh
                         │
                         │ Kafka request
                         ▼
                  Kafka Broker
                         │
                         ▼
                      Topic
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
             P0         P1         P2
```

The CLI is acting as a **Kafka Producer**.

---

# 3. Easy Analogy

Imagine a restaurant:

```text
You
 │
 │ Place order
 ▼
Waiter
 │
 ▼
Kitchen
```

Kafka analogy:

```text
You type message
       │
       ▼
Console Producer
       │
       ▼
Kafka Broker
       │
       ▼
Topic / Partition
```

The producer is basically saying:

> "Kafka, I have a new record. Please append it to this topic."

---

# 4. Basic Example

First make sure the topic exists:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Suppose you have:

```text
orders
```

Start producer:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

You'll get an interactive prompt.

Now type:

```text
Order-1001
```

Press Enter.

Then:

```text
Order-1002
```

Press Enter.

Then:

```text
Order-1003
```

Each line is treated as a **separate record**.

---

# 5. What Happens When You Press Enter?

Suppose you type:

```text
Order-1001
```

The conceptual flow is:

```text
You type:

Order-1001
     │
     ▼
Console Producer
     │
     ▼
Kafka Producer Client
     │
     ▼
Kafka Broker
     │
     ▼
Topic: orders
     │
     ▼
Partition
     │
     ▼
Kafka Log
```

---

# 6. Important — One Line = One Record

If you type:

```text
Order-1001
```

and press Enter:

```text
Record 1
```

Then:

```text
Order-1002
```

becomes:

```text
Record 2
```

So:

```text
Order-1001 → Record
Order-1002 → Record
Order-1003 → Record
```

---

# 7. Where Does the Message Go?

Suppose:

```text
orders
│
├── P0
├── P1
└── P2
```

When you send:

```text
Order-1001
```

the producer needs to determine which partition receives the record.

Conceptually:

```text
Producer
    │
    ▼
orders
    │
    ├── P0
    ├── P1
    └── P2
```

The exact partition selection depends on the producer's configuration and whether a key is supplied.

---

# 8. Producer With No Key

The simple command:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

doesn't explicitly provide a key.

You type:

```text
Order-1001
Order-1002
Order-1003
```

The producer determines partition placement according to its producer/partitioner behavior.

For your current learning, the important point is:

> **The producer does not simply write to "the topic." A record is ultimately appended to one specific partition of that topic.**

---

# 9. Producer With a Key

You can also send records with keys.

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

Now type:

```text
customer-101:Order-1001
customer-102:Order-1002
customer-101:Order-1003
```

The producer interprets:

```text
Key              Value

customer-101  →  Order-1001
customer-102  →  Order-1002
customer-101  →  Order-1003
```

The key is important because it can influence which partition receives the record.

---

# 10. Why Would We Use Keys?

Real-world example:

You have:

```text
Customer 101
Customer 102
Customer 103
```

You want records for the same customer to consistently go to the same partition.

Conceptually:

```text
customer-101
      │
      ▼
     P0

customer-102
      │
      ▼
     P1

customer-103
      │
      ▼
     P2
```

This can help maintain ordering **for records sharing the same key**.

You've already learned partition ordering, so connect it like this:

```text
Same key
   ↓
Same partition
   ↓
Partition ordering
```

---

# 11. Producer and `--bootstrap-server`

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic orders
```

Remember what you learned previously:

```text
Console Producer
       │
       ▼
kafka-1:9092
       │
       ▼
Metadata
       │
       ▼
Find relevant partition/broker
       │
       ▼
Send record
```

`kafka-1:9092` is the **bootstrap server**, not necessarily the broker that ultimately handles the partition.

---

# 12. Producer Architecture With Multiple Brokers

Suppose:

```text
              Kafka Cluster

        ┌────────┬────────┬────────┐
        │        │        │        │
       B1       B2       B3       B4
```

Topic:

```text
orders
│
├── P0 → B2 Leader
├── P1 → B3 Leader
└── P2 → B1 Leader
```

Producer:

```text
                 Producer CLI
                      │
                      ▼
                     B1
                 bootstrap
                      │
                      ▼
                  Metadata
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       P0 → B2     P1 → B3     P2 → B1
```

The producer discovers where the relevant partition leader is and sends the record accordingly.

---

# 13. Producer Does Not Write Directly to All Replicas

Suppose:

```text
P0

Leader → B1
Replica → B2
Replica → B3
```

The producer sends the record to the **partition leader**.

```text
Producer
    │
    ▼
   B1
 Leader
    │
    ├────────► B2
    │
    └────────► B3
```

The followers/replicas are handled by Kafka's replication mechanism.

For your current topic, remember:

> **Producer → Partition Leader**

---

# 14. Producer + Append Model

You've specifically emphasized this earlier, so it should be included here.

Kafka records are **appended to the partition log**.

Conceptually:

```text
Partition 0

Offset
  0     Order-1001
  1     Order-1002
  2     Order-1003
  3     Order-1004
```

The producer adds:

```text
Order-1005
```

Kafka appends it:

```text
  0     Order-1001
  1     Order-1002
  2     Order-1003
  3     Order-1004
  4     Order-1005
```

### Important:

Kafka's normal record model is:

> **APPEND — not UPDATE or DELETE of an existing record.**

The producer doesn't say:

```text
"Update offset 2."
```

It produces another record.

---

# 15. Producer CLI Example — Real Case

Imagine an e-commerce system.

Topic:

```text
orders
```

Producer sends:

```text
order-1001
order-1002
order-1003
```

Architecture:

```text
E-Commerce Application
        │
        ▼
   Kafka Producer
        │
        ▼
      orders
        │
        ├── P0
        ├── P1
        └── P2
```

The console producer is simply a **testing version** of what a real application producer would do.

---

# 16. Console Producer vs Real Application Producer

### Console Producer

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Human types:

```text
Order-1001
```

### Real application

For example:

```text
Java Application
      │
      ▼
Kafka Producer API
      │
      ▼
Kafka Broker
```

The important distinction:

> **`kafka-console-producer.sh` is primarily a testing/learning tool, not normally how production applications publish events.**

---

# 17. Useful CLI Options

For now, focus on these:

| Option                | Purpose                             |
| --------------------- | ----------------------------------- |
| `--bootstrap-server`  | Kafka broker endpoint               |
| `--topic`             | Topic to produce to                 |
| `--property`          | Configure console producer behavior |
| `--producer-property` | Pass producer configuration         |

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

---

# 18. Testing Producer + Consumer Together

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
```

Consumer receives:

```text
Order-1001
```

Architecture:

```text
                Producer CLI
                     │
                     ▼
                  Broker
                     │
                     ▼
                  orders
                     │
                     ▼
                Consumer CLI
```

This is one of the best first Kafka CLI labs.

---

# 19. Senior Platform Engineer Troubleshooting Use

Suppose an application team says:

> "Kafka topic `orders` isn't accepting messages."

You can quickly test the topic yourself:

```bash
kafka-console-producer.sh \
  --bootstrap-server kafka-01:9092 \
  --topic orders
```

Send:

```text
test-message
```

If it succeeds, you have verified that:

```text
Broker reachable
       ↓
Topic accessible
       ↓
Producer request accepted
       ↓
Record produced
```

Then you can investigate the application separately.

---

# 20. Important Things to Remember

### 1. Producer is a client

```text
Console Producer
      ↓
Kafka Client
```

### 2. It connects using bootstrap server

```text
--bootstrap-server kafka-1:9092
```

### 3. It produces to a topic

```text
--topic orders
```

### 4. Records ultimately go to partitions

```text
Topic
 ├── P0
 ├── P1
 └── P2
```

### 5. Producer sends to the appropriate partition leader

```text
Producer → Partition Leader
```

### 6. Kafka appends records

```text
Record 1
Record 2
Record 3
Record 4
```

not:

```text
UPDATE Record 2
DELETE Record 3
```

---

# 21. Commands to Keep in Your Notes

### Basic producer

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

### Producer with keys

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

Then:

```text
customer-101:Order-1001
customer-102:Order-1002
customer-101:Order-1003
```

---

# Final Mental Model

```text
                 kafka-console-producer.sh
                           │
                           │
                           ▼
                    --bootstrap-server
                           │
                           ▼
                     Kafka Broker
                           │
                      Metadata
                           │
                           ▼
                      Topic: orders
                           │
                ┌──────────┼──────────┐
                ▼          ▼          ▼
               P0         P1         P2
                │
                ▼
          Partition Leader
                │
                ▼
          Append Record
                │
                ▼
          Offset assigned
```

### One-line interview answer:

> **`kafka-console-producer.sh` is Kafka's command-line producer used mainly for testing and troubleshooting. It connects to Kafka through `--bootstrap-server`, targets a topic, and sends records that are ultimately appended to a specific partition, with the appropriate partition leader handling the write.**
