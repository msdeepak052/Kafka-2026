# Kafka Console Producer CLI — Complete Notes

## 1. What is `kafka-console-producer.sh`?

`kafka-console-producer.sh` is Kafka's built-in CLI tool for **producing records/messages to a Kafka topic**.

It is mainly useful for:

* Testing Kafka
* Sending sample records
* Troubleshooting
* Validating topic connectivity
* Understanding producer behavior
* Testing producer configurations

Basic syntax:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Architecture:

```text
User
 │
 │ type message
 ▼
kafka-console-producer.sh
 │
 ▼
Kafka Producer Client
 │
 ▼
Kafka Broker
 │
 ▼
Topic
 │
 ├── Partition 0
 ├── Partition 1
 └── Partition 2
```

---

# 2. Easy Analogy

Imagine a courier system.

```text
You
 │
 │ Package
 ▼
Courier Service
 │
 ▼
Distribution Center
 │
 ▼
Specific Delivery Lane
```

Kafka:

```text
You type record
      │
      ▼
Console Producer
      │
      ▼
Kafka Broker
      │
      ▼
Topic
      │
      ▼
Specific Partition
```

The producer is essentially saying:

> "Kafka, I have a new record. Please append it to this topic."

---

# 3. Basic Producer

First check your topics:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Suppose:

```text
orders
payments
```

Start producer:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Now type:

```text
Order-1001
Order-1002
Order-1003
```

Each line becomes a separate Kafka record. This is also how Kafka's official CLI quickstart demonstrates the console producer. ([Apache Kafka][1])

---

# 4. What Happens When You Press Enter?

You type:

```text
Order-1001
```

Conceptually:

```text
Order-1001
     │
     ▼
Console Producer
     │
     ▼
Kafka Producer Client
     │
     ▼
Metadata
     │
     ▼
Find Partition Leader
     │
     ▼
Send Produce Request
     │
     ▼
Kafka Partition
     │
     ▼
Append Record
     │
     ▼
Offset assigned
```

---

# 5. One Line = One Record

If you enter:

```text
Order-1001
Order-1002
Order-1003
```

Kafka receives:

```text
Record 1 → Order-1001
Record 2 → Order-1002
Record 3 → Order-1003
```

The producer doesn't create one giant message containing all three lines.

---

# 6. Producer Does Not Simply Write to "The Topic"

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

the producer ultimately has to choose a specific partition.

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

The partition selection depends on things such as:

* Whether a key is provided
* The partitioner
* Explicit partition selection, if used

Kafka's current producer documentation describes the default partitioning behavior and key-based partition selection. ([Apache Kafka][2])

---

# 7. Producer With No Key

Basic command:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Send:

```text
Order-1001
Order-1002
Order-1003
```

There is no explicit key.

Conceptually:

```text
Record
  │
  ▼
Partitioner
  │
  ▼
Choose partition
```

---

# 8. Producer With a Key

You can configure the console producer to parse a key from each input line:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

Now:

```text
customer-101:Order-1001
customer-102:Order-1002
customer-101:Order-1003
```

means:

```text
Key              Value

customer-101  →  Order-1001
customer-102  →  Order-1002
customer-101  →  Order-1003
```

---

# 9. Why Use a Key?

Suppose:

```text
customer-101
```

generates:

```text
Order-1
Order-2
Order-3
```

You may want those records to consistently go to the same partition.

Conceptually:

```text
customer-101
      │
      ▼
    Hash
      │
      ▼
     P0

customer-102
      │
      ▼
    Hash
      │
      ▼
     P1
```

Therefore:

```text
Same key
   ↓
Same partition
   ↓
Partition ordering
```

This is particularly useful when ordering matters for a particular entity such as:

```text
customer_id
order_id
truck_id
account_id
```

---

# 10. Bootstrap Server

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic orders
```

The bootstrap server is used for the **initial connection and metadata discovery**.

It does not mean:

> "Every message will always be sent to kafka-1."

Conceptually:

```text
Producer
   │
   ▼
kafka-1:9092
   │
   ▼
Metadata
   │
   ▼
Find partition leader
   │
   ▼
Send request to appropriate broker
```

Kafka recommends specifying more than one bootstrap server for resilience, even though the list does not need to contain every broker. ([Apache Kafka][2])

Example:

```bash
--bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092
```

---

# 11. Producer + Multiple Brokers

Suppose:

```text
Kafka Cluster

B1
B2
B3
```

Topic:

```text
orders

P0 → Leader B2
P1 → Leader B3
P2 → Leader B1
```

Producer:

```text
                Producer
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

The producer discovers where the relevant partition leader is and sends the produce request there.

---

# 12. Producer → Partition Leader

Suppose:

```text
P0

Leader   → B1
Follower → B2
Follower → B3
```

The producer sends the produce request to the partition leader:

```text
Producer
    │
    ▼
   B1
 Leader
    │
    ├──────► B2
    │
    └──────► B3
```

Kafka handles replication between the leader and replicas.

Remember:

```text
Producer
    ↓
Partition Leader
    ↓
Replication
```

---

# 13. Kafka APPENDS Records

You've already established this concept, and it is extremely important.

Suppose:

```text
orders-P0

Offset 0 → Order-1001
Offset 1 → Order-1002
Offset 2 → Order-1003
Offset 3 → Order-1004
```

Producer sends:

```text
Order-1005
```

Kafka appends:

```text
Offset 0 → Order-1001
Offset 1 → Order-1002
Offset 2 → Order-1003
Offset 3 → Order-1004
Offset 4 → Order-1005
```

### Kafka's normal record model:

> **APPEND — not UPDATE or DELETE of an existing record.**

The producer doesn't say:

```text
Update offset 2
```

Instead, it produces another record.

---

# 14. Producer Properties

Now we get to the important addition you asked for.

The console producer allows you to pass **actual Kafka producer configuration** using:

```bash
--producer-property
```

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --producer-property acks=all
```

This means:

```text
Console Producer
      │
      ▼
Kafka Producer Configuration
      │
      └── acks=all
```

Your previous document already identified `--producer-property` as the mechanism for passing producer configuration. 

---

# 15. `acks`

`acks` controls how much acknowledgement the producer requires before considering the produce request successful.

There are three important values:

```text
acks=0
acks=1
acks=all
```

Current Kafka 4.x defaults `acks` to `all`. ([Apache Kafka][2])

---

## `acks=0`

```bash
--producer-property acks=0
```

Producer does not wait for a broker acknowledgement.

```text
Producer
   │
   │ record
   ▼
Broker
   │
   X
No ACK waited for
```

### Advantage

* Very low producer-side latency

### Disadvantage

* Weakest durability
* Producer cannot reliably know whether the broker received the record
* Retries don't help because the producer doesn't wait for acknowledgement

So:

```text
acks=0
↓
Fast
↓
Low durability
```

Kafka documents that with `acks=0`, the returned offset is `-1` and retries don't take effect in the normal way. ([Apache Kafka][2])

---

# 16. `acks=1`

```bash
--producer-property acks=1
```

The leader writes the record to its local log and acknowledges the producer without waiting for all followers.

```text
Producer
   │
   ▼
Leader
   │
   ├── local write ✅
   │
   ├── ACK → Producer
   │
   └── Followers may still be replicating
```

Potential problem:

```text
Leader writes
    ↓
ACK
    ↓
Leader crashes
    ↓
Followers didn't replicate yet
    ↓
Potential data loss
```

So:

```text
acks=1
↓
Better than acks=0
↓
Less durable than acks=all
```

---

# 17. `acks=all`

```bash
--producer-property acks=all
```

This is the strongest acknowledgement setting.

The leader waits for acknowledgement from the **in-sync replicas (ISR)** required for the write.

```text
Producer
    │
    ▼
 Leader B1
    │
    ├──────► B2
    │         │
    │        ACK
    │
    └──────► B3
              │
             ACK
    │
    ▼
Producer receives success
```

Kafka describes `acks=all` as the strongest available producer acknowledgement guarantee. It is equivalent to:

```text
acks=-1
```

([Apache Kafka][2])

### Mental model:

```text
acks=0
   ↓
Don't wait

acks=1
   ↓
Leader confirms

acks=all
   ↓
ISR confirms
```

---

# 18. `acks=all` + `min.insync.replicas`

This is an **extremely important Platform Engineer concept**.

Suppose:

```text
Replication Factor = 3
```

```text
P0

B1 → Leader
B2 → ISR
B3 → ISR
```

Configure:

```text
min.insync.replicas=2
```

Producer:

```text
acks=all
```

Now Kafka requires at least **2 in-sync replicas** for the write to succeed.

```text
RF = 3

B1 ─┐
B2 ─┼── at least 2 ISR required
B3 ─┘
```

If:

```text
ISR = 3
```

write succeeds.

If:

```text
ISR = 2
```

write succeeds.

If:

```text
ISR = 1
```

write fails rather than accepting a write with insufficient in-sync replicas.

Kafka explicitly documents this `acks=all` + `min.insync.replicas` combination as a way to enforce stronger durability. ([Apache Kafka][3])

### Important distinction

```text
replication.factor = 3
```

means:

> We have 3 replicas.

While:

```text
min.insync.replicas = 2
```

means:

> At least 2 replicas must be in sync for an `acks=all` write to succeed.

---

# 19. Console Producer With `acks=all`

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --producer-property acks=all
```

For a production-style durability demonstration:

```bash
kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic orders \
  --producer-property acks=all
```

---

# 20. `enable.idempotence`

Another very important producer property:

```text
enable.idempotence=true
```

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --producer-property enable.idempotence=true
```

What problem does it solve?

Potential duplicate:

```text
Producer
   │
   ▼
Broker writes record
   │
   X
ACK lost
   │
   ▼
Producer retries
   │
   ▼
Same record could be written again
```

Idempotent producer behavior is designed to prevent such producer retries from creating duplicate copies.

Kafka 4.0 documents idempotence as enabled by default when there are no conflicting configurations. ([Apache Kafka][4])

---

# 21. Idempotence Requirements

When explicitly enabling:

```text
enable.idempotence=true
```

Kafka requires compatible settings including:

```text
acks=all
retries > 0
max.in.flight.requests.per.connection <= 5
```

([Apache Kafka][4])

So the mental model is:

```text
enable.idempotence=true
        │
        ├── acks=all
        ├── retries > 0
        └── max.in.flight <= 5
```

### Important

Don't confuse:

```text
idempotence
```

with:

```text
transactions
```

They are related reliability mechanisms, but not the same thing. We don't need to jump into transactions here.

---

# 22. `retries`

Producer can retry transient failures.

Example:

```bash
--producer-property retries=5
```

Conceptually:

```text
Producer
   │
   ▼
Broker
   │
   X temporary failure
   │
   ▼
Retry
   │
   ▼
Broker
   │
   ▼
Success
```

Kafka's current documentation recommends generally leaving `retries` unset and using `delivery.timeout.ms` to control overall retry behavior. ([Apache Kafka][2])

For learning:

```text
retries
   ↓
How many retry attempts can be made
```

But don't think of `retries` alone as a complete reliability strategy.

---

# 23. `delivery.timeout.ms`

This is an important producer property.

```bash
--producer-property delivery.timeout.ms=120000
```

It controls the upper bound on the time Kafka's producer will take to report success/failure for a record after `send()` returns.

Conceptually:

```text
send()
  │
  ▼
Try
  │
  ├── success → done
  │
  ├── retry
  │
  ├── retry
  │
  └── timeout → failure
```

Current Kafka 4.0 default:

```text
delivery.timeout.ms=120000
```

= **2 minutes**. ([Apache Kafka][2])

---

# 24. `request.timeout.ms`

This controls how long the producer waits for a response to a request.

Example:

```bash
--producer-property request.timeout.ms=30000
```

Meaning:

```text
Producer
   │
   ▼
Request
   │
   ▼
Wait for broker response
   │
   ├── response → success
   │
   └── timeout → failure/retry behavior
```

Don't confuse:

```text
request.timeout.ms
```

with:

```text
delivery.timeout.ms
```

### Simple distinction:

```text
request.timeout.ms
        ↓
One request's waiting time

delivery.timeout.ms
        ↓
Overall delivery deadline
```

Kafka specifies that `delivery.timeout.ms` should be greater than or equal to the relevant request timeout plus linger time. ([Apache Kafka][2])

---

# 25. Batching — `batch.size`

Kafka producers don't necessarily send every record as an independent network request.

They can batch records.

```text
Record 1 ─┐
Record 2 ─┼──► Batch ──► Broker
Record 3 ─┘
```

Configure:

```bash
--producer-property batch.size=32768
```

The value is in bytes.

Current Kafka 4.0 default:

```text
batch.size=16384
```

= 16 KiB. ([Apache Kafka][2])

### Why batching?

```text
Without batching:

Record → Request
Record → Request
Record → Request
```

versus:

```text
Record ─┐
Record ─┼──► Batch → Request
Record ─┘
```

Batching can improve throughput and compression efficiency.

---

# 26. `linger.ms`

`linger.ms` controls how long the producer can wait for more records to arrive so that it can create a larger batch.

Example:

```bash
--producer-property linger.ms=10
```

Conceptually:

```text
Record 1 arrives
      │
      ▼
Wait briefly for more records
      │
      ├── Record 2
      ├── Record 3
      └── Record 4
      │
      ▼
Send batch
```

In Kafka 4.0, the default is **5 ms**, changed from 0 in Kafka 4.0. ([Apache Kafka][2])

So don't use old tutorials that claim:

```text
linger.ms default = 0
```

for Kafka 4.x.

---

# 27. `batch.size` vs `linger.ms`

Very important distinction:

```text
batch.size
    ↓
How large the batch can become

linger.ms
    ↓
How long producer waits for more records
```

Example:

```text
batch.size = 32 KB
linger.ms  = 10 ms
```

The producer can effectively send when:

```text
Batch becomes full
        OR
linger time expires
```

Conceptually:

```text
             ┌── batch full ──► SEND
Records ─────┤
             └── 10ms expires ─► SEND
```

---

# 28. Compression

Producer can compress batches.

Example:

```bash
--producer-property compression.type=zstd
```

Current Kafka supports:

```text
none
gzip
snappy
lz4
zstd
```

([Apache Kafka][2])

Architecture:

```text
Records
   │
   ▼
Batch
   │
   ▼
Compression
   │
   ▼
Network
   │
   ▼
Broker
```

Benefits:

* Lower network traffic
* Potentially lower storage usage
* Better throughput in many workloads

Tradeoff:

* CPU is required for compression/decompression.

---

# 29. Example With Compression

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --producer-property compression.type=zstd
```

For a test:

```text
Order-1001
Order-1002
Order-1003
```

The producer batches/compresses data before sending according to its configuration.

---

# 30. `buffer.memory`

Producer maintains memory for records waiting to be sent.

Example:

```bash
--producer-property buffer.memory=67108864
```

That's:

```text
64 MiB
```

Conceptually:

```text
Application / CLI
       │
       ▼
Producer Buffer
       │
       ├── Batch
       ├── Batch
       └── Batch
       │
       ▼
Broker
```

Current Kafka 4.0 default:

```text
buffer.memory = 33554432
```

= 32 MiB. ([Apache Kafka][2])

---

# 31. `max.request.size`

Controls the maximum size of a producer request.

Example:

```bash
--producer-property max.request.size=1048576
```

That's:

```text
1 MiB
```

Conceptually:

```text
Producer
   │
   ▼
Request
   │
   └── must respect max.request.size
```

Important:

> This is a **producer-side limit**. The broker also has its own limits.

Kafka's current documentation notes that the server has its own record-batch size limit, which may differ from the producer's setting. ([Apache Kafka][2])

---

# 32. `max.in.flight.requests.per.connection`

This controls how many unacknowledged requests the producer can have in flight on one connection.

Example:

```bash
--producer-property max.in.flight.requests.per.connection=5
```

Current Kafka 4.0 default:

```text
5
```

([Apache Kafka][2])

Important relationship:

```text
enable.idempotence=true
        ↓
max.in.flight <= 5
```

Kafka explicitly requires this compatibility for idempotence. ([Apache Kafka][4])

---

# 33. `client.id`

You can identify the producer client.

```bash
--producer-property client.id=orders-test-producer
```

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --producer-property client.id=orders-test-producer
```

This is useful operationally because the client ID can help identify the source of requests in Kafka-side monitoring/logging.

Kafka describes `client.id` as a logical identifier used to track the source of client requests. ([Apache Kafka][2])

---

# 34. Important Producer Properties — Cheat Sheet

| Property                                | What it controls                               |
| --------------------------------------- | ---------------------------------------------- |
| `acks`                                  | Required broker acknowledgement                |
| `enable.idempotence`                    | Prevent duplicate writes from producer retries |
| `retries`                               | Retry transient send failures                  |
| `delivery.timeout.ms`                   | Overall delivery deadline                      |
| `request.timeout.ms`                    | Wait time for an individual request response   |
| `batch.size`                            | Maximum batch size                             |
| `linger.ms`                             | Wait time to accumulate records into a batch   |
| `compression.type`                      | Record-batch compression                       |
| `buffer.memory`                         | Producer buffer memory                         |
| `max.request.size`                      | Maximum producer request size                  |
| `max.in.flight.requests.per.connection` | Unacknowledged requests per connection         |
| `client.id`                             | Logical producer identifier                    |

---

# 35. Acks Comparison

This should be memorized:

```text
                 Durability
                    ▲
                    │
acks=0       ───────┘ Lowest

acks=1       ───────── Medium

acks=all     ───────────── Highest
```

More practically:

```text
acks=0
Producer → Broker
No ACK wait


acks=1
Producer → Leader
             │
             └── Leader writes → ACK


acks=all
Producer → Leader
             │
             ├── ISR
             ├── ISR
             └── ISR
                  │
                  ▼
                ACK
```

---

# 36. Acks + Replication Example

Suppose:

```text
Replication Factor = 3

P0:
B1 → Leader
B2 → ISR
B3 → ISR
```

Producer:

```text
acks=all
```

Then:

```text
Producer
   │
   ▼
B1
 │
 ├──► B2
 │
 └──► B3
```

If enough ISR replicas acknowledge:

```text
SUCCESS
```

Now imagine:

```text
B3 fails
```

ISR:

```text
B1
B2
```

If:

```text
min.insync.replicas=2
```

the write can still succeed.

If ISR falls to:

```text
B1
```

then:

```text
acks=all
+
min.insync.replicas=2
```

causes the write to fail instead of accepting a write below the configured durability threshold. ([Apache Kafka][3])

---

# 37. Recommended Learning Example

For your lab, create:

```text
Topic: orders
Partitions: 3
Replication Factor: 3
```

Then use:

```bash
kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic orders \
  --producer-property acks=all \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=zstd \
  --producer-property linger.ms=5 \
  --producer-property batch.size=16384 \
  --producer-property client.id=orders-cli
```

Then send:

```text
Order-1001
Order-1002
Order-1003
```

This gives you a practical demonstration of:

```text
Producer
   │
   ├── acks=all
   ├── idempotence
   ├── batching
   ├── compression
   └── client identification
   │
   ▼
Partition Leader
   │
   ▼
ISR
```

---

# 38. Producer With Key + Properties

You can combine key parsing with producer properties:

```bash
kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092,kafka-2:9092,kafka-3:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=zstd \
  --producer-property client.id=orders-cli
```

Then:

```text
customer-101:Order-1001
customer-102:Order-1002
customer-101:Order-1003
```

Here:

```text
--property parse.key=true
        ↓
Console producer behavior

--property key.separator=:
        ↓
How CLI parses key/value

--producer-property acks=all
        ↓
Actual Kafka producer configuration

--producer-property enable.idempotence=true
        ↓
Actual Kafka producer configuration
```

### This distinction is important.

---

# 39. `--property` vs `--producer-property`

Don't confuse these.

### `--property`

Used for **console producer behavior**.

Example:

```bash
--property parse.key=true
--property key.separator=:
```

### `--producer-property`

Used for **Kafka Producer configuration**.

Example:

```bash
--producer-property acks=all
--producer-property compression.type=zstd
--producer-property linger.ms=5
```

Your original document already listed both options, but this distinction is worth explicitly remembering. 

---

# 40. Producer + Consumer Test

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
  --topic orders \
  --producer-property acks=all
```

Type:

```text
Order-1001
Order-1002
Order-1003
```

Consumer sees:

```text
Order-1001
Order-1002
Order-1003
```

Official Kafka's quickstart uses this same producer/consumer CLI pattern to demonstrate messages flowing through a topic. ([Apache Kafka][1])

---

# 41. Senior Platform Engineer Troubleshooting

Suppose an application team says:

> "Our producer can't publish to Kafka."

You can test:

```bash
kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic orders \
  --producer-property acks=all \
  --producer-property client.id=platform-test
```

Send:

```text
platform-test-message
```

If successful, you've verified a basic path:

```text
Client
  ↓
Bootstrap connection
  ↓
Metadata
  ↓
Topic
  ↓
Partition leader
  ↓
Produce request
  ↓
Acknowledgement
```

If you get an error, it gives you a useful starting point for troubleshooting.

---

# 42. Production Reliability Mental Model

For a production-oriented producer, think about these together:

```text
                 Producer Reliability
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
       acks=all       Idempotence      Retries
          │              │              │
          ▼              ▼              ▼
       Durability     Duplicates      Failures
                         │
                         ▼
                 delivery.timeout.ms
```

And on the Kafka side:

```text
Replication Factor
        +
min.insync.replicas
        +
acks=all
        ↓
Stronger durability
```

---

# 43. Important Current Kafka 4.x Note

Since you're working with a newer Kafka version, don't blindly follow old tutorials.

For Kafka 4.0:

```text
acks
   default = all

enable.idempotence
   default = true

linger.ms
   default = 5 ms
```

The `linger.ms` default changed from 0 to 5 ms in Kafka 4.0, and the current producer documentation lists `acks=all` and idempotence as defaults when there are no conflicting settings. ([Apache Kafka][2])

This is especially important because many older Kafka tutorials you'll find online show older defaults.

---

# 44. Complete Architecture

```text
                         USER
                          │
                          │
                    Type Record
                          │
                          ▼
              kafka-console-producer.sh
                          │
             ┌────────────┴────────────┐
             │                         │
        CLI properties          Producer properties
             │                         │
             │                   acks=all
             │                   retries
             │                   idempotence
             │                   batch.size
             │                   linger.ms
             │                   compression
             │                   timeouts
             │                         │
             └────────────┬────────────┘
                          ▼
                  Kafka Producer
                          │
                          ▼
                      Metadata
                          │
                          ▼
                  Partition Selection
                          │
                          ▼
                  Partition Leader
                          │
                          ▼
                     Replication
                          │
                 ┌────────┼────────┐
                 ▼        ▼        ▼
                ISR      ISR      ISR
                 │        │        │
                 └────────┼────────┘
                          ▼
                     ACK / Error
                          │
                          ▼
                       Producer
```

---

# 45. Final Command Cheat Sheet

### Basic

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

### With key

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

### With durability

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --producer-property acks=all
```

### With idempotence

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --producer-property acks=all \
  --producer-property enable.idempotence=true
```

### With compression + batching

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --producer-property acks=all \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=zstd \
  --producer-property linger.ms=5 \
  --producer-property batch.size=16384
```

### With key + reliability properties

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=: \
  --producer-property acks=all \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=zstd \
  --producer-property client.id=orders-cli
```

Then:

```text
customer-101:Order-1001
customer-102:Order-1002
customer-101:Order-1003
```

---

# 🔥 Final Mental Model

```text
                 Console Producer
                       │
                       ▼
                  Kafka Producer
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
       Key/Part.     Batching    Compression
          │            │            │
          └────────────┼────────────┘
                       ▼
                 Partition Leader
                       │
                       ▼
                    Replicas
                       │
                       ▼
                      ISR
                       │
                       ▼
                  ACK = all
                       │
                       ▼
                 Record Success
```

### The properties I want you to remember first:

```text
acks
enable.idempotence
retries
delivery.timeout.ms
request.timeout.ms
batch.size
linger.ms
compression.type
buffer.memory
max.request.size
max.in.flight.requests.per.connection
client.id
```

And the most important relationship for your **Senior Platform Engineer** perspective:

```text
                    acks=all
                       +
             min.insync.replicas
                       +
              replication.factor
                       ↓
              Producer Durability
```

**One-line interview answer:**

> **The Kafka console producer is a CLI wrapper around the Kafka producer client. It sends records to topic partitions through their leaders, and producer properties such as `acks`, idempotence, retries, batching, compression and timeouts control the durability, reliability, performance and delivery behavior of those records.**

[1]: https://kafka.apache.org/26/getting-started/quickstart/?utm_source=chatgpt.com "Quick Start | Apache Kafka"
[2]: https://kafka.apache.org/40/configuration/producer-configs/?utm_source=chatgpt.com "Producer Configs | Apache Kafka"
[3]: https://kafka.apache.org/40/configuration/topic-level-configs/?utm_source=chatgpt.com "Topic-Level Configs | Apache Kafka"
[4]: https://kafka.apache.org/40/javadoc/constant-values.html?utm_source=chatgpt.com "Constant Field Values (kafka 4.0.2 API)"
