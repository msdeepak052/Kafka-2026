# 13. Kafka Producers — How Producers Send Requests to Kafka

This is the natural next concept after understanding **Consumers and how they fetch records**.

We already know:

```text
Producer
    ↓
Kafka
    ↓
Topic
    ↓
Partitions
    ↓
Records
    ↓
Consumers
```

Now let's understand the **producer side** in detail:

> **How does a producer send a request to Kafka, how does Kafka decide where the record goes, and how does the record finally get stored in a partition?**

I'll stay within the concepts you've covered so far.

---

# 1. What is a Kafka Producer?

* A **Kafka Producer** is an application/client that sends records to Kafka.
* It sends records to a **Kafka topic**.
* The record ultimately gets stored in a **partition** of that topic.
* The producer is responsible for deciding **which partition** the record should go to, based on the partitioning mechanism.
* Kafka then assigns the record an **offset** when it is appended to the partition.

### Simple definition

> **Kafka Producer = An application that creates records and sends them to Kafka topics.**

---

# 2. Easy Analogy 📦

Think of an e-commerce warehouse.

You are a seller who wants to send packages to a warehouse.

```text
Seller
  ↓
Warehouse
  ↓
Department
  ↓
Shelf
```

In Kafka:

```text
Producer
  ↓
Kafka Broker
  ↓
Topic
  ↓
Partition
```

The producer says:

> "I have this order event. Please store it in the `orders` topic."

Kafka determines which partition should contain that record.

---

# 3. Producer Architecture

Suppose:

```text
Topic: orders
Partitions: 3
```

We can visualize:

```text
                         Producer
                            │
                            │ Record
                            ▼
                     Kafka Cluster
                            │
                    Topic: orders
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
             P0            P1            P2
              │             │             │
              ▼             ▼             ▼
           Records       Records       Records
```

The important flow is:

```text
Application
    ↓
Kafka Producer
    ↓
Kafka Broker
    ↓
Topic
    ↓
Partition
    ↓
Record
```

---

<img width="1844" height="1013" alt="image" src="https://github.com/user-attachments/assets/4b071343-36be-4964-b94d-151f1175ea74" />

<img width="1844" height="1013" alt="image" src="https://github.com/user-attachments/assets/c1aa4488-3915-48fe-8374-678c1b59979f" />

---
# 4. What Does a Producer Actually Send?

Suppose your application generates:

```text
Order ID = 101
Event = ORDER_CREATED
```

The producer creates a Kafka record conceptually containing:

```text
Key   → 101
Value → ORDER_CREATED
```

So:

```text
Producer
   │
   │ Record
   │
   ├── Key
   └── Value
        │
        ▼
      Kafka
```

For example:

```text
Key:
order-101

Value:
Order Created
```

---

# 5. Producer Sends a Request to Kafka

The producer doesn't simply "throw" the message at Kafka.

It communicates with Kafka using Kafka's **request/response protocol**.

Conceptually:

```text
Producer
    │
    │ Produce Request
    ▼
Kafka Broker
    │
    │ Process request
    ▼
Partition
    │
    │ Store records
    ▼
Kafka
```

Then Kafka can send a response back:

```text
Kafka Broker
    │
    │ Response
    ▼
Producer
```

So the basic communication is:

```text
Producer
    │
    │ REQUEST
    ▼
Kafka
    │
    │ RESPONSE
    ▼
Producer
```

This is similar to how we looked at consumers:

```text
Consumer
    │
    │ Request
    ▼
Kafka
    │
    │ Records
    ▼
Consumer
```

But the direction of the data is different:

```text
PRODUCER:

Producer ───────► Kafka
                  │
                  ▼
               Store


CONSUMER:

Consumer ───────► Kafka
                  │
                  ▼
               Fetch
```

---

# 6. Complete Producer Flow

Let's walk through the entire process.

Suppose the application wants to send:

```text
Order #1001 Created
```

### Step 1 — Application creates an event

```text
Application
    │
    ▼
Order #1001 Created
```

---

### Step 2 — Producer receives the record

```text
Application
    │
    ▼
Kafka Producer
```

The producer prepares the record.

```text
Key   → order-1001
Value → ORDER_CREATED
```

---

### Step 3 — Producer determines the topic

```text
Topic = orders
```

So:

```text
Producer
    │
    ├── Topic → orders
    ├── Key   → order-1001
    └── Value → ORDER_CREATED
```

---

### Step 4 — Producer determines the partition

Suppose the partitioning logic selects:

```text
P1
```

Then:

```text
orders
   │
   ├── P0
   ├── P1 ← Record goes here
   └── P2
```

---

### Step 5 — Producer sends a request

Conceptually:

```text
Producer
    │
    │ Produce Request
    │ Topic = orders
    │ Partition = P1
    │ Record = ORDER_CREATED
    ▼
Kafka Broker
```

---

### Step 6 — Kafka appends the record

Kafka stores it in P1:

```text
P1

Offset 0 → Previous Event
Offset 1 → Previous Event
Offset 2 → ORDER_CREATED
```

Kafka assigns the record its offset.

For example:

```text
ORDER_CREATED → Offset 2
```

---

# 7. Very Important Relationship

Now connect **Producer + Partition + Offset**:

```text
Producer
    │
    │ Record
    ▼
Partition
    │
    │ Kafka appends record
    ▼
Offset assigned
```

So:

> **The producer chooses/supplies the destination information, while Kafka assigns the record's offset when it is appended to the partition.**

---

# 8. How Does Producer Know Which Broker to Contact?

This is an important part of the request flow.

Suppose your Kafka cluster has:

```text
Broker 1
Broker 2
Broker 3
```

The producer needs to communicate with the Kafka cluster.

It is normally configured with one or more **bootstrap server addresses**.

Example:

```text
bootstrap.servers =
broker1:9092,
broker2:9092,
broker3:9092
```

Think of these as:

> **Initial addresses the producer can use to discover the Kafka cluster.**

---

# 9. Bootstrap Server Analogy 🗺️

Imagine you arrive at a huge office campus.

You know:

```text
Main Reception
```

but you don't initially know where:

```text
Payments Department
Inventory Department
HR Department
```

are located.

You go to reception:

```text
Producer
   │
   ▼
Bootstrap Broker
   │
   ▼
Kafka cluster information
```

The producer can then determine where it needs to send its requests.

### Important:

> **Bootstrap servers are used to initially connect to the Kafka cluster; they don't mean that every record must permanently go through that particular broker.**

---

# 10. Producer → Topic → Partition

Suppose:

```text
Topic: payments

P0
P1
P2
```

Producer sends:

```text
Payment ID = 101
```

The producer's partitioning logic determines:

```text
Payment 101 → P1
```

Then the request conceptually becomes:

```text
Producer
    │
    │ Produce Request
    │
    │ Topic: payments
    │ Partition: P1
    │ Record: Payment 101
    ▼
Kafka Broker
    │
    ▼
P1
```

---

# 11. How Does Producer Decide the Partition?

This is directly related to the **partition concept** you already learned.

There are two basic cases to understand at this stage.

### Case 1 — Producer has a key

Example:

```text
Key = customer-101
```

The producer can use the key to consistently determine a partition.

Conceptually:

```text
customer-101
      │
      ▼
Partitioning logic
      │
      ▼
     P1
```

So:

```text
customer-101 → P1
```

Another event with the same key can be mapped to the same partition under the same partitioning setup.

This is useful when you want ordering for that key.

---

# 12. Example With Customer ID

Suppose:

```text
Topic: customer-events
Partitions: 3
```

Producer sends:

```text
Customer 101 → Login
Customer 101 → Purchase
Customer 101 → Logout
```

Using:

```text
key = customer-101
```

they can be routed to the same partition:

```text
customer-101
      │
      ▼
     P1
      │
      ├── Login
      ├── Purchase
      └── Logout
```

This connects directly to what you learned about **ordering in partitions**.

```text
Same key
   ↓
Same partition
   ↓
Partition ordering
```

---

# 13. Case 2 — No Key

If the producer doesn't provide a key, Kafka's producer-side partitioning logic can distribute records among the available partitions.

For example:

```text
Event A → P0
Event B → P1
Event C → P2
Event D → P0
Event E → P1
```

Conceptually:

```text
             Producer
                 │
        ┌────────┼────────┐
        ▼        ▼        ▼
       P0       P1       P2
```

The exact partition-selection behavior depends on the Kafka client/version and producer configuration, so don't memorize a simplistic "no key always means round-robin" rule.

For your current stage, remember:

> **A key can influence partition selection; without a key, the producer can distribute records across partitions.**

---

# 14. Multiple Records

Suppose your application creates:

```text
Order 101
Order 102
Order 103
Order 104
```

The producer can send records efficiently rather than treating every event as a completely separate network interaction.

Conceptually:

```text
Application
    │
    │
    ▼
Producer
    │
    │ Records
    │
    ├── Order 101
    ├── Order 102
    ├── Order 103
    └── Order 104
    │
    ▼
Kafka
```

The producer can organize records into requests efficiently.

For now, the key concept is simply:

> **The producer sends Kafka produce requests containing records.**

---

# 15. Request Flow Diagram 🔥

Here's the architecture you should remember:

```text
                   Application
                       │
                       │ create event
                       ▼
                  Kafka Producer
                       │
                       │
                 ┌─────┴─────┐
                 │           │
              Topic          Key
                 │           │
                 └─────┬─────┘
                       ▼
                Partition Selection
                       │
                       ▼
                Produce Request
                       │
                       ▼
                  Kafka Broker
                       │
                       ▼
                 Kafka Partition
                       │
                       ▼
                    Record
                       │
                       ▼
                    Offset
```

---

# 16. Producer Request vs Consumer Request

This is a very useful comparison.

### Producer

```text
Producer
   │
   │ "Store these records"
   ▼
Kafka
```

### Consumer

```text
Consumer
   │
   │ "Give me records"
   ▼
Kafka
```

So:

|            | Producer                      | Consumer                        |
| ---------- | ----------------------------- | ------------------------------- |
| Main job   | Send records                  | Read records                    |
| Direction  | Application → Kafka           | Kafka → Application             |
| Works with | Topics/partitions             | Topics/partitions               |
| Partition  | Record is sent to a partition | Records are read from partition |
| Offset     | Kafka assigns it              | Consumer uses position/offset   |

---

# 17. Real-World Example — Payment System 💳

Application receives a payment:

```text
Payment ID: PAY-1001
Customer ID: CUST-50
Amount: ₹5,000
```

Application creates:

```text
Key:
CUST-50

Value:
PAYMENT_COMPLETED
```

Producer:

```text
Application
     │
     ▼
Kafka Producer
     │
     │ Topic = payments
     │ Key = CUST-50
     ▼
Partition Selection
     │
     ▼
P2
     │
     ▼
Produce Request
     │
     ▼
Kafka
```

Kafka appends:

```text
payments / P2

Offset 0 → ...
Offset 1 → ...
Offset 2 → PAYMENT_COMPLETED
```

The record now has:

```text
Topic     = payments
Partition = P2
Offset    = 2
```

This gives you a complete identity/position for that record.

---

# 18. What Happens If the Producer Sends 100 Events?

Suppose:

```text
Topic: orders
Partitions: 3
```

Producer generates:

```text
100 events
```

They can be distributed:

```text
P0 → Events
P1 → Events
P2 → Events
```

So Kafka can use multiple partitions to distribute the workload.

```text
                  Producer
                     │
             100 events
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
         P0         P1         P2
```

This is why partitions are important for Kafka scalability.

---

# 19. What Happens to Ordering?

Suppose the producer sends:

```text
Customer A:
Event 1
Event 2
Event 3
```

and all three use:

```text
Key = Customer A
```

They can go to:

```text
P1
```

Then:

```text
P1

Offset 10 → Event 1
Offset 11 → Event 2
Offset 12 → Event 3
```

So the ordering we learned earlier is preserved **within that partition**.

---

# 20. Senior Platform Engineer Perspective

When you're designing a producer, think:

```text
Application
    │
    ├── What topic?
    │
    ├── What key?
    │
    └── What event?
    │
    ▼
Producer
    │
    ▼
Partition
    │
    ▼
Kafka
```

The important questions at **this stage** are:

* Which topic should the event go to?
* What should be the partition key?
* How should events be distributed across partitions?
* Do related events need to stay in the same partition for ordering?
* Which Kafka broker should the producer communicate with?

---

# 21. Production Scenario

Imagine:

```text
Order Service
```

generates:

```text
ORDER_CREATED
ORDER_UPDATED
ORDER_SHIPPED
```

You have:

```text
Topic = order-events
Partitions = 6
```

You decide:

```text
Key = orderId
```

So:

```text
Order-1001
    ↓
Partition 3

Order-1002
    ↓
Partition 5

Order-1003
    ↓
Partition 1
```

Now events belonging to the same order can remain together:

```text
Order-1001 → P3

ORDER_CREATED
ORDER_UPDATED
ORDER_SHIPPED
```

while different orders can use different partitions:

```text
Order-1001 → P3
Order-1002 → P5
Order-1003 → P1
```

This gives you:

> **Ordering for an order + distribution across partitions.**

---

# 22. The Complete Producer Lifecycle

Memorize this flow:

```text
1. Application creates event
           ↓
2. Producer receives record
           ↓
3. Producer identifies topic
           ↓
4. Producer determines partition
           ↓
5. Producer creates Produce Request
           ↓
6. Request goes to Kafka broker
           ↓
7. Kafka appends record to partition
           ↓
8. Kafka assigns offset
           ↓
9. Producer receives response
```

---

# 23. One Important Distinction

Don't say:

❌ **"Producer sends the message to a topic and Kafka randomly stores it somewhere."**

A better explanation is:

> **"The producer sends a produce request for a topic, and the record is directed to a specific partition based on the partitioning logic. Kafka then appends the record to that partition and assigns its offset."**

That's a much stronger **Senior Platform Engineer interview answer**.

---

# 24. Interview Question 🎯

### "How does a Kafka Producer send a message?"

A good answer:

> "A Kafka producer is a client application that creates records and sends them to a Kafka topic using Kafka's produce request protocol. The producer determines the target partition based on the partitioning logic, often using the record key when one is provided. It then sends the produce request to the appropriate Kafka broker. Kafka appends the record to the partition and assigns it an offset. The producer receives a response from Kafka after the request is handled."

---

# 25. 🔥 Final Mental Model

```text
                    APPLICATION
                         │
                    Create Event
                         │
                         ▼
                   KAFKA PRODUCER
                         │
                  Topic + Key + Value
                         │
                         ▼
                Partition Selection
                         │
                         ▼
                  Produce Request
                         │
                         ▼
                   KAFKA BROKER
                         │
                         ▼
                     PARTITION
                         │
                    Append Record
                         │
                         ▼
                      OFFSET
```

### Remember these 6 points:

1. **Producer creates/sends records.**
2. **Producer sends a Produce Request to Kafka.**
3. **The record belongs to a specific topic partition.**
4. **The key can influence which partition receives the record.**
5. **Kafka appends the record to the partition.**
6. **Kafka assigns the record an offset.**

That completes the **basic producer → Kafka → partition → offset flow** without jumping into producer-specific topics that come later.
