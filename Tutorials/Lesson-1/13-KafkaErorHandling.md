# 13. What If Wrong Data Goes Into Kafka?

This is a very important **producer-side** question.

We just learned:

```text
Application
    ↓
Producer
    ↓
Kafka
    ↓
Topic
    ↓
Partition
```

Now the question is:

> **What happens if the producer sends incorrect or invalid data to Kafka?**


##  Kafka: Append, Not Update/Delete

For the Kafka model you're learning:

> **Kafka records are appended to the partition log. You don't normally update or delete an individual existing record.**

Example:

```text
Partition 0

Offset 10 → Order 123 → DELIVERED
Offset 11 → Order 124 → CREATED
Offset 12 → Order 125 → SHIPPED
```

If you realize:

```text
Offset 10 → DELIVERED ❌
```

you don't do:

```text
❌ UPDATE Offset 10 → CANCELLED
```

Instead, you append a new event:

```text
Offset 13 → Order 123 → CANCELLED
```

So the history becomes:

```text
Offset 10 → Order 123 → DELIVERED
Offset 11 → Order 124 → CREATED
Offset 12 → Order 125 → SHIPPED
Offset 13 → Order 123 → CANCELLED
```

### Think of Kafka like a diary 📖

You write:

```text
10:00 → Order 123 delivered
```

Later you discover it was wrong.

You **don't erase the previous page**.

You write:

```text
10:05 → Correction: Order 123 cancelled
```

The history remains.

---

## So for your current topic, remember these 4 points

```text
1. Kafka is append-oriented
        ↓
2. Existing records aren't normally updated
        ↓
3. Existing records aren't normally individually deleted
        ↓
4. Corrections are represented by new events
```

Therefore:

```text
Wrong Event
    ↓
DON'T UPDATE ❌
DON'T DELETE ❌
    ↓
APPEND CORRECTION EVENT ✅
```

And this is exactly why your screenshot's **Strategy 1 — "Treat events as immutable facts"** is important.

### One nuance

Kafka does have **retention and deletion mechanisms** that can remove old data according to configured policies, but that's different from an application saying:

> "Delete this particular record at offset 10."

We can cover those mechanisms when they appear later in your chronology.

So yes: **"Kafka = append, not update/delete" should have been explicitly highlighted in my previous answer.**


---

# 1. First Understand: Kafka Doesn't Understand Your Business Data

This is the most important point.

Kafka primarily acts as a **distributed event/log platform**.

Suppose your application sends:

```json
{
  "orderId": 101,
  "amount": 5000
}
```

Kafka stores the record.

But Kafka doesn't inherently know:

```text
❌ Is orderId valid?
❌ Is ₹5000 the correct amount?
❌ Should amount be positive?
❌ Does this order actually exist?
❌ Is this the correct customer?
```

Kafka's basic responsibility is:

```text
Receive record
      ↓
Store record
      ↓
Make record available to consumers
```

---

# 2. Easy Analogy 📦

Imagine a **courier company**.

You give the courier:

```text
Package
Address
Contents
```

The courier's primary job is:

```text
Receive package
      ↓
Transport package
      ↓
Deliver package
```

The courier doesn't necessarily know:

> "Is the information inside this package logically correct?"

Similarly:

```text
Producer
   ↓
Kafka
   ↓
Consumer
```

Kafka is primarily responsible for **storing and transporting the event**, not understanding whether your business logic is correct.

---

# 3. Example of Wrong Data

Suppose your application should send:

```json
{
  "orderId": 101,
  "status": "CREATED",
  "amount": 5000
}
```

But due to an application bug, it sends:

```json
{
  "orderId": 101,
  "status": "CREATED",
  "amount": -5000
}
```

Kafka can receive the record:

```text
Producer
   │
   │ amount = -5000
   ▼
Kafka
   │
   ▼
Partition
   │
   ▼
Record stored
```

Kafka doesn't automatically say:

> ❌ "Amount cannot be negative."

---

# 4. Where Should Validation Happen?

The **application/producer** should generally validate data before sending it.

```text
Application
     │
     ▼
Validate Data
     │
     ├── Invalid ──► Reject
     │
     └── Valid
           │
           ▼
       Kafka Producer
           │
           ▼
         Kafka
```

For example:

```text
Order ID exists?       ✓
Amount > 0?            ✓
Status valid?          ✓
Required fields?       ✓
        │
        ▼
Send to Kafka
```

---

# 5. Good Data Flow

Suppose the application receives:

```json
{
  "orderId": 101,
  "amount": 5000,
  "status": "CREATED"
}
```

Application validates:

```text
orderId → valid ✓
amount  → valid ✓
status  → valid ✓
```

Then:

```text
Application
     ↓
Validation ✓
     ↓
Producer
     ↓
Kafka
     ↓
Partition
```

---

# 6. Bad Data Flow

Suppose:

```json
{
  "orderId": 101,
  "amount": -5000,
  "status": "CREATED"
}
```

Validation should catch it:

```text
Application
     ↓
Validation
     ↓
amount = -5000 ❌
     ↓
Don't send to Kafka
```

So Kafka never receives the bad event.

---

# 7. What If Bad Data Still Gets Into Kafka?

This can happen.

For example:

```text
Application bug
      ↓
Incorrect event
      ↓
Producer
      ↓
Kafka
      ↓
Partition
```

Now Kafka has stored the incorrect event.

A consumer may read:

```text
Consumer
   ↓
Incorrect event
```

This can potentially cause downstream problems.

For example:

```text
Kafka
  ↓
Payment Consumer
  ↓
Wrong payment amount
  ↓
Incorrect processing
```

Therefore:

> **Bad producer data can propagate to downstream consumers.**

---

# 8. Why Is This Dangerous?

Imagine one Kafka topic:

```text
order-events
```

and three consumer groups:

```text
order-service
payment-service
analytics-service
```

A wrong event enters Kafka:

```text
Wrong Event
     │
     ▼
order-events
     │
 ┌───┼──────────┐
 ▼   ▼          ▼
Order Payment Analytics
```

The same incorrect event can be consumed independently by multiple applications.

That's why **producer-side validation is very important**.

---

# 9. What Should the Producer Validate?

At a basic level:

### Required fields

```text
orderId
customerId
amount
status
```

### Data types

```text
orderId → integer/string
amount  → number
status  → string
```

### Business rules

```text
amount > 0
status must be valid
orderId must exist
```

### Example

```text
Incoming Event
      ↓
┌─────────────────────┐
│ Validate            │
│                     │
│ orderId ✓           │
│ amount ✓            │
│ status ✓            │
└─────────────────────┘
      ↓
Producer
      ↓
Kafka
```

---

# 10. What If the Producer Sends Completely Invalid Data?

For example, the application accidentally sends:

```text
"Hello Kafka!!!"
```

instead of:

```json
{
  "orderId": 101,
  "status": "CREATED"
}
```

Kafka can still treat the payload as data.

Conceptually:

```text
Producer
   │
   │ "Hello Kafka!!!"
   ▼
Kafka
   │
   ▼
Partition
   │
   ▼
Record
```

Kafka itself isn't necessarily responsible for understanding that your application expected an order event.

---

# 11. Kafka Is Not Your Business Validation Layer

This is a very important Senior Platform Engineer distinction:

```text
Application
    │
    ├── Business validation
    ├── Data validation
    └── Event creation
            │
            ▼
       Kafka Producer
            │
            ▼
          Kafka
            │
            ▼
        Consumers
```

Think of Kafka as:

> **The transport/storage layer for your events.**

The application owns the meaning of those events.

---

# 12. What If the Producer Sends to the Wrong Topic?

This is another type of "wrong data."

Suppose the application should send:

```text
payment-events
```

but accidentally sends:

```text
order-events
```

The producer could successfully send the record.

```text
Application
     │
     │ Wrong topic
     ▼
order-events
     │
     ▼
Kafka
```

Kafka doesn't necessarily know:

> "This payment event belongs in `payment-events`."

That's an application/configuration problem.

---

# 13. What If the Producer Uses the Wrong Key?

Suppose:

```text
Key should be:
orderId
```

but the application accidentally uses:

```text
customerId
```

Then the partition mapping may be different from what you intended.

For example:

```text
Expected:

orderId
   ↓
Partition P1
```

but:

```text
customerId
   ↓
Partition P3
```

This can affect how your events are distributed and potentially your expected ordering behavior.

So the producer needs to use the **correct key** according to the application's design.

---

# 14. Senior Platform Engineer Perspective

When someone says:

> "Wrong data is going into Kafka."

Don't immediately blame Kafka.

Trace the flow:

```text
Application
     ↓
What data was generated?
     ↓
Producer
     ↓
What topic?
     ↓
What key?
     ↓
What partition?
     ↓
Kafka
```

You want to determine:

### 1. Was the data wrong before Kafka?

```text
Application → Wrong
```

Then the issue is likely upstream/application-side.

### 2. Was the topic wrong?

```text
Correct data
     ↓
Wrong topic
```

Then investigate producer configuration.

### 3. Was the partition key wrong?

```text
Correct event
     ↓
Wrong key
     ↓
Unexpected partition
```

### 4. Was Kafka actually responsible?

Usually, Kafka is simply storing what the producer sent.

---

# 15. Simple Production Example

Imagine:

```text
Order Service
```

generates:

```json
{
  "orderId": 1001,
  "status": "CREATED",
  "amount": 5000
}
```

Correct flow:

```text
Order Service
     ↓
Validate
     ↓
Kafka Producer
     ↓
order-events
     ↓
Partition
     ↓
Offset assigned
```

But a bug generates:

```json
{
  "orderId": 1001,
  "status": "CREATED",
  "amount": -5000
}
```

If validation is missing:

```text
Order Service
     ↓
❌ Wrong event
     ↓
Producer
     ↓
Kafka
     ↓
Partition
     ↓
Consumers
```

Now multiple downstream applications may receive the bad event.

---

# 16. What Should We Do When Bad Data Is Already There?

At this stage, remember the basic approach:

```text
1. Identify the bad event
        ↓
2. Identify topic
        ↓
3. Identify partition
        ↓
4. Identify offset
        ↓
5. Fix producer/application
        ↓
6. Decide how the bad event should be handled
```

The **topic + partition + offset** gives you a precise location for the problematic record.

That's directly connected to what you learned about offsets.

---

# 17. 🔥 Key Mental Model

```text
                  APPLICATION
                       │
                  Create Event
                       │
                       ▼
                  VALIDATION
                       │
                ┌──────┴──────┐
                ▼             ▼
             Invalid         Valid
                │             │
                X             ▼
                         KAFKA PRODUCER
                              │
                              ▼
                         KAFKA TOPIC
                              │
                              ▼
                          PARTITION
                              │
                              ▼
                            OFFSET
                              │
                              ▼
                          CONSUMERS
```

### Remember:

> **Kafka generally stores what the producer sends.**

> **The producer/application should validate that the event is correct before sending it.**

> **If incorrect data enters Kafka, consumers may receive it too.**

> **Topic + Partition + Offset helps us precisely locate a problematic record.**

---

### Interview answer 🎯

If asked:

> **"What happens if wrong data is sent to Kafka?"**

Say:

> "Kafka primarily acts as the event storage and transport layer. It doesn't inherently understand the business correctness of the payload. If a producer sends an invalid event, Kafka can store it in the target partition like any other record. Therefore, validation should generally happen in the producing application before publishing the event. If bad data reaches Kafka, we can identify the affected topic, partition, and offset and then investigate the producer and downstream processing."

This keeps us within the concepts you've covered so far—**Producer → Topic → Partition → Offset → Consumer**—without jumping ahead.
Yes. I checked the three approaches in your images against Kafka's behavior and official Kafka documentation. Your images are **conceptually correct**, but there are a couple of important nuances I would add so you learn the topic correctly without jumping ahead.

# 14. What If Wrong Data Goes Into Kafka?

We have learned:

```text
Application
     ↓
Kafka Producer
     ↓
Kafka Topic
     ↓
Partition
     ↓
Offset
     ↓
Kafka Consumer
```

Now imagine the producer sends incorrect data:

```json
{
  "orderId": 123,
  "status": "DELIVERED"
}
```

But actually the order should have been:

```text
CANCELLED
```

What do we do?

There are **3 important strategies**.

---

# Strategy 1 — Treat Events as Immutable Facts

### Your image is conceptually correct. ✅

The basic idea is:

> **Once an event has been written to Kafka, we don't go back and modify that historical event.**

Kafka records are appended to partitions, and the records have offsets identifying their position. ([Apache Kafka][1])

### Example

Suppose the producer incorrectly sent:

```json
{
  "orderId": 123,
  "status": "DELIVERED"
}
```

Kafka now has:

```text
Partition 0

Offset 10
    ↓
Order 123 → DELIVERED
```

We **don't go back and change offset 10** to:

```text
CANCELLED
```

Instead, we can produce another event:

```json
{
  "orderId": 123,
  "status": "CANCELLED"
}
```

So the history becomes:

```text
Partition 0

Offset 10 → Order 123 → DELIVERED
Offset 11 → Order 123 → CANCELLED
```

The consumer can process the sequence:

```text
DELIVERED
    ↓
CANCELLED
```

and derive the latest state:

```text
Final State
Order 123 = CANCELLED
```

### Easy analogy 📖

Think of a **bank statement**.

You shouldn't erase:

```text
₹10,000 credited
```

because later you discover it was wrong.

Instead, you create another transaction:

```text
₹10,000 credited
₹10,000 reversed
```

The complete history remains.

---

## Why is this useful?

Because Kafka's event history can tell you:

```text
What happened?
     ↓
When did it happen?
     ↓
What was corrected?
     ↓
What is the latest state?
```

This way, you don't lose historical information.

### Important correction to the image ⚠️

The statement:

> "Consumers build final state by processing events in order."

is **correct as a design pattern**, but it is not something Kafka automatically does.

Your **consumer/application logic** has to interpret the events and construct the final state.

This thinking is commonly associated with **event sourcing**, but we'll leave the deeper event-sourcing topic for its proper place in your chronology.

---

# Strategy 2 — Validate Before Producing

### This is the **preferred first line of defense**. ✅

Your image is correct:

> **Don't let bad data enter Kafka in the first place.**

The flow should ideally be:

```text
Application
     │
     ▼
Validate Event
     │
     ├──────────────┐
     │              │
   Invalid         Valid
     │              │
     ▼              ▼
   Reject       Kafka Producer
                    │
                    ▼
                  Kafka
```

---

# What Should We Validate?

### 1. Schema validation

Example expected:

```json
{
  "orderId": 123,
  "status": "DELIVERED"
}
```

But application sends:

```json
{
  "orderId": "ABC",
  "status": 123
}
```

That's structurally wrong.

---

### 2. Required fields

Expected:

```text
orderId ✓
status  ✓
```

But producer sends:

```json
{
  "orderId": 123
}
```

Missing:

```text
status ❌
```

Reject it.

---

### 3. Type checks

For example:

```text
orderId → integer
amount  → number
status  → string
```

If application sends:

```json
{
  "orderId": 123,
  "amount": "five thousand"
}
```

validation should catch it.

---

### 4. Business rules

This is different from simple schema validation.

Example:

```text
amount must be > 0
```

Producer sends:

```json
{
  "orderId": 123,
  "amount": -5000
}
```

Schema may technically be valid because `-5000` is a number.

But business validation says:

```text
amount = -5000
       ↓
INVALID ❌
```

So it shouldn't be published.

---

# Strategy 2 Architecture

```text
                  Application
                       │
                       ▼
                Validate Event
                       │
              ┌────────┴────────┐
              │                 │
           Invalid             Valid
              │                 │
              ▼                 ▼
           Reject          Kafka Producer
                                │
                                ▼
                              Kafka
                                │
                                ▼
                             Topic
                                │
                                ▼
                           Partition
```

### Senior Platform Engineer perspective

The best place to stop bad data is:

> **Before it enters the Kafka ecosystem.**

Because once the event is in Kafka, multiple consumers may receive it.

---

# Schema Registry — Small Note

Your image says:

> "This is why Schema Registry exists (we'll cover later)."

That's directionally correct. **Schema Registry is used for managing/validating schemas**, but don't worry about it now.

We'll cover it when you reach it in your sequence.

For now, remember only:

```text
Schema validation
      ↓
Prevent malformed/incompatible data
      ↓
Before/around publishing
```

---

# Strategy 3 — Dead Letter Queue (DLQ)

### Your image is also conceptually correct, but there is an important clarification. ⚠️

A DLQ is generally useful when a **consumer cannot successfully process a record**.

For example:

```text
Main Topic
     │
     ▼
 Consumer
     │
     X
   FAILS
     │
     ▼
 DLQ Topic
```

Kafka Connect explicitly supports sending records that cause processing errors to a configured DLQ topic. ([Apache Kafka][2])

---

# Example

Suppose Kafka contains:

```text
orders

Offset 100 → Good event
Offset 101 → Good event
Offset 102 → Malformed event ❌
Offset 103 → Good event
```

Consumer processes:

```text
100 → ✓
101 → ✓
102 → ❌
```

The consumer/application may decide:

```text
Problematic event
       ↓
DLQ Topic
```

For example:

```text
orders
   │
   ├── Offset 100 ✓
   ├── Offset 101 ✓
   ├── Offset 102 ❌
   └── Offset 103
           │
           ▼
      Consumer fails
           │
           ▼
     orders-dlq
```

The problematic event can then be investigated separately.

---

# What Does a DLQ Store?

Your image lists:

```text
Malformed data
Unexpected values
Poison messages
```

That's reasonable.

### Malformed data

```json
{
  "orderId":
```

Invalid JSON.

### Unexpected values

```json
{
  "orderId": 123,
  "status": "UNKNOWN_STATUS"
}
```

### Poison message

A record that repeatedly causes the consumer to fail.

Conceptually:

```text
Consumer
   ↓
Message
   ↓
FAIL
   ↓
Retry
   ↓
FAIL
   ↓
Retry
   ↓
FAIL
```

Instead of allowing that one problematic record to continually block processing, a DLQ strategy can isolate it.

---

# What Happens Later?

Your image says:

```text
Inspect
   ↓
Fix logic
   ↓
Replay if needed
```

That's a good operational workflow.

For example:

```text
             DLQ
              │
              ▼
          Investigate
              │
              ▼
          Fix problem
              │
              ▼
       Decide what to do
              │
        ┌─────┴─────┐
        ▼           ▼
     Discard      Replay
```

But **replay is an application/operational decision**, not something Kafka automatically performs just because a DLQ exists.

---

# 🔥 The Three Strategies Together

Now combine your three images:

```text
                   WRONG DATA
                       │
                       ▼
              ┌─────────────────┐
              │ Where caught?   │
              └────────┬────────┘
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      BEFORE        AFTER IT     CONSUMER
      KAFKA         ENTERS       CANNOT
                    KAFKA        PROCESS
          │            │            │
          ▼            ▼            ▼
    VALIDATION     CORRECTION      DLQ
          │          EVENT           │
          ▼            │            ▼
       Reject          ▼         Investigate
                    New Event        │
                         │           ▼
                         ▼        Fix Logic
                     History         │
                                  Replay
```

---

# Which Strategy Is Best?

### 🥇 Strategy 2 — Validation before producing

**Best first defense.**

```text
Bad data
   ↓
Validation
   ↓
REJECT
```

Don't put known-invalid data into Kafka.

---

### 🥈 Strategy 1 — Correction event

Useful when:

* An event was already published
* The business state later changes
* You need to preserve history
* You want events to represent what happened

Example:

```text
DELIVERED
    ↓
CANCELLED
```

You preserve both events rather than rewriting history.

---

### 🥉 Strategy 3 — DLQ

Useful when:

```text
Event is already in Kafka
        ↓
Consumer receives it
        ↓
Cannot process it
        ↓
Isolate problematic event
```

Then:

```text
Inspect → Fix → Replay if appropriate
```

---

# Very Important Distinction

Don't mix these three situations:

### ❌ Bad data BEFORE Kafka

```text
Application
    ↓
Bad data
    ↓
❌ Don't publish
```

➡️ **Validation**

---

### ❌ Business state changed AFTER publication

```text
DELIVERED
    ↓
CANCELLED
```

➡️ **New correction event**

---

### ❌ Consumer cannot process an existing event

```text
Kafka
   ↓
Consumer
   ↓
FAIL
   ↓
DLQ
```

➡️ **Dead Letter Queue**

---

# Senior Platform Engineer Interview Answer 🎯

If the interviewer asks:

> **"What happens if wrong data goes into Kafka?"**

A strong answer is:

> "Ideally, we validate the event before producing it so invalid data doesn't enter Kafka. If an incorrect business event has already been published, we generally don't modify the historical event; instead, we can publish a corrective event so consumers can derive the correct state while preserving history. If a consumer receives a record that it cannot process, we can use a DLQ strategy to isolate the problematic record, investigate it, fix the processing issue, and replay it if required."

### And remember the distinction:

```text
Validation
    ↓
Prevent bad data


Correction Event
    ↓
Correct business state without rewriting history


DLQ
    ↓
Handle records consumers cannot process
```

This is the **validated version of exactly what your three images are teaching**, while keeping the deeper topics like Schema Registry and event sourcing for when they appear later in your chronology. ([Apache Kafka][1])

[1]: https://kafka.apache.org/41/implementation/message-format/?utm_source=chatgpt.com "Message Format | Apache Kafka"
[2]: https://kafka.apache.org/28/kafka-connect/user-guide/?utm_source=chatgpt.com "User Guide | Apache Kafka"
