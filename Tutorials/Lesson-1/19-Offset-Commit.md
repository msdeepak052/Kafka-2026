# 19. Offset Commit Strategies & Delivery Guarantees

This is the natural next topic after **Offsets + Consumers + Consumer Groups**.

We already know:

```text
Kafka Partition
      │
      ├── Offset 0
      ├── Offset 1
      ├── Offset 2
      └── Offset 3
```

A consumer reads these records.

Now the important question is:

> **How does Kafka know how far the consumer has successfully processed?**

That's where **offset commits** come in.

---

# 1. What is an Offset Commit?

A consumer keeps track of its progress.

Suppose:

```text
P0

Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
Offset 3 → Order D
```

Consumer processes:

```text
A ✓
B ✓
C ✓
```

The consumer can **commit its progress**.

Conceptually:

```text
Consumer
   │
   └── "I have processed up to here"
                 ↓
              Offset
```

The committed offset is stored by Kafka for the consumer group.

### Easy analogy 📖

Imagine reading a book.

```text
Page 1 ✓
Page 2 ✓
Page 3 ✓
```

You put a bookmark at page 4.

If you close the book and reopen it:

```text
Resume from page 4
```

The committed offset is like Kafka's **bookmark** for the consumer group.

---

# 2. Why Does Offset Commit Matter?

Because consumers can fail.

Suppose:

```text
P0

Offset 100 → Order A
Offset 101 → Order B
Offset 102 → Order C
```

Consumer processes A and B.

Then:

```text
Consumer 💥 CRASH
```

When it restarts, it needs to know:

> "Where should I continue from?"

The committed offset provides the recovery position.

---

# 3. The Critical Problem

Here's where delivery guarantees come in.

Suppose:

```text
Offset 100 → Order A
```

Consumer receives it.

Now there are two things happening:

```text
1. Process the message
2. Commit the offset
```

**Which happens first?**

That's the fundamental question behind:

```text
At-most-once
At-least-once
Exactly-once
```

---

# 4. Strategy 1 — Commit BEFORE Processing

Flow:

```text
Read message
    ↓
Commit offset
    ↓
Process message
```

Example:

```text
Offset 100 → Payment ₹5,000
```

Consumer:

```text
Read Offset 100
      ↓
Commit Offset 101
      ↓
Process payment
```

Now suppose:

```text
Process payment
       ↓
       💥 Consumer crashes
```

The offset was already committed.

When the consumer restarts:

```text
Kafka
  ↓
Resume from Offset 101
```

It **doesn't process Offset 100 again**.

---

# 5. The Problem

What if the crash happened **before processing completed**?

```text
Offset 100
   ↓
Commit ✓
   ↓
Process
   ↓
💥 Crash
```

The event was committed but wasn't successfully processed.

So:

```text
Message
   ↓
❌ Processing lost
```

This gives us:

# At-Most-Once Delivery

> **The message is processed zero or one time, but not more than once.**

You may lose a message, but you avoid duplicate processing caused by consumer restart.

---

# 6. Easy Analogy — Parcel Delivery 📦

Imagine a delivery agent marks:

```text
"Package delivered" ✓
```

**before actually delivering the package.**

Then:

```text
Agent crashes
```

The system thinks:

```text
Delivered ✓
```

But the customer never got it.

That's the basic danger of committing before processing.

---

# 7. At-Most-Once Flow

```text
Kafka
  │
  ▼
Consumer
  │
  ▼
Commit Offset
  │
  ▼
Process Message
  │
  X
Crash possible
```

### Benefit

```text
No duplicate processing
```

### Risk

```text
Message can be lost
```

---

# 8. Strategy 2 — Process BEFORE Committing

Now reverse the order.

```text
Read message
    ↓
Process message
    ↓
Commit offset
```

Example:

```text
Offset 100 → Payment ₹5,000
```

Consumer:

```text
Read Offset 100
      ↓
Process payment ✓
      ↓
Commit Offset 101
```

Everything is successful.

Great.

But now consider a crash.

---

# 9. Consumer Crashes Before Commit

```text
Read Offset 100
      ↓
Process payment ✓
      ↓
💥 Crash
      ↓
Offset NOT committed
```

When the consumer restarts:

```text
Kafka
  ↓
Last committed offset = 100
  ↓
Read Offset 100 again
```

So:

```text
Payment ₹5,000
       ↓
Processed once
       ↓
Crash
       ↓
Processed AGAIN
```

Now we have a duplicate.

---

# 10. At-Least-Once Delivery

This gives us:

> **At-least-once delivery.**

Meaning:

> Kafka will allow the record to be processed again if the offset wasn't committed, so the record won't be lost as easily—but duplicate processing is possible.

Flow:

```text
Read
 ↓
Process
 ↓
Commit
```

If crash occurs before commit:

```text
Process
  ↓
Crash
  ↓
Read again
  ↓
Process again
```

---

# 11. Easy Analogy 📦

Imagine a delivery agent:

```text
Deliver package
      ↓
Then mark "Delivered"
```

If the agent delivers the package:

```text
Package delivered ✓
```

but crashes before marking it:

```text
Status update ❌
```

The system may send another delivery attempt.

Customer could potentially receive:

```text
Package #1
Package #2
```

That's the duplicate-processing idea behind **at-least-once**.

---

# 12. At-Least-Once Architecture

```text
Kafka
  │
  ▼
Consumer
  │
  ▼
Process Message
  │
  ├── SUCCESS
  │      ↓
  │   Commit Offset
  │
  └── CRASH
         ↓
    Offset not committed
         ↓
      Reprocess
```

### Benefit

```text
Less chance of losing messages
```

### Risk

```text
Duplicate processing possible
```

---

# 13. At-Most-Once vs At-Least-Once

|                      | At-Most-Once      | At-Least-Once    |
| -------------------- | ----------------- | ---------------- |
| Commit               | Before processing | After processing |
| Message loss         | Possible          | Minimized        |
| Duplicate processing | Generally avoided | Possible         |
| Main concern         | Loss              | Duplicates       |
| Typical approach     | Commit → Process  | Process → Commit |

---

# 14. Real Payment Example 💳

Suppose:

```text
Topic:
payment-events

Offset 100:
PAYMENT ₹5,000
```

Consumer:

```text
Payment Service
```

### At-most-once

```text
Read payment
      ↓
Commit offset
      ↓
Charge customer
      ↓
💥 Crash
```

Payment might **never happen**.

---

### At-least-once

```text
Read payment
      ↓
Charge customer ✓
      ↓
💥 Crash
      ↓
Offset wasn't committed
      ↓
Read payment again
      ↓
Charge customer again ❌
```

Now the customer could potentially be charged twice.

This is why **at-least-once processing requires careful application design when operations aren't naturally idempotent**.

---

# 15. Idempotency Becomes Important

Suppose the consumer receives:

```text
PAYMENT-1001
```

twice:

```text
PAYMENT-1001
PAYMENT-1001
```

You don't want:

```text
₹5,000
+
₹5,000
=
₹10,000 ❌
```

Instead, the application can recognize:

```text
Payment ID = PAYMENT-1001

Already processed ✓
```

and avoid performing the business operation twice.

Conceptually:

```text
Kafka
  │
  ├── PAYMENT-1001
  └── PAYMENT-1001
          ↓
      Consumer
          ↓
   Check Payment ID
          ↓
    Already processed
          ↓
       Ignore
```

This is why **idempotent consumer/application logic** is important with at-least-once delivery.

We can go deeper into idempotency when it appears in your course.

---

# 16. Strategy 3 — Exactly Once

Now we reach the most misunderstood term.

> **Exactly-once processing means the intended result of processing an event is reflected once, without duplicates or loss under the supported failure model.**

But be careful:

### Don't think:

```text
Kafka magically guarantees
every external operation happens exactly once.
```

That's too broad.

---

# 17. Simple Exactly-Once Example

Imagine Kafka-to-Kafka processing:

```text
Topic A
   ↓
Consumer/Processor
   ↓
Topic B
```

Suppose:

```text
Input:
Order 1001
```

We want:

```text
Output:
Order 1001 processed
```

without producing duplicate output if the processor crashes.

Kafka provides transactional mechanisms that can coordinate processing and output writes in supported Kafka workflows.

Conceptually:

```text
Topic A
   ↓
Process
   ↓
Topic B
```

with the processing/output and offset progress handled transactionally.

---

# 18. Important Exactly-Once Caveat ⚠️

Suppose your consumer does:

```text
Kafka
  ↓
Consumer
  ↓
External Payment API
  ↓
Bank
```

Kafka cannot automatically make the **external bank API** exactly-once just because Kafka has exactly-once capabilities.

For example:

```text
Kafka Event
    ↓
Consumer
    ↓
Bank API
    ↓
₹5,000 charged ✓
    ↓
Consumer crashes
```

Kafka may retry the event.

Now:

```text
Same payment
    ↓
Bank API again
```

You need mechanisms such as an **idempotency key / idempotent business operation** on the external system to safely handle that.

So:

> **Exactly-once semantics are strongest within systems that participate in the relevant transactional mechanism; external side effects require additional design.**

---

# 19. Three Delivery Guarantees

Now put them side-by-side.

```text
             DELIVERY GUARANTEES
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
   At-Most-Once  At-Least-Once  Exactly-Once
        │            │            │
     Commit       Process       Transactional
     before       before        processing/
     process      commit        supported flow
        │            │            │
      Loss        Duplicate      No duplicate
      possible    possible       result in
                                 supported EOS
```

---

# 20. Easy Real-World Analogy

Imagine a restaurant order.

### At-Most-Once

```text
Mark order completed
        ↓
Prepare food
```

If restaurant crashes after marking:

```text
Order marked completed
Food never prepared ❌
```

**Possible loss.**

---

### At-Least-Once

```text
Prepare food
     ↓
Mark completed
```

If restaurant crashes before marking:

```text
Food prepared ✓
Status not updated
```

The order may be prepared again.

**Possible duplicate.**

---

### Exactly-Once

```text
Prepare + record completion
        ↓
Atomic operation
```

Under a system that supports the required transactional semantics:

```text
Either both happen
or neither happens
```

That's the basic intuition.

---

# 21. What About Auto Commit?

Kafka consumers can automatically commit offsets.

Conceptually:

```text
Consumer
    ↓
Poll records
    ↓
Auto commit periodically
```

This can make offset management easier, but the timing of the commit relative to actual processing matters.

For example:

```text
Poll records
   ↓
Auto commit happens
   ↓
Application still processing
   ↓
💥 Crash
```

The committed progress may be ahead of what was actually processed.

That can lead to records being skipped after restart.

So you shouldn't simply think:

> **"Auto commit = safe."**

It depends on how the application processes the records and how commits are configured.

---

# 22. Manual Commit

With manual commit, the application decides when to commit.

For example:

```text
Poll
 ↓
Process
 ↓
Success
 ↓
Commit
```

This gives the application more control over the relationship between:

```text
Processing
    +
Offset commit
```

This is often easier to reason about when you're learning delivery semantics.

---

# 23. Example With Offsets

Suppose:

```text
P0

Offset 10 → A
Offset 11 → B
Offset 12 → C
```

Consumer reads:

```text
A ✓
B ✓
C ✓
```

Then commits:

```text
Committed position → 13
```

If consumer crashes:

```text
Restart
   ↓
Resume from committed position
```

The exact offset semantics can be phrased carefully because the committed offset represents the **next record to read**, so after successfully processing offset 12, committing 13 means:

```text
Resume from 13
```

---

# 24. Production Example — Order Processing

Let's use the case study from the previous topic.

```text
order-events
```

contains:

```text
Offset 100 → ORD-1001 CREATED
Offset 101 → ORD-1002 CREATED
Offset 102 → ORD-1003 CREATED
```

Inventory consumer processes Offset 100.

### At-most-once

```text
Read 100
 ↓
Commit 101
 ↓
Reserve inventory
 ↓
Crash
```

Potential result:

```text
Order event skipped
Inventory not reserved ❌
```

---

### At-least-once

```text
Read 100
 ↓
Reserve inventory ✓
 ↓
Crash
 ↓
Offset 101 was not committed
 ↓
Read 100 again
 ↓
Reserve inventory again ❌
```

Potential duplicate operation.

---

### Exactly-once / transactional approach

For a Kafka-supported transactional processing flow:

```text
Read 100
 ↓
Process
 ↓
Produce result
 ↓
Commit processing progress transactionally
```

If the transaction succeeds:

```text
Result + progress → committed
```

If it fails:

```text
Result + progress → not committed
```

This avoids duplicate Kafka output in the supported transactional workflow.

---

# 25. 🔥 The Most Important Diagram

```text
                    Kafka Record
                         │
                         ▼
                     Consumer
                         │
              ┌──────────┼──────────┐
              │          │          │
              ▼          ▼          ▼
          COMMIT      PROCESS    DELIVERY
           ORDER       ORDER      GUARANTEE
              │          │
              │          │
       ┌──────┴───┐  ┌───┴────────┐
       ▼          ▼  ▼            ▼
    BEFORE      AFTER            TRANSACTIONAL
    PROCESS    PROCESS
       │          │
       ▼          ▼
 At-Most-Once At-Least-Once
       │          │
       ▼          ▼
    Loss       Duplicate
   possible    possible
```

---

# 26. Senior Platform Engineer Mental Model

When troubleshooting a Kafka consumer, always ask:

```text
1. When is the record processed?
             ↓
2. When is the offset committed?
             ↓
3. What happens if the consumer crashes
   between those two operations?
             ↓
4. Can the record be lost?
             ↓
5. Can the record be processed again?
```

This lets you identify the delivery guarantee.

---

# 27. Interview Questions 🎯

### Q: What is at-most-once?

> "The consumer commits the offset before processing the record. If it crashes after committing but before processing, the record may be skipped. Therefore duplicates are avoided at the cost of possible message loss."

### Q: What is at-least-once?

> "The consumer processes the record first and commits the offset after successful processing. If it crashes after processing but before committing, the record can be consumed again, so duplicates are possible."

### Q: What is exactly-once?

> "Exactly-once semantics aim to ensure that processing produces one effective result without loss or duplication within the supported transactional processing scope. Kafka provides transactional mechanisms for supported Kafka-to-Kafka workflows, but external systems such as payment APIs need additional idempotency or transactional mechanisms."

---

# 🔥 Final Cheat Sheet

| Strategy          | Order                    | Failure scenario                                        | Result                   |
| ----------------- | ------------------------ | ------------------------------------------------------- | ------------------------ |
| **At-most-once**  | Commit → Process         | Crash after commit                                      | **Possible loss**        |
| **At-least-once** | Process → Commit         | Crash before commit                                     | **Possible duplicate**   |
| **Exactly-once**  | Transactional processing | Transaction fails/succeeds atomically in supported flow | **One effective result** |

### The easiest way to remember:

```text
AT-MOST-ONCE
Commit first
    ↓
Don't repeat
    ↓
But can lose


AT-LEAST-ONCE
Process first
    ↓
Don't lose easily
    ↓
But can repeat


EXACTLY-ONCE
Transactional
    ↓
One effective result
    ↓
Within supported scope
```

**This is why offset management is not just "where did I read up to?" — it directly determines what can happen when a consumer crashes.**
