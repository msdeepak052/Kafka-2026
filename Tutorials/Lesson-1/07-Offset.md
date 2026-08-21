# 5. Kafka Offset

**Offset is one of the most important Kafka concepts for a Platform Engineer**, because offsets are directly involved in **consumer progress, replay, consumer lag, troubleshooting, recovery, and reprocessing**.

---

# 1. What is an Offset?

* An **offset is a unique sequential number assigned to each record within a Kafka partition**.
* It tells us the **position of a record in that partition**.
* Kafka uses offsets so consumers can keep track of **how far they have read**.

### Simple definition

> **Offset = The position/sequence number of a record inside a Kafka partition.**

Example:

```text
Topic: orders
Partition: 0

Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
Offset 3 → Order D
Offset 4 → Order E
```

---

# 2. Easy Analogy 📖

Think of a **book**.

```text
Page 1 → Event A
Page 2 → Event B
Page 3 → Event C
Page 4 → Event D
```

Your bookmark is currently at:

```text
Page 3
```

You know:

> "I have already read up to here."

Kafka's offset works similarly.

```text
Kafka Partition
────────────────────────────
0 → A
1 → B
2 → C
3 → D
4 → E
────────────────────────────
        ↑
   Consumer position
```

The consumer knows where it is in the stream.

---

# 3. Offset Belongs to a Partition

This is **extremely important**.

Suppose:

```text
Topic: orders
```

has 3 partitions:

```text
P0:
0 → A
1 → B
2 → C

P1:
0 → D
1 → E
2 → F

P2:
0 → G
1 → H
2 → I
```

Notice:

```text
P0 → offset 0
P1 → offset 0
P2 → offset 0
```

That's perfectly valid.

### Therefore:

❌ There is no single global offset for a Kafka topic.

✅ Each partition has its own offset sequence.

---

# 4. Why Does Kafka Need Offsets?

Imagine a consumer reads:

```text
A
B
C
D
```

Then the consumer crashes.

When it comes back, Kafka needs to know:

> "Where should I continue reading?"

That's where the offset comes in.

```text
Partition:

0 → A
1 → B
2 → C
3 → D
4 → E
      ↑
   Consumer
```

If the consumer had successfully processed through offset `3`, it can continue from the appropriate next position.

---

# 5. Producer and Offset

When a producer sends a record:

```text
Producer
   │
   ▼
Kafka Partition
```

Kafka appends the record:

```text
P0

Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
```

Kafka assigns the offsets.

### Important:

> **The producer does not normally decide the offset. Kafka assigns it when the record is appended to the partition log.**

---

# 6. Offset Is Not a Global Message ID

This is another common mistake.

Suppose:

```text
P0 → offset 10
P1 → offset 10
```

Both can exist.

Therefore:

```text
10
```

by itself does **not uniquely identify a record across the entire topic**.

You need:

```text
Topic + Partition + Offset
```

For example:

```text
orders + P1 + offset 10
```

That identifies the position of the record.

---

# 7. Consumer and Offset

Suppose:

```text
P0:

0 → A
1 → B
2 → C
3 → D
4 → E
```

Consumer reads:

```text
A
B
C
```

Its progress is tracked through offsets.

Conceptually:

```text
0 → processed
1 → processed
2 → processed
3 → next
```

The consumer's position is therefore around:

```text
offset 3
```

The exact distinction between **current position**, **committed offset**, and **next offset to fetch** is important, so let's separate them.

---

# 8. Current Position vs Committed Offset

This is a **Senior-level concept**.

There are two important ideas:

### Current position

Where the consumer is currently reading/processing.

### Committed offset

The position the consumer has explicitly committed as its progress.

Example:

```text
Partition:

0 → A
1 → B
2 → C
3 → D
4 → E
```

Consumer:

```text
Read:
A
B
C
D
```

But suppose it has only committed:

```text
Committed offset = 3
```

Then:

```text
Current processing position → 4
Committed progress         → 3
```

If the consumer crashes, Kafka can use the committed offset to determine where to resume.

---

# 9. Why Commit Offsets?

Imagine:

```text
P0:

0 → Payment A
1 → Payment B
2 → Payment C
3 → Payment D
```

Consumer:

```text
Process A ✓
Process B ✓
Process C ✓
```

Then:

```text
Consumer 💥
```

If it had committed its progress correctly, after restart it can resume from the appropriate point.

Without committed offsets, the consumer may need to start based on its configured reset behavior.

---

# 10. Consumer Group + Offset

Offsets become even more interesting with **consumer groups**.

Suppose:

```text
Topic: payments

P0
P1
P2
```

Consumer group:

```text
payment-service
```

Kafka tracks progress for the group.

Conceptually:

```text
payment-service

P0 → committed offset 100
P1 → committed offset 250
P2 → committed offset 180
```

These offsets belong to:

```text
Consumer Group + Topic + Partition
```

This means two different consumer groups can have completely different offsets for the same partition.

---

# 11. Same Topic, Different Consumer Groups

Suppose:

```text
Topic: orders
```

Two consumer groups:

```text
payment-service
analytics-service
```

They can have:

```text
payment-service:
P0 → offset 500

analytics-service:
P0 → offset 200
```

That's completely normal.

Why?

Because they consume the same event stream independently.

```text
                 orders
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
    payment-service      analytics-service
          │                   │
       offset 500           offset 200
```

This is one of the reasons Kafka works well as an **event-streaming platform**.

---

# 12. Where Are Consumer Offsets Stored?

Kafka stores committed consumer offsets in a special internal Kafka topic:

```text
__consumer_offsets
```

Think:

```text
Consumer Group
      │
      │ commit
      ▼
__consumer_offsets
      │
      ▼
Kafka
```

This is an important topic for Kafka administrators.

### Don't confuse:

```text
Application Topic
    ↓
orders
payments
```

with:

```text
Kafka Internal Topic
    ↓
__consumer_offsets
```

---

# 13. Offset Commit

Consumers can commit offsets automatically or manually.

### Automatic commit

Consumer configuration can enable:

```properties
enable.auto.commit=true
```

Kafka clients periodically commit offsets.

### Manual commit

The application explicitly decides when to commit.

Conceptually:

```text
Read message
   ↓
Process message
   ↓
Processing successful
   ↓
Commit offset
```

Manual control can be useful when you need stronger control over processing semantics.

---

# 14. Why Commit Timing Matters

This is extremely important.

Suppose:

```text
Read message
   ↓
Commit offset
   ↓
Process message
```

Then the application crashes here:

```text
Read ✓
Commit ✓
Process ❌
```

Kafka thinks:

> "This message has already been processed."

The message may not be processed again.

This can lead to **message loss from the application's processing perspective**.

---

Now:

```text
Read
   ↓
Process
   ↓
Commit
```

If processing succeeds:

```text
Process ✓
Commit ✓
```

Good.

If the consumer crashes before commit:

```text
Process ✓
Commit ❌
```

The message can potentially be processed again.

This can lead to **duplicate processing**.

---

# 15. At-Least-Once vs Exactly-Once

This is where offset management becomes connected to Kafka delivery semantics.

### At-most-once

```text
Commit
 ↓
Process
```

Potential:

```text
Message lost
```

### At-least-once

```text
Process
 ↓
Commit
```

Potential:

```text
Message processed twice
```

### Exactly-once

Requires additional Kafka/application mechanisms and careful end-to-end design.

Don't simply assume:

> "Kafka automatically gives exactly-once processing."

It doesn't.

---

# 16. Offset and Consumer Lag

This is one of the most important operational uses of offsets.

Suppose:

```text
Latest offset = 1000
Consumer committed offset = 800
```

Conceptually:

```text
Lag ≈ 1000 - 800
     ≈ 200
```

So the consumer is behind the producer by approximately 200 records.

```text
Partition:

0 ─────────────────────────────── 1000
                              ↑
                         Latest record

                         ↑
                       800
                   Consumer
```

The distance between the consumer's progress and the latest available data represents **consumer lag**.

---

# 17. Why Consumer Lag Matters

Imagine:

```text
Payment Topic
```

Producer:

```text
10,000 messages/sec
```

Consumer:

```text
5,000 messages/sec
```

Then:

```text
Incoming > Processing
```

Lag keeps increasing:

```text
100
500
1,000
10,000
100,000
```

As a Platform Engineer, you'd investigate:

```text
Consumer lag
    ↓
Consumer health
    ↓
Consumer processing time
    ↓
Number of consumers
    ↓
Partition count
    ↓
Downstream dependencies
```

---

# 18. Offset Reset

What happens if there is no valid committed offset?

Kafka consumers have a setting:

```properties
auto.offset.reset
```

Common values:

### `earliest`

Start from the earliest available record.

```text
A → B → C → D → E
↑
start here
```

### `latest`

Start from the latest position.

```text
A → B → C → D → E
                  ↑
                start
```

### `none`

Throw an error if no valid offset exists.

---

# 19. Replay Using Offsets 🔥

One of Kafka's biggest strengths is replay.

Suppose:

```text
orders

0 → A
1 → B
2 → C
3 → D
4 → E
```

Consumer has reached:

```text
offset 5
```

Now you discover:

> "There was a bug in our order-processing application."

If the records are still retained, you can reset the consumer group's offsets.

For example:

```text
Current:

0 → A
1 → B
2 → C
3 → D
4 → E
         ↑
       offset 5
```

Reset:

```text
0 → A
↑
Start again
```

Then the application can replay the events.

This is **extremely useful operationally**.

---

# 20. Real Production Scenario

Imagine your payment service has a bug.

Between:

```text
10:00 AM → 10:30 AM
```

it incorrectly processed payment events.

You fix the application.

Instead of asking the producer to resend everything, you can potentially:

```text
1. Identify affected offsets
2. Fix application
3. Reset consumer group offset
4. Replay retained events
5. Monitor consumer lag
6. Validate downstream results
```

This is one of Kafka's biggest operational advantages.

---

# 21. Offset and Partition Assignment

Suppose:

```text
Topic: payments

P0
P1
P2
```

Consumer group:

```text
C1
C2
```

Assignment:

```text
C1 → P0, P1
C2 → P2
```

Offsets are tracked per partition:

```text
C1:
P0 → offset 500
P1 → offset 700

C2:
P2 → offset 900
```

If C1 crashes:

```text
C1 ❌
```

Kafka rebalances:

```text
C2 → P0, P1, P2
```

C2 can resume P0 and P1 from their committed offsets.

This is why **offsets are essential for consumer recovery**.

---

# 22. Offset Is Not Deleted When Consumer Reads

This is a very common misunderstanding.

Suppose:

```text
P0

0 → A
1 → B
2 → C
```

Consumer reads:

```text
A
B
C
```

The offset doesn't mean:

> "Delete message 0."

Instead:

> "The consumer has progressed to this position."

The records remain according to the topic's retention policy.

---

# 23. Offset vs Record Deletion

Very important:

```text
Consumer reads message
       ↓
Offset advances
       ↓
Message remains in Kafka
       ↓
Retention policy eventually removes it
```

So:

**Consumer progress ≠ message deletion**

---

# 24. Offset vs Timestamp

Kafka also allows operational workflows based on timestamps.

For example:

> "Start processing events from 2 PM."

Conceptually:

```text
2 PM
 │
 ▼
Find corresponding offset
 │
 ▼
Start consumer from that offset
```

This is useful when troubleshooting production incidents.

---

# 25. Useful Kafka CLI Commands

### Check consumer group

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group payment-service
```

You might see:

```text
TOPIC     PARTITION   CURRENT-OFFSET   LOG-END-OFFSET   LAG
payments      0             800              1000        200
payments      1             450               500         50
payments      2             900               900          0
```

This is **extremely important for Kafka operations**.

---

# 26. How to Read This Output

For:

```text
payments P0
```

we have:

```text
CURRENT-OFFSET = 800
LOG-END-OFFSET = 1000
LAG = 200
```

Conceptually:

```text
Partition P0

0 ─────────────────── 800 ─────────────── 1000
                     ↑                    ↑
                  Consumer             Latest
```

So the consumer is behind by roughly:

```text
200 records
```

---

# 27. Senior Platform Engineer Troubleshooting

Suppose you get an alert:

> **Consumer lag = 2 million**

Don't immediately increase consumers.

Investigate:

```text
Consumer Lag
     │
     ├── Is consumer alive?
     │
     ├── Is consumer processing slowly?
     │
     ├── Is downstream DB slow?
     │
     ├── Is there a hot partition?
     │
     ├── Are all partitions affected?
     │
     ├── Are there enough consumers?
     │
     ├── Is partition count sufficient?
     │
     └── Is there a rebalance loop?
```

Example:

```text
P0 → Lag 10
P1 → Lag 15
P2 → Lag 2,000,000 🔥
P3 → Lag 20
```

This strongly suggests a **partition-specific problem**, rather than simply "Kafka is slow."

---

# 28. Important Difference: Current Offset vs Log End Offset

These two terms are extremely important.

### Current Offset

The consumer group's committed progress.

### Log End Offset

The offset representing the end/latest position of the partition log.

Example:

```text
Current Offset = 800
Log End Offset = 1000
```

Therefore:

```text
Lag ≈ 200
```

---

# 29. Offset and Retention

Suppose:

```text
Topic retention = 7 days
```

Consumer hasn't consumed an old message for 8 days.

Kafka may already have deleted that record.

Then:

```text
Old offset
   ↓
No longer available
```

The consumer cannot replay data that has already been removed due to retention.

Therefore:

> **Replay capability depends on the data still being retained.**

---

# 30. Offset + Senior Platform Engineer Mental Model

Think of Kafka like a DVR 📺.

```text
Kafka Partition
───────────────────────────────────
A    B    C    D    E    F    G
0    1    2    3    4    5    6
                         ↑
                    Consumer position
```

The producer keeps adding:

```text
H
I
J
```

The consumer can:

```text
▶ Continue from current position
⏪ Go back and replay
⏩ Catch up
```

as long as the records still exist.

---

# 31. Interview Questions

### Q1. What is a Kafka offset?

> An offset is a sequential position assigned to a record within a partition. Consumers use offsets to track their progress through the partition.

### Q2. Is offset unique across a Kafka topic?

> No. Offsets are unique only within a partition. To identify a record position, you need topic, partition, and offset.

### Q3. Where are committed consumer offsets stored?

> Kafka stores committed consumer group offsets in the internal `__consumer_offsets` topic.

### Q4. What is consumer lag?

> Consumer lag represents how far a consumer group is behind the latest available data in a partition, typically derived from the difference between the log end offset and the consumer's committed/current offset.

### Q5. Can Kafka replay messages?

> Yes, provided the records are still retained. We can reset consumer group offsets to an earlier position and reprocess the retained records.

### Q6. What happens if a consumer crashes?

> After reassignment/rebalance, another consumer can resume processing the assigned partition from its committed offset.

---

# 32. 🔥 The 8 Things You Must Remember

```text
1. Offset = position of a record
2. Offset belongs to a partition
3. No global topic offset
4. Consumer commits offsets
5. Committed offsets are stored in __consumer_offsets
6. Consumer lag is related to offset difference
7. Offset reset enables replay
8. Retention determines how far back you can replay
```

### The complete mental model

```text
                     Kafka Topic
                          │
                          ▼
                     Partition P0
                          │
          ┌───────────────┴───────────────┐
          ▼                               ▼
     Offset 100                      Offset 101
      Event A                         Event B
          │                               │
          └───────────────┬───────────────┘
                          ▼
                    Consumer Group
                          │
                          ▼
                  Committed Offset
                          │
                          ▼
                   __consumer_offsets
```

<img width="1788" height="980" alt="image" src="https://github.com/user-attachments/assets/21cd9105-947c-4d3e-a50b-138593d2e2cd" />


And the operational chain you should remember:

> **Partition → Offset → Consumer Position → Commit → Consumer Lag → Replay/Recovery**

That chain is fundamental to Kafka operations.
