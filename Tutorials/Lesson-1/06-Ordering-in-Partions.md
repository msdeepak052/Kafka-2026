# Kafka Partition Ordering

This is a **very important Kafka concept**, especially because partitioning and ordering are directly connected.

The single rule to remember is:

> 🔥 **Kafka guarantees ordering of records within a partition, but NOT across partitions.**

---

# 1. What does "ordering" mean?

Suppose a producer sends these events:

```text
Event 1 → Order Created
Event 2 → Payment Successful
Event 3 → Order Shipped
Event 4 → Order Delivered
```

If all four events go into the **same partition**:

```text
Partition 0

Offset 0 → Order Created
Offset 1 → Payment Successful
Offset 2 → Order Shipped
Offset 3 → Order Delivered
```

Kafka maintains this order:

```text
Created
   ↓
Payment
   ↓
Shipped
   ↓
Delivered
```

The consumer reads them in that partition order.

---

# 2. Easy Analogy 📚

Imagine a **queue at a bank**.

```text
Person A
Person B
Person C
Person D
```

The bank processes them:

```text
A → B → C → D
```

That's like a **Kafka partition**.

Now imagine you have 3 counters:

```text
Counter 1 → A, C
Counter 2 → B, D
Counter 3 → E, F
```

Now you can't say exactly which counter will finish first.

Similarly, with multiple Kafka partitions:

```text
P0 → A → B → C

P1 → X → Y → Z
```

Kafka guarantees:

```text
A → B → C
```

and:

```text
X → Y → Z
```

But it does **not** guarantee:

```text
A → X → B → Y → C → Z
```

---

# 3. Ordering Within a Partition

Consider:

```text
Topic: orders
```

with:

```text
P0
```

Producer sends:

```text
Order Created
Payment Completed
Order Shipped
Order Delivered
```

Kafka stores:

```text
P0

Offset 0 → Created
Offset 1 → Payment
Offset 2 → Shipped
Offset 3 → Delivered
```

The offsets establish the sequence.

```text
0 → 1 → 2 → 3
```

So the consumer sees the records in that partition's log order.

---

# 4. What Happens With Multiple Partitions?

Suppose:

```text
orders
│
├── P0
└── P1
```

Records may be distributed:

```text
P0:
Offset 0 → Order A
Offset 1 → Order C
Offset 2 → Order E

P1:
Offset 0 → Order B
Offset 1 → Order D
Offset 2 → Order F
```

Kafka guarantees:

```text
P0:
A → C → E

P1:
B → D → F
```

But **not**:

```text
A → B → C → D → E → F
```

---

# 5. Why Can't Kafka Guarantee Global Ordering?

Because partitions are processed independently.

Imagine:

```text
                 Topic
                   │
          ┌────────┴────────┐
          ▼                 ▼
         P0                P1
          │                 │
       Broker 1          Broker 2
          │                 │
       Consumer 1       Consumer 2
```

Suppose:

```text
P0:
A → B → C
```

and:

```text
P1:
X → Y → Z
```

Consumer 1 and Consumer 2 process independently.

Maybe:

```text
C1: A ─── B ───── C
C2: ─ X ── Y ─────── Z
```

Network latency, processing time, broker load, etc. can differ.

Kafka therefore cannot guarantee a global sequence across independent partitions.

---

# 6. How Do We Maintain Ordering for a Customer?

This is where the **message key** becomes extremely important.

Suppose you have:

```text
Customer ID = 101
```

Events:

```text
Order Created
Payment Successful
Order Shipped
Order Delivered
```

You can use:

```text
key = customer-101
```

Kafka's partitioner can consistently map that key to the same partition.

```text
customer-101
      │
      ▼
   Partition 2
      │
      ├── Order Created
      ├── Payment Successful
      ├── Order Shipped
      └── Order Delivered
```

Now those events maintain their order **within that partition**.

---

# 7. Real-World Example — Banking 💳

Imagine a bank account:

```text
Account: 12345
```

Events:

```text
1. Deposit ₹10,000
2. Withdraw ₹2,000
3. Deposit ₹5,000
4. Withdraw ₹1,000
```

Correct order:

```text
Deposit 10K
     ↓
Withdraw 2K
     ↓
Deposit 5K
     ↓
Withdraw 1K
```

If these events are distributed randomly:

```text
P0 → Deposit 10K
P1 → Withdraw 2K
P0 → Deposit 5K
P1 → Withdraw 1K
```

multiple consumers could process them independently.

That can create problems if the application requires strict sequencing.

Instead:

```text
key = account-12345
```

All events for that account can go to:

```text
P0
```

```text
P0
│
├── Deposit 10K
├── Withdraw 2K
├── Deposit 5K
└── Withdraw 1K
```

Now the sequence for that account is preserved.

---

# 8. Important: Ordering Is Usually Per Key

This is the pattern you'll see in real systems:

```text
Key = customerId
```

Then:

```text
Customer A → P0
Customer B → P1
Customer C → P2
Customer D → P0
```

You get:

```text
P0:
Customer A event 1
Customer A event 2
Customer D event 1
Customer D event 2

P1:
Customer B event 1
Customer B event 2

P2:
Customer C event 1
Customer C event 2
```

Ordering is maintained **within each partition**, so events for a given key stay ordered as long as they continue mapping to that partition.

---

# 9. But There's a Trade-Off ⚠️

Suppose:

```text
customer-101
```

generates **50% of all Kafka traffic**.

If you use:

```text
customerId
```

as the key:

```text
customer-101 → P0
```

you could get:

```text
P0 🔥 → 50% traffic

P1    → 15%
P2    → 15%
P3    → 10%
P4    → 10%
```

Now P0 is a **hot partition**.

So:

> **Ordering requirements and load distribution must be designed together.**

This is a very important Senior Platform Engineer consideration.

---

# 10. More Partitions Don't Give Global Ordering

Suppose someone says:

> "We need ordering, so let's create 20 partitions."

That does **not** give you global ordering.

You would have:

```text
P0 → ordered
P1 → ordered
P2 → ordered
...
P19 → ordered
```

But not:

```text
P0 + P1 + ... + P19
        ↓
Global order
```

If you genuinely need global ordering, you generally need to constrain the data to **one partition** for that ordering domain.

---

# 11. One Partition = Global Ordering for That Topic

If a topic has:

```text
Partitions = 1
```

then:

```text
Topic
 │
 ▼
P0
 │
 ├── Event 1
 ├── Event 2
 ├── Event 3
 └── Event 4
```

There is only one sequence.

Therefore, the topic has a single ordered stream.

### But the cost:

You sacrifice partition-level parallelism.

```text
1 partition
     ↓
Limited parallelism
     ↓
Limited scalability
```

So there is a trade-off:

```text
More partitions
     ↓
More scalability
     ↓
Less global ordering


Fewer partitions
     ↓
More ordering control
     ↓
Less parallelism
```

---

# 12. Consumer Groups and Ordering

Suppose:

```text
Topic: orders
Partitions = 3
```

Consumer group:

```text
C1
C2
C3
```

Assignment:

```text
P0 → C1
P1 → C2
P2 → C3
```

Ordering within each partition is maintained.

```text
P0 → C1
A → B → C

P1 → C2
X → Y → Z

P2 → C3
M → N → O
```

But you still don't get:

```text
A → X → M → B → Y → N...
```

as a global sequence.

---

# 13. What If Multiple Consumers Read the Same Partition?

Within a **single consumer group**, Kafka does not normally assign the same partition to multiple active consumers simultaneously.

For example:

```text
P0 → C1
```

not:

```text
P0 → C1
P0 → C2
```

This is important because it helps preserve sequential processing for that partition.

---

# 14. Ordering + Consumer Processing

There's another subtle point.

Kafka can deliver records in partition order, but your **application processing** can still break the effective business ordering if it processes records asynchronously.

Example:

```text
Kafka:

P0:
A → B → C
```

Consumer receives:

```text
A
B
C
```

But application does:

```text
Thread 1 → A → slow
Thread 2 → B → fast
Thread 3 → C → very fast
```

Processing could complete:

```text
B → C → A
```

So:

> **Kafka's partition ordering doesn't automatically guarantee that your application's downstream side effects happen in order.**

This is a very important Senior-level distinction.

---

# 15. Example: Order Processing

Kafka:

```text
P0

0 → Order Created
1 → Payment Completed
2 → Order Shipped
```

Consumer receives:

```text
0
1
2
```

But if the application sends them to asynchronous workers:

```text
Worker 1 → Created
Worker 2 → Payment
Worker 3 → Shipped
```

you could get:

```text
Payment processed
   ↓
Shipped processed
   ↓
Created processed
```

even though Kafka delivered them correctly.

Therefore, if **business ordering** is critical, the consumer architecture must preserve it too.

---

# 16. Production Scenario

### Requirement

> "All transactions for a bank account must be processed in order, but different accounts can be processed in parallel."

This is an excellent Kafka use case.

Use:

```text
key = accountId
```

Example:

```text
Account A → P0
Account B → P1
Account C → P2
Account D → P0
```

Now:

```text
P0:
Account A → Tx1 → Tx2 → Tx3
Account D → Tx1 → Tx2

P1:
Account B → Tx1 → Tx2 → Tx3

P2:
Account C → Tx1 → Tx2
```

You get:

### Within account:

```text
A:
Tx1 → Tx2 → Tx3
```

### Across accounts:

```text
A ───────────►
B ───────────►   Can process in parallel
C ───────────►
```

🔥 This is often the **ideal Kafka design**:

> **Sequential processing within a key, parallel processing across keys.**

---

# 17. Partition Ordering vs Topic Ordering

Don't say:

❌ "Kafka guarantees topic ordering."

Say:

✅ **"Kafka guarantees ordering within a partition."**

And if you need ordering for a business entity:

```text
Business entity
      ↓
Partition key
      ↓
Same partition
      ↓
Ordered events
```

For example:

```text
accountId
   ↓
partition key
   ↓
Partition 3
   ↓
Transaction sequence
```

---

# 18. Senior Platform Engineer Considerations

When an application says:

> "We need ordering."

Ask:

### 1. What needs to be ordered?

* Entire topic?
* Customer?
* Account?
* Order?
* Transaction?

### 2. What is the ordering key?

```text
customerId?
accountId?
orderId?
transactionId?
```

### 3. How much parallelism is required?

```text
1000 accounts
↓
Can they process in parallel?
```

### 4. Could one key become hot?

```text
customer-999 → 70% traffic?
```

### 5. Does consumer processing preserve ordering?

Kafka ordering alone isn't enough.

### 6. Can partition count change?

Increasing partitions can affect partition mapping and therefore ordering assumptions.

---

# 19. Interview Answer 🎯

### Question:

**"How does Kafka guarantee message ordering?"**

A strong answer:

> "Kafka guarantees ordering within an individual partition. Records in a partition are appended sequentially and assigned monotonically increasing offsets. Kafka doesn't provide ordering across multiple partitions. If the application requires ordering for a specific business entity, such as an account or customer, we normally use that entity's ID as the record key so that its events are routed to the same partition. This gives us ordering for that key while still allowing different keys to be processed in parallel."

---

# 20. Most Important Mental Model 🔥

```text
                 Kafka Topic
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
         P0         P1         P2
          │          │          │
          ▼          ▼          ▼
       Ordered    Ordered    Ordered
        Stream     Stream     Stream
          │          │          │
          ▼          ▼          ▼
         C1         C2         C3
```

### Remember:

```text
ONE PARTITION
     ↓
ONE ORDERED SEQUENCE
```

```text
MULTIPLE PARTITIONS
     ↓
MULTIPLE ORDERED SEQUENCES
     ↓
NO GLOBAL ORDER
```

And the most useful production pattern:

```text
             Account ID
                  │
                  ▼
            Partition Key
                  │
          ┌───────┴───────┐
          ▼               ▼
       Account A       Account B
          │               │
          ▼               ▼
         P0              P1
          │               │
     Tx1 → Tx2 → Tx3   Tx1 → Tx2 → Tx3
          │               │
          └───────┬───────┘
                  ▼
          Parallel processing
```

> **Same key → same partition → ordered events for that key. Different keys → potentially different partitions → parallelism.**
