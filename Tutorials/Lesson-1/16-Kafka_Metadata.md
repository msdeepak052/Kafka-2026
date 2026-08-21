# 16. Kafka Metadata

This comes naturally after **Leader & Follower** because now we need to answer:

> **How does a Producer or Consumer know which broker is responsible for a particular partition?**

The answer is **Kafka metadata**.

---

# 1. What is Kafka Metadata?

**Metadata is information about the Kafka cluster, topics, partitions, and their current locations/roles.**

Think of it as a **map of the Kafka cluster**.

It tells a Kafka client things such as:

```text
Topic
  ↓
Partitions
  ↓
Which brokers host those partitions?
  ↓
Who is the leader for each partition?
```

### Simple definition

> **Kafka metadata is information that tells clients how the Kafka cluster is currently organized and where they should send/read data.**

---
A **real Kafka metadata response is structured data**, but the exact wire-format is more complex than what we need right now. For your current learning stage, this is a good **simplified representation** of what Kafka metadata effectively tells a client.

### Sample Kafka Metadata

Suppose we have:

```text
Kafka Cluster

Broker 1 → 10.0.1.10:9092
Broker 2 → 10.0.1.11:9092
Broker 3 → 10.0.1.12:9092
```

Topic:

```text
orders
```

3 partitions, replication factor 3.

A simplified metadata response could look like:

```json
{
  "brokers": [
    {
      "nodeId": 1,
      "host": "10.0.1.10",
      "port": 9092
    },
    {
      "nodeId": 2,
      "host": "10.0.1.11",
      "port": 9092
    },
    {
      "nodeId": 3,
      "host": "10.0.1.12",
      "port": 9092
    }
  ],

  "topics": [
    {
      "name": "orders",

      "partitions": [
        {
          "partition": 0,
          "leader": 1,
          "replicas": [1, 2, 3]
        },
        {
          "partition": 1,
          "leader": 2,
          "replicas": [2, 3, 1]
        },
        {
          "partition": 2,
          "leader": 3,
          "replicas": [3, 1, 2]
        }
      ]
    }
  ]
}
```

**This is simplified/illustrative, not the exact Kafka wire-protocol JSON.**

---

## How to Read It

### Brokers

```json
{
  "nodeId": 1,
  "host": "10.0.1.10",
  "port": 9092
}
```

Means:

```text
Broker ID = 1
Address   = 10.0.1.10
Port      = 9092
```

---

### Partition 0

```json
{
  "partition": 0,
  "leader": 1,
  "replicas": [1, 2, 3]
}
```

Means:

```text
Topic: orders
Partition: P0

Leader:
    Broker 1

Replicas:
    Broker 1
    Broker 2
    Broker 3
```

Architecture:

```text
             orders / P0
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
       B1        B2        B3
     LEADER    FOLLOWER   FOLLOWER
```

---

### Partition 1

```json
{
  "partition": 1,
  "leader": 2,
  "replicas": [2, 3, 1]
}
```

Means:

```text
P1
│
├── B2 → LEADER
├── B3 → FOLLOWER
└── B1 → FOLLOWER
```

Notice something important:

```text
B1 = Leader for P0
B1 = Follower for P1
```

So **leader/follower is per partition**, exactly as we discussed.

---

### Partition 2

```text
P2

Leader:
    B3

Replicas:
    B3, B1, B2
```

---

# What the Producer Gets From This

Suppose producer wants to send:

```text
Topic = orders
Partition = 1
```

It looks at metadata:

```text
P1 → Leader = Broker 2
```

Therefore:

```text
Producer
    │
    │ Produce Request
    ▼
Broker 2
    │
    ▼
orders / P1
```

That's the practical purpose of the metadata.

---

# Visualize the Same Metadata

```text
                    KAFKA METADATA
                         │
          ┌──────────────┴──────────────┐
          │                             │
        BROKERS                       TOPIC
          │                             │
   ┌──────┼──────┐                    orders
   ▼      ▼      ▼                      │
  B1     B2     B3              ┌──────┼──────┐
  │      │      │                ▼      ▼      ▼
10.0   10.0   10.0              P0     P1     P2
.10    .11    .12                │      │      │
                                  │      │      │
                                B1-L   B2-L   B3-L
                                  │      │      │
                               B2,B3   B3,B1   B1,B2
                               replicas replicas replicas
```

### The easiest way to remember Kafka metadata:

```text
Metadata
   │
   ├── Who are the brokers?
   │
   ├── What partitions exist?
   │
   ├── Where are those partitions?
   │
   ├── Who is the leader?
   │
   └── What replicas exist?
```

So when you hear **"Kafka metadata"**, immediately think:

> **"The map that tells the Kafka client where the partition is and which broker is currently its leader."**

---
# 2. Easy Analogy 🗺️

Imagine you're delivering a package in a huge city.

You know:

```text
City
 ↓
Building
 ↓
Floor
 ↓
Room
```

But you don't know where the room is.

You need a **map/directory**:

```text
Order Department → Building A → Floor 3 → Room 301
Payment Department → Building B → Floor 2 → Room 205
```

Kafka metadata works similarly:

```text
Topic
 ↓
Partition
 ↓
Broker
 ↓
Leader
```

So a producer doesn't blindly send data to any broker.

It can use metadata to determine:

> **"Which broker is currently the leader for the partition I need?"**

---

# 3. Why Does Kafka Need Metadata?

Suppose your cluster has:

```text
Broker 1
Broker 2
Broker 3
```

Topic:

```text
orders
```

Partitions:

```text
P0
P1
P2
```

And:

```text
P0 → Leader = Broker 1
P1 → Leader = Broker 2
P2 → Leader = Broker 3
```

A producer wants to send an event to P1.

It needs to know:

```text
P1 → Broker 2
```

That's metadata.

```text
Producer
   │
   │ Metadata
   ▼
Kafka Cluster
   │
   └── P1 → Leader = Broker 2
                 │
                 ▼
             Produce Request
```

---

# 4. What Information Does Metadata Contain?

At your current learning stage, focus on these:

### Topic information

```text
orders
payments
inventory
```

### Partition information

```text
orders → P0, P1, P2
```

### Partition leader

```text
P0 → Broker 1
P1 → Broker 2
P2 → Broker 3
```

### Broker information

```text
Broker 1
Broker 2
Broker 3
```

### Replica information

Because you've just learned replication:

```text
P0 → B1, B2, B3
```

So conceptually:

```text
Metadata
   │
   ├── Topics
   ├── Partitions
   ├── Leaders
   ├── Replicas
   └── Brokers
```

---

# 5. Example Metadata

Imagine Kafka has:

```text
Topic: orders

Partitions: 3

Replication Factor: 3
```

Metadata might conceptually tell the producer:

```text
Topic: orders

Partition 0:
    Leader  → Broker 1
    Replicas → Broker 1, Broker 2, Broker 3

Partition 1:
    Leader  → Broker 2
    Replicas → Broker 2, Broker 3, Broker 1

Partition 2:
    Leader  → Broker 3
    Replicas → Broker 3, Broker 1, Broker 2
```

Think of this as Kafka's **routing map**.

---

# 6. How Producer Uses Metadata

This is the most important part.

Suppose:

```text
Topic = orders
Key = customer-123
```

Producer needs to determine the partition.

Conceptually:

```text
Producer
   │
   ├── Topic = orders
   ├── Key = customer-123
   │
   ▼
Partition selection
   │
   ▼
P1
```

Now the producer needs to know:

> **Who is the leader of P1?**

Metadata tells it:

```text
P1 → Broker 2
```

So:

```text
Producer
   │
   │ Produce Request
   ▼
Broker 2
   │
   ▼
P1
```

---

# 7. Producer Does NOT Just Send Everything to Bootstrap Broker

This is an important point.

Suppose your producer configuration says:

```text
bootstrap.servers =
broker1:9092
broker2:9092
broker3:9092
```

You might think:

> "I'll send every message to Broker 1."

❌ That's not how you should think about it.

Bootstrap servers are initially used to connect to the Kafka cluster and obtain metadata.

Then the producer can determine:

```text
P0 → Broker 1
P1 → Broker 2
P2 → Broker 3
```

and send requests to the appropriate leader.

```text
                  Producer
                     │
               Bootstrap
                     │
                     ▼
                Metadata
                     │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
       P0           P1           P2
        │            │            │
       B1           B2           B3
     Leader       Leader       Leader
```

---

# 8. What Happens If Leadership Changes?

This is where metadata becomes **very important**.

We just learned:

```text
P0

B1 → Leader
B2 → Follower
B3 → Follower
```

Suppose B1 fails:

```text
B1 ❌
```

Kafka changes the leadership:

```text
P0

B1 → ❌
B2 → NEW LEADER
B3 → Follower
```

But the producer may still have old metadata:

```text
Producer thinks:

P0 → B1
```

That's now incorrect.

---

# 9. Producer Refreshes Metadata

The producer discovers that its metadata is outdated.

Conceptually:

```text
Old Metadata

P0 → B1
      ↓
     ❌
```

Kafka now says:

```text
New Metadata

P0 → B2
```

The producer updates its metadata.

Then:

```text
Producer
   │
   ▼
New Metadata
   │
   ▼
P0 → B2
   │
   ▼
Produce Request
```

This is one reason metadata is essential in a distributed Kafka cluster.

---

# 10. Easy Analogy 🗺️

Imagine Google Maps.

You want to reach:

```text
Restaurant
```

Your map says:

```text
Road A → Open
```

But suddenly Road A is closed.

Your navigation system gets updated:

```text
Old route ❌
New route → Road B
```

Kafka metadata is somewhat similar:

```text
Old metadata:
P0 → Broker 1

Broker 1 fails

New metadata:
P0 → Broker 2
```

The client needs the **latest cluster map**.

---

# 11. Consumer Also Uses Metadata

Metadata isn't only for producers.

A consumer also needs to know where partitions are and which broker is currently responsible for them.

Suppose:

```text
Topic: orders

P0 → Leader B1
P1 → Leader B2
P2 → Leader B3
```

Consumer needs P1.

Metadata tells it:

```text
P1 → B2
```

So the consumer communicates with the appropriate broker.

Conceptually:

```text
Consumer
   │
   │ Metadata
   ▼
Kafka
   │
   │ P1 → B2
   ▼
Broker 2
   │
   ▼
P1
   │
   ▼
Records
```

---

# 12. Producer vs Consumer Metadata Usage

### Producer

Uses metadata to determine:

```text
Which partition?
      ↓
Which broker is leader?
      ↓
Send request
```

### Consumer

Uses metadata to determine:

```text
Which partition?
      ↓
Which broker is responsible?
      ↓
Fetch records
```

So:

```text
             Metadata
                 │
        ┌────────┴────────┐
        ▼                 ▼
     Producer          Consumer
        │                 │
   Find leader       Find partition
        │                 │
        ▼                 ▼
      Write             Read
```

---

# 13. Metadata and Partition Replication

Let's connect everything you've learned.

Suppose:

```text
Topic: orders
Partition: P0
Replication Factor: 3
```

Metadata can describe:

```text
P0

Leader:
B1

Replicas:
B1, B2, B3
```

So:

```text
                 P0
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
       B1        B2        B3
     Leader    Follower   Follower
```

The client needs to know the **leader** to send normal partition requests.

---

# 14. Metadata Is Not the Actual Data

This distinction is important.

Metadata doesn't mean:

```text
❌ Metadata contains Order 1001
```

Instead:

```text
Metadata:
P0 → Leader B1
```

Actual data:

```text
B1 / P0:

Offset 0 → Order 1001
Offset 1 → Order 1002
```

So:

```text
Metadata = Map 🗺️
Data = Actual records 📦
```

---

# 15. Example From Start to Finish

Let's follow a producer.

### Cluster

```text
B1
B2
B3
```

### Topic

```text
orders
```

### Partitions

```text
P0
P1
P2
```

### Leaders

```text
P0 → B1
P1 → B2
P2 → B3
```

---

### Step 1 — Producer starts

```text
Producer
   ↓
Bootstrap server
```

---

### Step 2 — Producer receives metadata

```text
P0 → B1
P1 → B2
P2 → B3
```

---

### Step 3 — Application creates event

```text
Order 1001
```

---

### Step 4 — Producer determines partition

```text
Order 1001
     ↓
    P1
```

---

### Step 5 — Producer looks at metadata

```text
P1 → B2
```

---

### Step 6 — Producer sends request

```text
Producer
    │
    ▼
B2
LEADER
    │
    ▼
P1
    │
    ▼
Order 1001
```

---

# 16. What If Metadata Is Stale?

This is a very important production scenario.

Suppose producer has:

```text
P1 → B2
```

But leadership changed:

```text
P1 → B3
```

Producer may initially try using its old information.

Kafka can indicate that the request needs to be redirected/retried based on the current cluster state, and the client refreshes metadata.

Conceptually:

```text
Producer
   │
   │ Old metadata
   ▼
B2
   │
   X
   │
   ▼
Refresh Metadata
   │
   ▼
P1 → B3
   │
   ▼
B3
   │
   ▼
Success
```

You don't need to memorize the exact protocol error yet.

Just remember:

> **Kafka clients maintain metadata and refresh it when the cluster topology or partition leadership changes.**

---

# 17. Why Metadata Is Critical for a Platform Engineer

Imagine a production cluster:

```text
100 Brokers
10,000 Partitions
```

A producer cannot manually know:

```text
Which broker?
Which partition?
Which leader?
```

for every request.

Kafka metadata provides the information needed for clients to route requests correctly.

So mentally:

```text
Kafka Cluster
      │
      ▼
   Metadata
      │
      ├── Topic
      ├── Partition
      ├── Leader
      ├── Replica
      └── Broker
```

---

# 18. The Complete Architecture 🔥

Now connect **everything you've learned so far**:

```text
                         Kafka Cluster
                              │
                         Metadata
                              │
           ┌──────────────────┼──────────────────┐
           ▼                  ▼                  ▼
          P0                 P1                 P2
           │                  │                  │
      Leader B1          Leader B2          Leader B3
           │                  │                  │
       Followers          Followers          Followers
        B2, B3             B1, B3             B1, B2
           ▲                  ▲                  ▲
           │                  │                  │
        Producer           Producer           Consumer
        requests           requests            reads
```

The key relationship is:

```text
Metadata
   ↓
Where is the partition?
   ↓
Who is its leader?
   ↓
Client knows where to send the request
```

---

# 19. 🔥 What You Must Remember

### Kafka Metadata

> **The cluster's routing/map information.**

It tells clients about:

* Brokers
* Topics
* Partitions
* Partition leaders
* Partition replicas

### Producer

```text
Metadata
   ↓
Find partition leader
   ↓
Send Produce Request
```

### Consumer

```text
Metadata
   ↓
Find relevant partition/broker
   ↓
Fetch records
```

### Leadership changes

```text
Old:
P0 → B1 Leader

B1 fails

New:
P0 → B2 Leader

        ↓
Metadata refreshed
        ↓
Clients learn new location
```

---

# 🎯 Interview Answer

If asked:

> **"What is Kafka metadata and why is it required?"**

Say:

> "Kafka metadata is information about the cluster topology, including brokers, topics, partitions, partition leaders and replicas. Kafka clients such as producers and consumers use this metadata to determine where a partition is located and which broker is currently its leader. For example, a producer uses metadata to determine the leader for a target partition and sends the produce request there. If partition leadership changes because of a broker failure, the client refreshes its metadata and learns the new leader."

### Mental shortcut:

```text
Metadata = Kafka's MAP 🗺️

Topic → Partition → Leader Broker → Request
```

And this is the key connection to your previous topic:

```text
Replication
     ↓
Leader + Followers
     ↓
Leadership can change
     ↓
Metadata tells clients
where the current leader is
```

I would **not jump into controller, KRaft, metadata quorum, or metadata internals yet**—those are separate concepts and should stay in their proper chronology.
