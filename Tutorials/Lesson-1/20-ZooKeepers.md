Absolutely. Since you're learning Kafka from a **Senior Platform Engineer perspective**, I'll explain ZooKeeper in the context of **what problem Kafka had, why ZooKeeper was introduced, what exactly ZooKeeper did, a real production case study, its limitations, and finally why KRaft replaced it**.

I’ll also distinguish **old Kafka/ZooKeeper architecture** from **modern Kafka/KRaft architecture**, so you don't mix the two.

---

# Kafka ZooKeeper — Complete Notes

## 1. First: What is ZooKeeper?

**Apache ZooKeeper** is a distributed coordination system.

In the old Kafka architecture, Kafka used ZooKeeper primarily for **cluster metadata and coordination**.

Think of it as:

> **ZooKeeper = the external coordination/metadata system that helped Kafka brokers agree on the state of the Kafka cluster.**

It was **not where Kafka's actual messages were stored**.

### Very important distinction

```text
                    Kafka Cluster
                         │
          ┌──────────────┴──────────────┐
          │                             │
       Kafka Data                  ZooKeeper
          │                             │
          ▼                             ▼
 Topics / Partitions              Cluster Metadata
 Messages                         Broker registration
 Replicas                          Controller election
 Offsets/data logs                Cluster configuration
```

So:

> **Kafka stored the actual event data. ZooKeeper stored/managed important cluster coordination metadata.**

Older Kafka documentation explicitly describes Kafka using ZooKeeper and requiring ZooKeeper to be started before the Kafka broker. ([Apache Kafka][1])

---

# 2. Why Did Kafka Need ZooKeeper?

This is the most important question.

Imagine you have:

```text
Kafka Cluster

Broker 1
Broker 2
Broker 3
Broker 4
Broker 5
```

Now ask:

### Who knows which brokers are alive?

```text
Broker 1 ?
Broker 2 ?
Broker 3 ?
...
```

### Who decides which broker is the controller?

```text
Broker 1
Broker 2
Broker 3
```

Only **one controller** should be active.

### Who maintains important cluster metadata?

For example:

```text
orders topic
 ├── P0 → Broker 1
 ├── P1 → Broker 2
 └── P2 → Broker 3
```

### What happens if Broker 1 dies?

Who coordinates:

```text
P0
 ↓
New leader?
```

Kafka needed a reliable distributed coordination mechanism.

That was one of the major roles ZooKeeper provided.

---

# 3. Easy Analogy — Company Headquarters 🏢

Imagine Kafka brokers are different branch offices:

```text
Branch 1
Branch 2
Branch 3
Branch 4
```

Each branch handles actual business.

But you need a **central coordination office** that knows:

```text
Who is alive?
Who is the current coordinator?
Which branch owns which responsibility?
What is the current cluster configuration?
```

That coordination office is analogous to **ZooKeeper** in the old architecture.

```text
              ZooKeeper
             /    |    \
            /     |     \
          B1      B2     B3
```

The brokers still perform the actual Kafka work.

---

# 4. What Exactly Did ZooKeeper Do for Kafka?

For your Senior Platform Engineer understanding, remember these major responsibilities.

### 1. Broker registration

ZooKeeper helped Kafka know which brokers were part of the cluster.

Conceptually:

```text
ZooKeeper

Broker 1 → alive
Broker 2 → alive
Broker 3 → alive
```

When a broker started, it registered itself.

When it disappeared, the cluster could detect that change.

---

### 2. Controller election

Kafka needed one broker to act as the **active controller**.

For example:

```text
Broker 1
Broker 2
Broker 3
```

ZooKeeper helped coordinate the election.

```text
Broker 1 → Controller
Broker 2 → Broker
Broker 3 → Broker
```

If Broker 1 failed:

```text
Broker 1 💥
     ↓
ZooKeeper detects change
     ↓
New controller elected
     ↓
Broker 2
```

---

### 3. Cluster metadata

ZooKeeper was used to store Kafka cluster metadata.

Examples included information related to:

```text
Topics
Partitions
Broker information
Partition assignments
Controller information
Configuration
ACL-related information in older Kafka setups
```

Kafka's older architecture documentation shows ZooKeeper being used for cluster metadata and coordination. ([Apache Kafka][2])

---

### 4. Partition leadership coordination

Suppose:

```text
orders-P0

Leader:
Broker 1

Followers:
Broker 2
Broker 3
```

If Broker 1 dies:

```text
Broker 1 💥
```

Kafka needs to coordinate the leadership change:

```text
Broker 2
    ↓
New Leader
```

ZooKeeper was part of the old coordination architecture involved in controller/partition state management.

---

# 5. ZooKeeper Was NOT the Data Store

This is extremely important.

Suppose:

```text
Topic: orders
Partition: P0
```

Actual records:

```text
Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
Offset 3 → Order D
```

These are stored by **Kafka brokers**.

Not:

```text
❌ ZooKeeper
```

Instead:

```text
                    Kafka
                     │
             ┌───────┴───────┐
             ▼               ▼
          Broker 1        Broker 2
             │               │
          Kafka Logs      Kafka Logs
```

ZooKeeper was dealing with **coordination/metadata**, not Kafka's high-volume event log.

---

# 6. Old Kafka Architecture

The classic architecture looked like:

```text
                         Producers
                             │
                             ▼
                    ┌─────────────────┐
                    │ Kafka Brokers   │
                    │                 │
                    │ B1  B2  B3     │
                    └────────┬────────┘
                             │
                             │ Metadata /
                             │ Coordination
                             ▼
                    ┌─────────────────┐
                    │   ZooKeeper     │
                    │                 │
                    │ ZK1 ZK2 ZK3     │
                    └─────────────────┘
```

And consumers interacted with Kafka brokers:

```text
Producer
   │
   ▼
Kafka Broker
   │
   ▼
Kafka Topic
   │
   ▼
Consumer
```

ZooKeeper wasn't sitting in the normal producer → broker → consumer data path.

---

# 7. Why Was ZooKeeper a Good Choice Originally?

This is important.

Don't think:

> "ZooKeeper was badly designed."

That's not true.

ZooKeeper was actually very useful for Kafka's original architecture.

It provided:

### Distributed coordination

Multiple Kafka brokers could coordinate reliably.

### Failure detection

The cluster could detect broker/session failures.

### Leader/controller election

It provided a coordination mechanism for selecting the Kafka controller.

### Strong consistency for coordination state

ZooKeeper was designed for distributed coordination where nodes need a consistent view of shared state.

### Mature distributed system

ZooKeeper was already widely used in distributed systems.

Kafka's own historical design documentation explicitly discusses ZooKeeper as a common quorum/coordination system. ([Apache Kafka][3])

---

# 8. Real Case Study — E-Commerce Kafka Cluster 🛒

Let's build a realistic example.

Suppose an e-commerce company has:

```text
Kafka Cluster

Broker 1
Broker 2
Broker 3
```

Topic:

```text
order-events
```

Partitions:

```text
P0
P1
P2
```

Replication factor:

```text
3
```

Conceptually:

```text
P0 → B1 Leader
      B2 Replica
      B3 Replica

P1 → B2 Leader
      B3 Replica
      B1 Replica

P2 → B3 Leader
      B1 Replica
      B2 Replica
```

---

# 9. Producer Sends an Order

Application sends:

```json
{
  "orderId": "ORD-1001",
  "status": "CREATED"
}
```

Producer needs metadata such as:

```text
order-events
P0
P1
P2
```

and information about where the partition leaders are.

It eventually sends the record to the appropriate broker.

For example:

```text
ORD-1001
    ↓
P1
    ↓
B2
```

where B2 is currently the leader of P1.

---

# 10. Now Broker 2 Dies 💥

Suppose:

```text
B2 💥
```

B2 was:

```text
P1 Leader
```

Now Kafka needs to react.

The old architecture involved:

```text
B2 failure
    ↓
ZooKeeper / controller coordination
    ↓
Controller determines new leadership
    ↓
B3 becomes leader for P1
```

Conceptually:

```text
Before:

P1
 │
 └── Leader → B2 ❌


After:

P1
 │
 └── Leader → B3 ✅
```

Now producers/consumers can be directed toward the new leader.

---

# 11. What Problem Did ZooKeeper Solve Here?

Without a coordination mechanism, imagine:

```text
B1 says:
"I think I'm controller."

B3 says:
"I think I'm controller."

B4 says:
"I think I'm controller."
```

You don't want multiple brokers independently making conflicting cluster decisions.

You need a **consistent coordination mechanism**.

ZooKeeper helped provide that.

---

# 12. Another Example — Broker Joins the Cluster

Suppose:

```text
Existing:

B1
B2
B3
```

You add:

```text
B4
```

The cluster needs to know:

```text
"Broker 4 exists."
```

Old architecture:

```text
B4 starts
  ↓
Registers with ZooKeeper
  ↓
Cluster/controller becomes aware
  ↓
Metadata updated
```

This is part of why ZooKeeper was important operationally.

---

# 13. ZooKeeper Ensemble

You generally wouldn't want:

```text
ZooKeeper 1
```

because one node is a single point of failure.

Instead:

```text
ZooKeeper Ensemble

ZK1
ZK2
ZK3
```

or:

```text
ZK1
ZK2
ZK3
ZK4
ZK5
```

ZooKeeper uses a quorum model.

For example:

```text
3 ZooKeeper nodes

ZK1
ZK2
ZK3
```

A majority is:

```text
2
```

So the ensemble can tolerate one node failure.

With five:

```text
ZK1
ZK2
ZK3
ZK4
ZK5
```

majority:

```text
3
```

and it can tolerate two failures.

---

# 14. The Operational Problem Appears

Now look at your infrastructure:

```text
Kafka Cluster
├── B1
├── B2
└── B3

ZooKeeper Cluster
├── ZK1
├── ZK2
└── ZK3
```

You've now got **two distributed systems to operate**.

As a Platform Engineer, this matters.

You need:

```text
Kafka monitoring
Kafka upgrades
Kafka networking
Kafka storage
Kafka security

+

ZooKeeper monitoring
ZooKeeper upgrades
ZooKeeper networking
ZooKeeper storage
ZooKeeper security
ZooKeeper quorum health
```

---

# 15. This Became a Major Problem as Kafka Grew

Kafka itself became increasingly sophisticated.

Clusters became:

```text
100s of brokers
1000s of topics
100,000s / millions of partitions
```

The amount of cluster metadata grew significantly.

Now you have:

```text
Kafka
   │
   └── External metadata system
             │
             └── ZooKeeper
```

This created architectural complexity.

---

# 16. Problem #1 — Two Systems to Manage

Old architecture:

```text
             Kafka
               │
               ▼
          ZooKeeper
```

Platform team has to operate both.

KRaft's goal is:

```text
             Kafka
               │
               ▼
        Kafka Controllers
               │
               ▼
       Kafka metadata log
```

Kafka's own 4.0 release announcement highlights that removing ZooKeeper eliminates the complexity of maintaining a separate ZooKeeper ensemble and reduces operational overhead. ([Apache Kafka][4])

---

# 17. Problem #2 — Metadata Scalability

Kafka's data plane scales extremely well.

But the metadata architecture had an external dependency:

```text
Kafka Brokers
      │
      ▼
ZooKeeper
```

As Kafka clusters became larger, Kafka needed a more native way to manage metadata.

The fundamental idea behind KRaft was:

> **Make Kafka itself responsible for its metadata using a replicated metadata log.**

---

# 18. Problem #3 — Kafka Was Dependent on an External System

Think about the architecture:

```text
Kafka
 │
 ├── Data
 │
 └── Depends on ZooKeeper
```

You install Kafka, but you also need:

```text
ZooKeeper
```

So Kafka wasn't completely self-contained.

With KRaft:

```text
Kafka
 │
 ├── Data
 │
 └── Metadata
```

Everything is managed within the Kafka architecture.

---

# 19. Problem #4 — Different Operational Lifecycles

You now have:

```text
Kafka version
ZooKeeper version
```

You have compatibility considerations.

You have to manage:

```text
Kafka upgrade
+
ZooKeeper upgrade
```

and ensure the whole system remains healthy.

That's extra operational complexity.

---

# 20. Problem #5 — Controller Architecture

The old architecture roughly looked like:

```text
Kafka Brokers
     │
     ▼
ZooKeeper
     │
     ▼
Controller Election
```

Kafka's controller state and ZooKeeper state were separate concepts.

KRaft's goal was to bring the metadata consensus mechanism **inside Kafka**.

---

# 21. Enter KRaft

KRaft stands for:

> **Kafka Raft**

It uses the **Raft consensus protocol** for Kafka's metadata quorum.

Instead of:

```text
Kafka
  ↓
ZooKeeper
  ↓
Metadata coordination
```

we now have:

```text
Kafka Controllers
       │
       ▼
Kafka Metadata Log
       │
       ▼
Raft Quorum
```

---

# 22. KRaft Architecture

Modern Kafka:

```text
                       Kafka Cluster
                            │
             ┌──────────────┴──────────────┐
             │                             │
             ▼                             ▼
        Kafka Brokers                 Controllers
             │                             │
             │                             ▼
             │                     Metadata Quorum
             │                             │
             │                       Raft consensus
             │                             │
             └──────────────┬──────────────┘
                            │
                            ▼
                     Kafka Metadata
```

The controllers store cluster metadata in the Kafka metadata log. Current Kafka documentation describes controllers as storing cluster metadata in memory and on disk, with a controller quorum providing redundancy. ([Apache Kafka][5])

---

# 23. What Is the Kafka Metadata Log?

This is one of the most important KRaft concepts.

Instead of relying on:

```text
ZooKeeper metadata
```

Kafka maintains a special **metadata log**.

Conceptually:

```text
Kafka Metadata Log

Offset 0 → Broker registered
Offset 1 → Topic created
Offset 2 → Partition assignment changed
Offset 3 → Broker removed
Offset 4 → Configuration changed
```

The metadata log is replicated across the controller quorum.

---

# 24. Easy Analogy 📖

Imagine the Kafka cluster has a master notebook:

```text
Cluster Notebook

Entry 1:
Broker 1 joined

Entry 2:
Broker 2 joined

Entry 3:
Topic orders created

Entry 4:
orders-P0 leader = Broker 2
```

Instead of keeping this notebook in a separate external system, Kafka itself maintains a **replicated metadata log**.

That is the core KRaft idea.

---

# 25. KRaft Controllers

Suppose:

```text
Controller 1
Controller 2
Controller 3
```

They form a quorum.

Conceptually:

```text
              Controller 1
                  │
                  │
             Metadata Log
              /         \
             /           \
       Controller 2   Controller 3
```

One controller acts as the quorum leader.

The others replicate the metadata log.

If the leader fails:

```text
Controller 1 💥
      ↓
Raft election
      ↓
Controller 2 becomes leader
```

This is the fundamental difference:

```text
OLD:

Kafka → ZooKeeper
        ↓
    Coordination


NEW:

Kafka Controllers
        ↓
   Raft quorum
        ↓
Metadata log
```

---

# 26. ZooKeeper vs KRaft

| Area                   | ZooKeeper Architecture  | KRaft Architecture      |
| ---------------------- | ----------------------- | ----------------------- |
| Metadata               | ZooKeeper               | Kafka metadata log      |
| Consensus/coordination | External ZooKeeper      | Kafka's Raft quorum     |
| Controllers            | Kafka broker controller | KRaft controller quorum |
| Extra system           | ZooKeeper required      | No ZooKeeper            |
| Deployment             | Kafka + ZK              | Kafka                   |
| Metadata management    | External                | Native to Kafka         |
| Operational complexity | Higher                  | Lower                   |
| Modern Kafka           | Legacy                  | Current architecture    |

---

# 27. Why Was ZooKeeper Called "Legacy"?

Important wording:

> **ZooKeeper was not replaced because it was bad. It was replaced because Kafka evolved toward a simpler, Kafka-native metadata architecture.**

Kafka officially marked ZooKeeper as deprecated in **Kafka 3.5** and planned its removal in Kafka 4.0. ([Apache Kafka][6])

Then:

### Kafka 3.3

KRaft became production-ready for new clusters. ([Apache Kafka][7])

### Kafka 3.5

ZooKeeper was officially deprecated. ([Apache Kafka][8])

### Kafka 3.9

Final major 3.x release supporting ZooKeeper mode. ([Apache Kafka][9])

### Kafka 4.0 — March 2025

ZooKeeper support was removed.

Kafka 4.0 runs entirely without ZooKeeper and operates in KRaft mode. ([Apache Kafka][4])

So for your interview:

```text
Kafka 3.3
   ↓
KRaft production-ready

Kafka 3.5
   ↓
ZooKeeper deprecated

Kafka 3.9
   ↓
Last 3.x with ZooKeeper

Kafka 4.0
   ↓
ZooKeeper removed
   ↓
KRaft only
```

---

# 28. Important: KRaft Didn't Just "Replace ZooKeeper With Another ZooKeeper"

This is a common misunderstanding.

Don't think:

```text
ZooKeeper
    ↓
KRaft ZooKeeper
```

❌ Wrong.

Instead:

```text
OLD

Kafka
  ↓
ZooKeeper
  ↓
Metadata coordination
```

becomes:

```text
NEW

Kafka Controllers
  ↓
Raft consensus
  ↓
Kafka Metadata Log
```

KRaft is a **Kafka-native metadata quorum architecture**.

---

# 29. Real Case Study — Before KRaft

Let's say your company runs:

```text
E-commerce Kafka

Kafka:
B1
B2
B3
B4
B5

ZooKeeper:
ZK1
ZK2
ZK3
```

Platform team must operate:

```text
Kafka
 ├── brokers
 ├── disks
 ├── networking
 ├── partitions
 └── replication

ZooKeeper
 ├── quorum
 ├── disks
 ├── networking
 ├── sessions
 └── health
```

Now a ZooKeeper quorum problem occurs:

```text
ZK1 💥
ZK2 💥
ZK3
```

Majority is lost.

You now have a coordination problem even though:

```text
Kafka B1 → healthy
Kafka B2 → healthy
Kafka B3 → healthy
Kafka B4 → healthy
Kafka B5 → healthy
```

Your Kafka data brokers may be healthy, but the **external coordination layer** has become a problem.

That's an important operational lesson.

---

# 30. The Same Case With KRaft

Now:

```text
Kafka

Controllers:
C1
C2
C3

Brokers:
B1
B2
B3
B4
B5
```

Metadata quorum:

```text
C1
C2
C3
```

Suppose:

```text
C1 💥
```

Remaining:

```text
C2
C3
```

Majority:

```text
2 / 3
```

The quorum can continue.

No separate ZooKeeper cluster exists.

---

# 31. Senior Platform Engineer View

This is where you should think differently from a normal Kafka developer.

A developer might remember:

> "ZooKeeper stores Kafka metadata."

You should understand:

```text
WHY?
 ↓
Distributed coordination
 ↓
Broker membership
 ↓
Controller election
 ↓
Cluster metadata
 ↓
Partition leadership coordination
```

Then:

```text
WHY REMOVE IT?
 ↓
External dependency
 ↓
Two systems to operate
 ↓
Metadata scalability/architecture limitations
 ↓
Kafka wanted native consensus
 ↓
KRaft
```

---

# 32. The Architecture Evolution

Memorize this diagram:

```text
                 OLD KAFKA
                    │
       ┌────────────┴────────────┐
       │                         │
       ▼                         ▼
 Kafka Brokers              ZooKeeper
       │                         │
       │                    Coordination
       │                    Metadata
       │                    Controller
       │                    Election
       │
       ▼
Actual Kafka Data
```

Then:

```text
                 MODERN KAFKA
                      │
          ┌───────────┴───────────┐
          │                       │
          ▼                       ▼
     Kafka Brokers          KRaft Controllers
          │                       │
          │                       ▼
          │                  Raft Quorum
          │                       │
          │                       ▼
          │                Metadata Log
          │
          ▼
      Kafka Data
```

---

# 33. What Exactly Moved From ZooKeeper to KRaft?

Think in terms of responsibility:

```text
OLD                         NEW

ZooKeeper                   KRaft Controller Quorum
    │                              │
    ├── Metadata                   ├── Metadata
    ├── Coordination               ├── Coordination
    ├── Controller election        ├── Controller election
    └── Cluster state              └── Cluster state
```

But the implementation is fundamentally different.

KRaft uses a **Raft-based metadata quorum** rather than ZooKeeper.

---

# 34. One Very Important Distinction

Don't say in an interview:

> ❌ "ZooKeeper stored Kafka messages."

Correct:

> ✅ "ZooKeeper was used by older Kafka architectures for cluster coordination and metadata; Kafka brokers stored the actual topic/partition data."

Don't say:

> ❌ "KRaft stores Kafka messages."

Correct:

> ✅ "KRaft provides Kafka's internal metadata quorum and consensus mechanism; Kafka brokers still store the actual topic data."

---

# 35. What Does a Platform Engineer Need to Know Today?

If you're deploying **modern Kafka**, you should primarily understand:

```text
KRaft
 ├── Controllers
 ├── Controller quorum
 ├── Raft
 ├── Metadata log
 ├── Controller leader
 ├── Broker ↔ controller communication
 └── Metadata replication
```

ZooKeeper knowledge is still useful because:

* You'll encounter older Kafka clusters.
* Many production environments historically used ZooKeeper.
* Migration from ZooKeeper to KRaft is an operational task.
* Interviews may ask why Kafka moved away from ZooKeeper.

---

# 36. Migration: ZooKeeper → KRaft

For existing clusters, migration isn't simply:

```text
Stop ZooKeeper
Start KRaft
```

❌ Don't think this.

Kafka provides a migration process.

Conceptually:

```text
OLD

Kafka Brokers
      │
      ▼
 ZooKeeper
```

↓

```text
Migration
```

↓

```text
Kafka Brokers
      │
      ▼
KRaft Controller Quorum
      │
      ▼
Metadata Log
```

Kafka 3.9 was the final and best 3.x release for the ZooKeeper-to-KRaft migration path, and Kafka 4.0 removed ZooKeeper mode entirely. ([Apache Kafka][9])

---

# 37. Migration Conceptual Phases

You don't need to memorize the commands yet.

Understand the idea:

```text
1. ZK Mode
      ↓
2. KRaft metadata loaded from ZK
      ↓
3. Hybrid phase
      ↓
4. Metadata dual-write
      ↓
5. Migration finalized
      ↓
6. ZooKeeper no longer used
```

Kafka's migration documentation describes these phases, including the initial ZooKeeper-based phase, KRaft metadata loading, hybrid operation, dual-write, and finalization. ([Apache Kafka][10])

---

# 38. One Real Interview Case

### Interviewer:

> "You have a Kafka cluster with 50 brokers and 3 ZooKeeper nodes. One ZooKeeper node goes down. What happens?"

Your answer:

> "A single ZooKeeper node failure doesn't necessarily impact the cluster if the remaining ZooKeeper ensemble still has quorum. For a three-node ensemble, two nodes constitute a majority. However, I'd investigate ZooKeeper health immediately because losing another node would cause quorum loss and can affect Kafka's coordination and metadata operations in the ZooKeeper-based architecture."

That's a **Platform Engineer answer**.

---

# 39. Another Interview Question

### "Why did Kafka move from ZooKeeper to KRaft?"

Strong answer:

> "ZooKeeper served Kafka well for many years as an external coordination and metadata system, but it meant operating two distributed systems and created additional operational and architectural complexity. As Kafka clusters and metadata requirements grew, Kafka moved to a native metadata quorum based on Raft. KRaft stores cluster metadata in Kafka's own metadata log and uses controller quorum consensus, eliminating the separate ZooKeeper dependency. KRaft became production-ready for new clusters in Kafka 3.3, ZooKeeper was deprecated in 3.5, and ZooKeeper support was removed in Kafka 4.0." ([Apache Kafka][7])

---

# 40. 🔥 Final Mental Model

Don't memorize hundreds of ZooKeeper details.

Remember this:

```text
                 WHY ZOOKEEPER?
                       │
                       ▼
             Kafka had many brokers
                       │
                       ▼
             Brokers needed to agree
                       │
          ┌────────────┼────────────┐
          ▼            ▼            ▼
      Membership    Controller    Metadata
                     Election
          │            │            │
          └────────────┼────────────┘
                       ▼
                  ZooKeeper
```

Then Kafka evolved:

```text
                 WHY KRAFT?
                       │
                       ▼
             Kafka wanted to own
             its metadata system
                       │
                       ▼
              Raft-based quorum
                       │
                       ▼
             KRaft Controllers
                       │
                       ▼
              Metadata Log
                       │
                       ▼
              No ZooKeeper
```

---

# 🔥 ZooKeeper vs KRaft — 30-Second Revision

```text
Zookeeper Era
────────────────────────────

Kafka Brokers
     │
     ▼
ZooKeeper Ensemble
     │
     ├── Broker coordination
     ├── Controller election
     ├── Cluster metadata
     └── Cluster state


KRaft Era
────────────────────────────

Kafka Brokers
     │
     │
KRaft Controllers
     │
     ▼
Raft Quorum
     │
     ▼
Kafka Metadata Log
```

### The one sentence you should remember:

> **ZooKeeper was Kafka's external coordination and metadata system; KRaft brings that metadata consensus into Kafka itself using a Raft-based controller quorum and metadata log.**

And the historical progression:

```text
ZooKeeper
   ↓
Useful for Kafka's early distributed coordination
   ↓
Kafka grows larger
   ↓
External coordination becomes operational/architectural overhead
   ↓
KRaft introduced
   ↓
KRaft production-ready for new clusters in 3.3
   ↓
ZooKeeper deprecated in 3.5
   ↓
3.9 = final 3.x release with ZK mode
   ↓
Kafka 4.0 = KRaft only
```

([Apache Kafka][7])

**For your Platform Engineer learning path, the next natural concept after this is KRaft itself: `Controller → Raft quorum → metadata log → broker registration → controller election → metadata propagation`.**

[1]: https://kafka.apache.org/26/getting-started/quickstart/?utm_source=chatgpt.com "Quick Start | Apache Kafka"
[2]: https://kafka.apache.org/33/security/zookeeper-authentication/?utm_source=chatgpt.com "ZooKeeper Authentication | Apache Kafka"
[3]: https://kafka.apache.org/090/design/design/?utm_source=chatgpt.com "Design | Apache Kafka"
[4]: https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/?utm_source=chatgpt.com "Apache Kafka 4.0.0 Release Announcement | Apache Kafka"
[5]: https://kafka.apache.org/40/operations/kraft/?utm_source=chatgpt.com "KRaft | Apache Kafka"
[6]: https://kafka.apache.org/35/operations/zookeeper/?utm_source=chatgpt.com "ZooKeeper | Apache Kafka"
[7]: https://kafka.apache.org/33/getting-started/upgrade/?utm_source=chatgpt.com "Upgrading | Apache Kafka"
[8]: https://kafka.apache.org/blog/2023/06/15/apache-kafka-3.5.0-release-announcement/?utm_source=chatgpt.com "Apache Kafka 3.5.0 Release Announcement | Apache Kafka"
[9]: https://kafka.apache.org/blog/2024/11/06/apache-kafka-3.9.0-release-announcement/?utm_source=chatgpt.com "Apache Kafka 3.9.0 Release Announcement | Apache Kafka"
[10]: https://kafka.apache.org/37/operations/kraft/?utm_source=chatgpt.com "KRaft | Apache Kafka"
