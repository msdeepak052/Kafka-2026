# KRaft — Complete Notes for a Senior Platform Engineer

Now that you've covered **ZooKeeper and why Kafka moved away from it**, KRaft is the natural next topic.

The goal here is not just to know *"KRaft replaces ZooKeeper."* You should understand:

* What KRaft is
* Why Kafka needed it
* Its architecture
* Controllers vs brokers
* Raft quorum
* Active controller
* Metadata log
* Metadata snapshots
* Broker registration
* Broker fencing
* Metadata propagation
* Controller election
* Failure scenarios
* Quorum and majority
* Combined vs dedicated mode
* Important configurations
* Monitoring/troubleshooting
* A complete production case study
* ZooKeeper vs KRaft
* Senior Platform Engineer interview questions

---

# 1. What Exactly Is KRaft?

**KRaft = Kafka + Raft-based metadata quorum.**

It is Kafka's **built-in metadata management and consensus architecture** that removes the need for ZooKeeper.

The fundamental idea is:

```text
OLD KAFKA

Kafka Brokers
      │
      ▼
ZooKeeper
      │
      ├── Metadata
      ├── Coordination
      └── Controller election
```

becomes:

```text
MODERN KAFKA

Kafka
 │
 ├── Brokers
 │
 └── Controllers
        │
        ▼
    Raft Quorum
        │
        ▼
  Metadata Log
```

KRaft was designed specifically to make Kafka manage its own metadata through a replicated log and quorum rather than relying on an external ZooKeeper cluster. ([Apache Cwiki][1])

---

# 2. What Problem Was KRaft Trying to Solve?

Let's start with the actual problem.

Old Kafka:

```text
             Kafka
               │
               │
         ┌─────┴─────┐
         ▼           ▼
      Brokers      ZooKeeper
                    │
                    ├── Metadata
                    ├── Election
                    └── Coordination
```

Kafka had to operate **two distributed systems**:

```text
Kafka
+
ZooKeeper
```

As a Platform Engineer, this means:

```text
Kafka monitoring
Kafka upgrades
Kafka networking
Kafka security
Kafka storage

        +

ZooKeeper monitoring
ZooKeeper upgrades
ZooKeeper networking
ZooKeeper security
ZooKeeper quorum
```

KRaft removes this external dependency.

---

# 3. What KRaft Solves

The main goals are:

### 1. Remove ZooKeeper

```text
Kafka
  ↓
No external ZooKeeper
```

### 2. Make metadata Kafka-native

Instead of:

```text
ZooKeeper → metadata
```

Kafka maintains:

```text
Kafka metadata log
```

### 3. Use a replicated metadata log

Metadata changes become ordered events:

```text
Metadata Event 1
Metadata Event 2
Metadata Event 3
Metadata Event 4
```

### 4. Use Raft for consensus

Controllers form a quorum:

```text
C1
C2
C3
```

and elect a leader.

### 5. Improve metadata scalability

KRaft was designed to support Kafka clusters with much larger metadata footprints and many more partitions. The original KIP explicitly identifies metadata scalability and supporting more partitions as a motivation. ([Apache Cwiki][1])

---

# 4. The Most Important KRaft Mental Model

Don't start by memorizing configuration parameters.

Remember this:

```text
                 KRaft
                   │
                   ▼
          Controller Quorum
                   │
              Raft Consensus
                   │
                   ▼
            Metadata Leader
                   │
                   ▼
             Metadata Log
                   │
                   ▼
         Cluster Metadata State
```

That's the heart of KRaft.

---

# 5. What Is Kafka Metadata?

Before understanding KRaft, remember what **metadata** means.

Suppose you have:

```text
Topic: orders

Partitions:
P0
P1
P2
```

Kafka needs to know:

```text
P0 → Leader B1
P1 → Leader B2
P2 → Leader B3
```

It also needs cluster information such as:

```text
Which brokers exist?
Which partitions exist?
Which replicas belong to each partition?
Which replicas are in ISR?
What topic/configuration changes exist?
```

This is **cluster metadata**.

---

# 6. Where Does This Metadata Live in KRaft?

In KRaft, metadata changes are recorded in a special replicated metadata log.

Conceptually:

```text
Metadata Log

Offset 0 → Broker 1 registered
Offset 1 → Broker 2 registered
Offset 2 → Topic "orders" created
Offset 3 → Partition P0 created
Offset 4 → P0 replica assignment
Offset 5 → Configuration changed
Offset 6 → Broker 3 registered
```

The metadata log is the **source of truth for cluster metadata** in the KRaft architecture. ([Apache Cwiki][2])

---

# 7. Easy Analogy — Company Master Register 📘

Imagine a large company.

You have:

```text
100 branches
```

You maintain a master register:

```text
Branch 1 → Active
Branch 2 → Active
Branch 3 → Closed

Department A → Branch 1
Department B → Branch 2
```

Every change gets written sequentially:

```text
Entry 1 → Branch 1 created
Entry 2 → Branch 2 created
Entry 3 → Department A created
Entry 4 → Branch 3 closed
```

That's similar to Kafka's metadata log.

Instead of asking:

> "What is the current state?"

from an external coordination system, Kafka can reconstruct the state by applying the ordered metadata events.

---

# 8. KRaft Architecture

A production-style KRaft cluster looks conceptually like this:

```text
                         KRaft Cluster
                              │
              ┌───────────────┴────────────────┐
              │                                │
              ▼                                ▼
       Kafka Brokers                     KRaft Controllers
              │                                │
              │                         ┌──────┼──────┐
              │                         ▼      ▼      ▼
              │                        C1     C2     C3
              │                         │      │      │
              │                         └──┬───┴───┬──┘
              │                            │
              │                       Raft Quorum
              │                            │
              │                            ▼
              │                     Metadata Log
              │                            │
              ▼                            ▼
       Actual Kafka Data             Cluster Metadata
```

The **broker data plane** stores your topic/partition data.

The **controller quorum** manages cluster metadata.

---

# 9. Broker vs Controller

This distinction is extremely important.

## Broker

The broker handles things such as:

```text
Producer requests
Consumer requests
Topic partitions
Record storage
Replication of partition data
Client traffic
```

Think:

> **Broker = data plane**

---

## Controller

The controller manages cluster metadata and coordination.

Think:

> **Controller = control plane**

It deals with things such as:

```text
Broker membership
Topic creation
Partition assignments
Leadership changes
Metadata changes
Cluster state
```

---

# 10. Easy Kubernetes Analogy

Since you're a Platform Engineer, this analogy should make sense.

Think:

```text
Kubernetes

Control Plane
     │
     ▼
API Server / Controllers
     │
     ▼
Cluster state


Kafka KRaft

Controller Quorum
     │
     ▼
Metadata Log
     │
     ▼
Kafka cluster state
```

Not exactly equivalent internally, but conceptually:

```text
KRaft Controller
       ≈
Kafka's metadata/control-plane authority
```

while:

```text
Kafka Broker
       ≈
Data-plane worker
```

---

# 11. Controller Quorum

KRaft controllers form a **quorum**.

Example:

```text
C1
C2
C3
```

These controllers replicate the metadata log.

One controller becomes the leader:

```text
C1 → Active Controller
C2 → Follower
C3 → Follower
```

Conceptually:

```text
                 C1
             ACTIVE LEADER
                 │
        ┌────────┴────────┐
        ▼                 ▼
       C2                C3
    FOLLOWER          FOLLOWER
```

The KRaft controller quorum uses Raft consensus to elect its leader and replicate metadata. ([Apache Cwiki][1])

---

# 12. What Is the Active Controller?

The controller that is currently the **leader of the metadata quorum** is the **active controller**.

Example:

```text
C1 → Active Controller
C2 → Follower
C3 → Follower
```

The active controller handles the authoritative metadata operations.

So:

```text
Producer
   │
   ▼
Broker
   │
   │ cluster metadata operation
   ▼
Active Controller
```

The followers maintain replicated metadata and can take over if the active controller fails. ([Apache Cwiki][3])

---

# 13. What Is Raft?

You don't need to become a Raft expert for Kafka administration, but you absolutely need the concept.

**Raft is a consensus algorithm.**

Its purpose is essentially:

> **Allow multiple nodes to agree on the same ordered state even when some nodes fail.**

Example:

```text
C1
C2
C3
```

They need to agree:

```text
Metadata Entry 1
Metadata Entry 2
Metadata Entry 3
```

Raft helps them agree on:

```text
Who is leader?
What metadata changes are committed?
What is the correct order?
```

---

# 14. Raft Leader Election

Suppose:

```text
C1 → Leader
C2 → Follower
C3 → Follower
```

C1 fails:

```text
C1 💥
```

The remaining controllers:

```text
C2
C3
```

elect a new leader.

For example:

```text
C2 → Leader
C3 → Follower
```

Now:

```text
C2 = Active Controller
```

This happens without ZooKeeper.

---

# 15. Quorum and Majority

This is extremely important for operations.

### 3 controllers

```text
C1
C2
C3
```

Majority:

```text
2
```

Therefore:

```text
3 controllers
→ can survive 1 failure
```

---

### 5 controllers

```text
C1
C2
C3
C4
C5
```

Majority:

```text
3
```

Therefore:

```text
5 controllers
→ can survive 2 failures
```

General rule:

```text
2F + 1 controllers
```

can tolerate:

```text
F failures
```

The KRaft design explicitly follows the majority/quorum model: three controllers can tolerate one failure, five can tolerate two. ([Apache Cwiki][1])

---

# 16. What If Majority Is Lost?

Suppose:

```text
C1
C2
C3
```

and:

```text
C1 💥
C2 💥
C3 ✅
```

Only:

```text
1 / 3
```

remains.

No majority.

Therefore the metadata quorum cannot safely continue making consensus decisions.

This is why controller quorum availability is **critical**.

---

# 17. Why Not Just One Controller?

You technically can build certain development environments with a single controller, but production should not rely on a single controller.

Because:

```text
1 Controller
     ↓
Controller fails
     ↓
No quorum
```

You lose the fault tolerance of the metadata control plane.

Production normally uses an odd number of controllers such as:

```text
3
5
```

depending on scale and availability requirements.

---

# 18. Metadata Changes Become Events

This is one of the biggest conceptual improvements.

Suppose an administrator creates:

```text
orders
```

The change becomes a metadata event.

Conceptually:

```text
Metadata Log

Entry 100:
CREATE_TOPIC orders

Entry 101:
CREATE_PARTITION orders-0

Entry 102:
CREATE_PARTITION orders-1
```

The controller quorum replicates these entries.

The current cluster state is derived from these metadata changes.

This is fundamentally aligned with Kafka's own log-based architecture. KIP-500 specifically describes metadata as an event log and explains that brokers can consume metadata events in order. ([Apache Cwiki][1])

---

# 19. Why Is an Ordered Metadata Log Better?

Imagine the old model.

Controller says:

```text
Change A
Change B
Change C
```

and pushes updates to brokers.

A broker might receive:

```text
A
C
```

but miss:

```text
B
```

That can lead to divergent state.

KRaft's design instead makes metadata changes part of an ordered replicated log:

```text
A → B → C → D
```

Brokers/controllers can consume the metadata sequence and apply changes in order.

The KIP specifically identified problems with brokers receiving incomplete or divergent controller updates in the older architecture and proposed the metadata log to address this. ([Apache Cwiki][1])

---

# 20. Metadata Propagation to Brokers

This is an important KRaft flow.

Suppose:

```text
Topic = orders
```

is created.

The flow is conceptually:

```text
Admin
  │
  ▼
Active Controller
  │
  ▼
Metadata Log
  │
  ▼
Metadata replicated to controller quorum
  │
  ▼
Brokers fetch metadata updates
```

The KRaft architecture uses incremental metadata fetching: brokers track the metadata offset they have processed and request newer changes rather than repeatedly rebuilding everything. ([Apache Cwiki][1])

---

# 21. Example — Creating a Topic

Suppose you execute:

```bash
kafka-topics.sh \
  --create \
  --topic orders \
  --partitions 3
```

Conceptually:

```text
Admin Command
      │
      ▼
Kafka Controller
      │
      ▼
Metadata Change
      │
      ▼
Metadata Log
      │
      ▼
Controller Quorum
      │
      ▼
Brokers learn:
"orders exists"
```

Then the brokers know:

```text
orders
 ├── P0
 ├── P1
 └── P2
```

---

# 22. Metadata Offset

Here's a subtle but important concept.

The metadata log has its own ordering/offset position.

Conceptually:

```text
Metadata Log

Offset 500 → Topic created
Offset 501 → Partition created
Offset 502 → Configuration changed
```

A broker might say:

```text
"I have processed metadata through offset 501."
```

The controller can then provide:

```text
"Here is metadata change 502."
```

This makes incremental metadata synchronization possible.

---

# 23. Metadata Cache

Brokers need metadata locally so they can serve requests efficiently.

Conceptually:

```text
Controller Metadata Log
          │
          ▼
       Broker
          │
          ▼
   Local Metadata Cache
```

The broker doesn't need to ask the controller for every individual client request.

It maintains the metadata it has learned.

---

# 24. What If a Broker Falls Behind?

Suppose controller metadata is at:

```text
Offset = 10,000
```

but Broker 3 has:

```text
Offset = 9,900
```

Broker 3 can fetch the missing changes:

```text
9,901
9,902
...
10,000
```

So it catches up.

This is much more efficient than blindly reconstructing everything from scratch.

The KRaft design specifically describes brokers fetching metadata deltas and only requiring a full metadata image when they are too far behind or have no cached metadata. ([Apache Cwiki][1])

---

# 25. Metadata Snapshots

Now imagine a cluster has millions of metadata entries.

If a new controller had to replay:

```text
Entry 1
Entry 2
Entry 3
...
Entry 10,000,000
```

that would take time.

KRaft therefore supports **metadata snapshots**.

Conceptually:

```text
Metadata Log

1
2
3
...
1,000,000

        ↓

Snapshot at 1,000,000
```

Instead of replaying everything:

```text
Snapshot
   +
New entries
```

can reconstruct current metadata.

Snapshots help control:

* Disk usage
* Startup/recovery time
* Catch-up time

Kafka's KRaft snapshot design explicitly addresses the need to prevent an indefinitely growing metadata log from causing disk and recovery problems. ([Apache Cwiki][4])

---

# 26. Snapshot Analogy 📸

Think about a video game.

Without a snapshot:

```text
Start game
 ↓
Replay 10 million actions
 ↓
Current state
```

With a snapshot:

```text
Saved game at Level 100
       +
Replay only actions after Level 100
       ↓
Current state
```

KRaft metadata snapshots work conceptually like this.

---

# 27. Broker Registration

When a broker starts, it needs to become a member of the Kafka cluster.

Old architecture:

```text
Broker
  ↓
ZooKeeper registration
```

KRaft:

```text
Broker
  ↓
KRaft Controller Quorum
```

The broker registers with the controller quorum and starts participating once it has the necessary current metadata. ([Apache Cwiki][1])

---

# 28. Broker Heartbeats

The controller needs to know:

> "Is this broker alive and still part of the cluster?"

KRaft uses broker-controller communication for this.

The metadata-fetch mechanism also serves as a heartbeat in the KRaft design. ([Apache Cwiki][1])

Conceptually:

```text
Broker
   │
   │ "I'm alive + give me metadata"
   ▼
Controller
```

---

# 29. Broker Fencing 🚧

This is an important Platform Engineer concept.

Suppose a broker is starting or has lost contact with the active controller.

KRaft doesn't want that broker blindly serving traffic using stale metadata.

So the broker can enter a **fenced** state.

Conceptually:

```text
Broker starts
    ↓
Needs current metadata
    ↓
Not ready
    ↓
FENCED
```

While fenced:

```text
❌ Don't serve normal client traffic
```

Once it catches up and becomes valid:

```text
FENCED
   ↓
Metadata caught up
   ↓
ONLINE
```

The KRaft design defines broker states including Offline, Fenced, Online and Stopping. ([Apache Cwiki][1])

---

# 30. Why Is Fencing Important?

Imagine:

```text
Old Broker
```

has stale information:

```text
P0 leader = Broker 1
```

But current cluster state says:

```text
P0 leader = Broker 2
```

If Broker 1 continues accepting writes as though it were leader, you could have dangerous inconsistency.

Fencing helps prevent stale brokers from behaving as valid cluster members.

---

# 31. Broker State Flow

Simplified:

```text
                 START
                   │
                   ▼
                OFFLINE
                   │
                   ▼
                FENCED
                   │
          Metadata synchronized
                   │
                   ▼
                ONLINE
                   │
             Shutdown request
                   │
                   ▼
               STOPPING
                   │
                   ▼
                OFFLINE
```

---

# 32. What Happens When a Broker Dies?

Let's use a real example.

Cluster:

```text
Controllers:

C1 → Leader
C2 → Follower
C3 → Follower


Brokers:

B1
B2
B3
```

Topic:

```text
orders
```

Suppose:

```text
P0 → B1 Leader
     B2 Replica
     B3 Replica
```

Now:

```text
B1 💥
```

The controller detects that the broker is no longer healthy/registered.

The cluster metadata is updated.

A suitable replica can become the new leader:

```text
P0 → B2 Leader
     B3 Replica
```

That metadata change becomes part of the metadata log.

---

# 33. Complete Broker Failure Flow

```text
B1
 │
 └── P0 Leader
        │
        ▼
     B1 fails
        │
        ▼
Controller detects failure
        │
        ▼
Metadata change
        │
        ▼
Metadata Log
        │
        ▼
Controller quorum commits change
        │
        ▼
New P0 leader selected
        │
        ▼
B2 → P0 Leader
        │
        ▼
Brokers update metadata
```

This is the KRaft control-plane flow.

---

# 34. Controller Failure

Now let's fail the controller instead.

Before:

```text
C1 → Active Controller
C2 → Follower
C3 → Follower
```

C1 crashes:

```text
C1 💥
```

Raft election occurs.

```text
C2 → Active Controller
C3 → Follower
```

The new active controller already has replicated metadata.

This is an important improvement over an architecture where the new controller has to rebuild its state from an external system.

The KRaft design specifically describes follower controllers as hot standbys and notes that controller failover doesn't require a lengthy full-state reload. ([Apache Cwiki][1])

---

# 35. Controller Failure vs Broker Failure

Don't confuse them.

### Broker failure

```text
Broker fails
    ↓
Data/partition leadership affected
    ↓
Controller coordinates metadata change
```

### Controller failure

```text
Controller fails
    ↓
Raft election
    ↓
New active controller
    ↓
Metadata control plane continues
```

Very important distinction for interviews.

---

# 36. KRaft Does NOT Store Your Application Messages

This misconception must be avoided.

Suppose:

```text
orders-P0

Offset 0 → Order A
Offset 1 → Order B
Offset 2 → Order C
```

These records are stored by the **Kafka brokers**.

KRaft manages:

```text
"Who owns P0?"
"Which brokers are replicas?"
"Which broker is leader?"
"What topics exist?"
```

So:

```text
KRaft
   ↓
Metadata / Control Plane

Kafka Broker
   ↓
Actual Event Data / Data Plane
```

---

# 37. KRaft Controller Quorum vs Data Replication

These are **two different replication mechanisms**.

### Application data

```text
orders-P0

B1 → Leader
B2 → Replica
B3 → Replica
```

This is Kafka's **partition replication**.

### Metadata

```text
Controller quorum

C1 → Metadata leader
C2 → Metadata replica
C3 → Metadata replica
```

This is KRaft's **metadata quorum**.

Don't mix them.

---

# 38. Two Types of "Leader"

This is an excellent interview trap.

### Partition Leader

Example:

```text
orders-P0
    ↓
B1 = Partition Leader
```

Handles data requests for that partition.

### Controller Quorum Leader

Example:

```text
C1 = Active Controller
```

Handles metadata/control-plane leadership.

So:

```text
Partition Leader
      ≠
KRaft Controller Leader
```

---

# 39. Combined Mode vs Dedicated Controllers

KRaft supports different deployment approaches.

## Combined mode

A process can have both:

```text
broker
+
controller
```

Conceptually:

```text
Node 1 → Broker + Controller
Node 2 → Broker + Controller
Node 3 → Broker + Controller
```

This is convenient for:

* Development
* Small environments
* Simpler deployments

---

# 40. Dedicated Controller Mode

For larger production environments, controllers can be separated:

```text
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
...
```

Architecture:

```text
             Controllers
          ┌────┬────┬────┐
          │ C1 │ C2 │ C3 │
          └────┴────┴────┘
                  │
             Metadata
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
       B1        B2        B3
```

This separation lets you scale the metadata/control plane independently from the data plane.

---

# 41. Why Dedicated Controllers Can Be Better

Suppose you have:

```text
50 Brokers
3 Controllers
```

If a broker is under heavy workload:

```text
B25
CPU = 95%
```

it doesn't necessarily mean your metadata quorum should be overloaded too.

With dedicated controllers:

```text
Controllers
   ↓
Control plane

Brokers
   ↓
Data plane
```

you get better isolation.

---

# 42. `process.roles`

In KRaft configuration, `process.roles` determines whether a Kafka process acts as:

```text
broker
controller
```

or both.

Conceptually:

```text
process.roles=broker
```

means:

```text
Broker only
```

while:

```text
process.roles=controller
```

means:

```text
Controller only
```

and:

```text
process.roles=broker,controller
```

means:

```text
Combined
```

---

# 43. `node.id`

KRaft uses:

```text
node.id
```

to identify a Kafka node.

Example:

```text
node.id=1
```

Another:

```text
node.id=2
```

This is an important KRaft configuration concept.

---

# 44. `controller.quorum.voters`

In static quorum configurations, controllers need to know the controller voter set.

Conceptually:

```text
controller.quorum.voters=
1@controller1:9093,
2@controller2:9093,
3@controller3:9093
```

Meaning:

```text
Node 1 → controller1:9093
Node 2 → controller2:9093
Node 3 → controller3:9093
```

This lets the controllers establish the quorum.

Kafka's current documentation also supports newer dynamic quorum mechanisms, so don't assume every modern Kafka deployment uses the old static voter configuration forever. The KRaft architecture has evolved beyond the original static quorum design. ([Apache Cwiki][5])

---

# 45. Controller Listener

Controllers need their own communication endpoint.

Conceptually:

```text
listeners

BROKER:
9092

CONTROLLER:
9093
```

For example:

```text
PLAINTEXT://broker1:9092
CONTROLLER://controller1:9093
```

The exact security/listener configuration depends on your deployment.

---

# 46. `metadata.log.dir`

This is particularly important for a Platform Engineer.

KRaft controllers need persistent storage for the metadata log.

Kafka supports:

```text
metadata.log.dir
```

which specifies where the metadata log is stored.

If it isn't explicitly configured, Kafka can use the first configured log directory. ([Apache Kafka][6])

Conceptually:

```text
Controller
    │
    ▼
metadata.log.dir
    │
    ▼
Metadata Log
```

---

# 47. Why Is Metadata Storage Important?

Imagine:

```text
Controller
   │
   ▼
Metadata
```

and the controller machine loses its disk.

If metadata quorum data isn't safely replicated and recovered, you can have a serious control-plane problem.

Therefore, as a Platform Engineer, you care about:

```text
Controller disk
      ↓
Persistent storage
      ↓
Metadata durability
      ↓
Controller recovery
```

Kafka documentation specifically warns about safely replacing a KRaft controller disk and checking replication status before provisioning a replacement. ([Apache Kafka][6])

---

# 48. KRaft Metadata Quorum Health

Kafka provides tooling to inspect the metadata quorum.

For example:

```bash
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --replication
```

This can show information such as:

```text
NodeId
LogEndOffset
Lag
LastFetchTimestamp
Status
```

Kafka's documentation specifically recommends this tool when checking metadata replication before replacing a controller disk. ([Apache Kafka][6])

---

# 49. What Should You Monitor?

As a Platform Engineer, don't just monitor:

```text
CPU
Memory
Disk
```

For KRaft, also monitor metadata quorum health.

Important concepts include:

```text
Current State
Current Leader
Current Epoch
High Watermark
Replication lag
Metadata log health
Controller availability
```

Kafka exposes KRaft quorum metrics such as current state, current leader, current epoch and high watermark. ([Apache Kafka][7])

---

# 50. What Is High Watermark Here?

At a high level, the metadata quorum's high watermark represents the point up to which metadata log entries are considered committed by the quorum.

Think:

```text
Metadata Log

100
101
102
103
104
```

If:

```text
High Watermark = 103
```

then:

```text
100 → committed
101 → committed
102 → committed
103 → committed
104 → not yet committed
```

This is analogous to the importance of committed positions in Kafka's replicated logs, but don't confuse the **metadata quorum's log position** with a topic partition's consumer offset.

---

# 51. KRaft Metadata Flow — Complete

Let's put it together.

Suppose:

```text
Admin:
Create topic orders
```

Flow:

```text
Admin
  │
  ▼
Active Controller
  │
  ▼
Metadata Change
  │
  ▼
Metadata Log
  │
  ├──────────┐
  ▼          ▼
C2          C3
Replica     Replica
  │          │
  └────┬─────┘
       │
       ▼
Metadata committed
       │
       ▼
Brokers fetch metadata
       │
       ▼
Local metadata cache
```

That's the **control-plane flow**.

---

# 52. Complete Production Case Study

Let's use an e-commerce company.

## Architecture

```text
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

Topic:

```text
orders
```

Partitions:

```text
P0
P1
P2
P3
```

---

# 53. Step 1 — Controllers Form Quorum

```text
C1
C2
C3
```

Raft election:

```text
C1 → Active Controller
C2 → Follower
C3 → Follower
```

---

# 54. Step 2 — Broker Starts

```text
B1 starts
```

It contacts the controller quorum.

```text
B1
 │
 ▼
Controller
 │
 ▼
Register B1
```

Metadata is updated.

---

# 55. Step 3 — Topic Creation

Admin:

```text
Create orders
4 partitions
RF = 3
```

Controller records metadata.

Conceptually:

```text
Metadata Log

Entry X:
Topic = orders

Entry X+1:
P0 created

Entry X+2:
P1 created

Entry X+3:
P2 created

Entry X+4:
P3 created
```

---

# 56. Step 4 — Replica Assignment

Controller determines assignments.

Example:

```text
P0:
B1 B2 B3

P1:
B2 B3 B4

P2:
B3 B4 B5

P3:
B4 B5 B1
```

This metadata becomes part of the metadata state.

---

# 57. Step 5 — Producer Sends Data

Producer sends:

```text
ORD-1001
```

Suppose:

```text
ORD-1001 → P1
```

Metadata says:

```text
P1 Leader = B2
```

Producer sends:

```text
Producer
   │
   ▼
B2
   │
   ▼
orders-P1
```

Notice:

**KRaft is not storing `ORD-1001`.**

B2 stores the actual record.

---

# 58. Step 6 — Broker B2 Fails

```text
B2 💥
```

B2 was leader for P1.

Controller detects the failure.

Metadata changes:

```text
P1 Leader:
B2 ❌
B3 ✅
```

The metadata change is recorded in the metadata log.

Brokers update their metadata.

Now:

```text
Producer
   │
   ▼
B3
   │
   ▼
orders-P1
```

---

# 59. Step 7 — Active Controller Fails

Now:

```text
C1 💥
```

Remaining:

```text
C2
C3
```

Quorum still exists:

```text
2 / 3
```

Raft election:

```text
C2 → Active Controller
C3 → Follower
```

Because the metadata state has been replicated, the new controller can take over without relying on ZooKeeper.

---

# 60. Step 8 — Broker Joins Again

Suppose B2 comes back.

```text
B2
 │
 ▼
Controller
 │
 ▼
Catch up metadata
 │
 ▼
Online
```

If it is not sufficiently synchronized:

```text
B2 → Fenced
```

Once it catches up:

```text
B2 → Online
```

---

# 61. What KRaft Solved in This Case

### Old architecture

```text
B2 fails
 ↓
Controller
 ↓
ZooKeeper coordination
 ↓
Metadata change
 ↓
Brokers
```

### KRaft

```text
B2 fails
 ↓
Active Controller
 ↓
Metadata Log
 ↓
Raft quorum
 ↓
Metadata propagated
 ↓
Brokers
```

The entire control plane is now **Kafka-native**.

---

# 62. KRaft vs ZooKeeper — Deep Comparison

| Area                   | ZooKeeper                          | KRaft                     |
| ---------------------- | ---------------------------------- | ------------------------- |
| Metadata owner         | ZooKeeper                          | Kafka metadata log        |
| Consensus              | ZooKeeper                          | Raft                      |
| Controller election    | Coordinated using ZK               | Controller quorum         |
| External dependency    | Yes                                | No                        |
| Metadata log           | External coordination model        | Native replicated log     |
| Broker registration    | ZooKeeper                          | Controller quorum         |
| Metadata propagation   | Controller push model historically | Metadata log/fetch model  |
| Controller failover    | Reload/rebuild state historically  | Replicated metadata state |
| Deployment             | Kafka + ZK                         | Kafka                     |
| Operational complexity | Higher                             | Lower                     |
| Modern Kafka           | Legacy                             | Standard architecture     |

The architectural motivation and differences are laid out in KIP-500. ([Apache Cwiki][1])

---

# 63. The Biggest Architectural Difference

This is probably the most important point of the entire topic.

### ZooKeeper architecture

```text
Kafka metadata

        ↓

External system

        ↓

ZooKeeper
```

### KRaft

```text
Kafka metadata

        ↓

Kafka's own replicated log

        ↓

Raft quorum
```

In other words:

> **KRaft turns Kafka metadata itself into a replicated, ordered log managed by Kafka's controller quorum.**

---

# 64. Why "Log" Is So Important

Kafka already has this powerful primitive:

```text
Append
 ↓
Offset
 ↓
Ordering
 ↓
Replication
 ↓
Replay
```

KRaft applies the same general idea to **cluster metadata**.

Instead of treating metadata as disconnected state:

```text
"Topic exists"
"Broker exists"
"Partition changed"
```

Kafka can represent metadata changes as an ordered sequence.

KIP-500 explicitly calls out this "metadata as an event log" design. ([Apache Cwiki][1])

---

# 65. Important: Metadata Log ≠ Your Topic

Don't confuse:

```text
orders topic
```

with:

```text
KRaft metadata log
```

### `orders`

Contains:

```text
Order events
```

### Metadata log

Contains:

```text
Cluster metadata changes
```

Conceptually:

```text
                    Kafka
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
     Data Topics             Metadata Log
          │                       │
    Orders/events          Cluster state
```

---

# 66. Important: Metadata Log ≠ Consumer Offset

You've already learned offsets.

There are different concepts:

```text
Topic Partition Offset
        ↓
Consumer's position in application data


Metadata Log Position
        ↓
Position in cluster metadata history
```

Don't mix them.

---

# 67. Important: KRaft Doesn't Replace Kafka Replication

You still have normal Kafka replication:

```text
P0:

B1 → Leader
B2 → Replica
B3 → Replica
```

KRaft doesn't replace that.

KRaft manages **cluster metadata**.

```text
KRaft
 ↓
Who should be leader?
Which brokers exist?
Which replicas belong to partition?
What topics exist?
```

Kafka replication handles:

```text
Actual records
```

---

# 68. Platform Engineer Failure Matrix

| Failure                           | What happens                                                       |
| --------------------------------- | ------------------------------------------------------------------ |
| One broker fails                  | Controller updates cluster metadata; partition leadership may move |
| One controller fails              | Raft elects another controller if quorum remains                   |
| Majority controllers fail         | Metadata quorum loses majority                                     |
| Broker starts with stale metadata | Broker can remain fenced until synchronized                        |
| Controller disk fails             | Requires careful recovery/replacement procedure                    |
| Broker disk fails                 | Kafka partition replica/data recovery mechanisms apply             |

---

# 69. Production Design — How Many Controllers?

A common production starting point:

```text
3 controllers
```

Why?

```text
3
↓
Majority = 2
↓
Can tolerate 1 failure
```

For a larger/critical environment:

```text
5 controllers
```

gives:

```text
Majority = 3
Can tolerate 2 failures
```

But don't automatically choose five just because five is "more HA."

More nodes also mean more infrastructure and quorum coordination.

---

# 70. Why Odd Number?

Because quorum needs a majority.

Compare:

```text
3 nodes → majority 2
4 nodes → majority 3
5 nodes → majority 3
```

Going:

```text
3 → 4
```

adds a node but does not improve failure tolerance:

```text
3 → tolerate 1 failure
4 → tolerate 1 failure
```

while:

```text
5 → tolerate 2 failures
```

Therefore odd-sized quorums are generally efficient.

---

# 71. Senior Platform Engineer: What Do You Monitor?

For KRaft, I'd build monitoring around:

### Controller quorum

```text
Current leader
Current state
Current epoch
Quorum availability
```

### Metadata replication

```text
High watermark
Log end offset
Replication lag
Controller catch-up
```

### Brokers

```text
Broker registration
Broker state
Fenced brokers
Metadata lag
```

### Storage

```text
metadata.log.dir
Disk usage
Disk failures
I/O latency
```

Kafka exposes dedicated KRaft quorum metrics including current state, current leader, current epoch and high watermark. ([Apache Kafka][7])

---

# 72. Troubleshooting Scenario #1

### Problem:

```text
Broker won't become available.
```

You check:

```text
Broker state = FENCED
```

What do you investigate?

```text
1. Can broker reach controller?
2. Controller quorum healthy?
3. Is metadata up to date?
4. Network connectivity?
5. Controller listener?
6. Authentication/TLS?
7. Metadata log lag?
```

The key idea:

> **A broker may be alive as a process but not be an active cluster member.**

---

# 73. Troubleshooting Scenario #2

### Problem:

```text
Controller quorum unhealthy.
```

Check:

```text
C1 → Leader
C2 → Follower
C3 → Follower
```

Then:

```text
C1 ❌
C2 ❌
C3 ✅
```

You have:

```text
1/3
```

No quorum.

Therefore the first priority is restoring controller quorum availability.

---

# 74. Troubleshooting Scenario #3

### Broker Disk Failure

Suppose controller metadata is healthy:

```text
C1
C2
C3
```

but:

```text
B2 data disk ❌
```

Don't confuse this with a controller failure.

You investigate:

```text
Kafka data directories
Partition replicas
Replication state
Broker logs
Disk health
```

KRaft's metadata quorum may be completely healthy while a broker's **data plane** has a storage problem.

---

# 75. Troubleshooting Scenario #4 — Controller Disk Failure

Suppose:

```text
C2 metadata disk ❌
```

This is more serious because the controller stores metadata state.

You need to verify that the remaining controller quorum has the committed metadata before replacing/reprovisioning the failed controller's storage.

Kafka specifically documents using `kafka-metadata-quorum.sh --bootstrap-server ... describe --replication` to inspect replication status in this situation. ([Apache Kafka][6])

---

# 76. The Complete KRaft Architecture

Keep this diagram.

```text
                           KAFKA CLUSTER
                                │
              ┌─────────────────┴─────────────────┐
              │                                   │
              ▼                                   ▼
         DATA PLANE                           CONTROL PLANE
              │                                   │
              ▼                                   ▼
         Kafka Brokers                       KRaft Controllers
              │                            ┌──────┼──────┐
              │                            ▼      ▼      ▼
        ┌─────┼─────┐                     C1     C2     C3
        ▼     ▼     ▼                      │      │      │
       B1    B2    B3                      └──┬───┴───┬──┘
        │     │     │                         │
        │     │     │                    Raft Quorum
        │     │     │                         │
        │     │     │                         ▼
        │     │     │                  Metadata Log
        │     │     │                         │
        │     │     │                         ▼
        │     │     │                  Cluster Metadata
        │     │     │
        ▼     ▼     ▼
    Topic/Partition Data
```

---

# 77. Complete End-to-End KRaft Flow

```text
Admin / Broker Event
        │
        ▼
Active Controller
        │
        ▼
Metadata Change
        │
        ▼
Raft Metadata Quorum
        │
        ├──────────┐
        ▼          ▼
       C2         C3
    Metadata     Metadata
     Replica     Replica
        │          │
        └────┬─────┘
             ▼
       Metadata Committed
             │
             ▼
       Brokers Fetch Update
             │
             ▼
     Local Metadata Cache
             │
             ▼
     Brokers act on new state
```

---

# 78. KRaft in One Real Production Story

Let's put everything together.

### Initial state

```text
Controllers:

C1 → Leader
C2 → Follower
C3 → Follower

Brokers:

B1
B2
B3
B4
B5
```

### Topic

```text
orders

P0 → B1 B2 B3
P1 → B2 B3 B4
P2 → B3 B4 B5
```

### Customer places order

```text
Producer
   ↓
orders-P1
   ↓
B2
```

### B2 fails

```text
B2 ❌
```

Controller updates metadata:

```text
P1 Leader = B3
```

Metadata change:

```text
Metadata Log
     ↓
Replicated to C2/C3
     ↓
Committed
```

Brokers learn:

```text
P1 → B3
```

Producer refreshes its metadata and sends future traffic to B3.

### Now C1 fails

```text
C1 ❌
```

Raft election:

```text
C2 → Leader
C3 → Follower
```

No ZooKeeper required.

### B2 returns

```text
B2
 ↓
Register
 ↓
Fetch metadata
 ↓
Catch up
 ↓
Online
```

That is the **KRaft lifecycle**.

---

# 79. ZooKeeper → KRaft Evolution

Keep this timeline in your notes:

```text
OLD KAFKA
    │
    ▼
Kafka Brokers + ZooKeeper
    │
    │
    │  ZooKeeper handled
    │  coordination/metadata
    ▼
KRaft introduced
    │
    ▼
Kafka Controller Quorum
    │
    ▼
Raft Consensus
    │
    ▼
Kafka Metadata Log
    │
    ▼
ZooKeeper dependency removed
```

KRaft's architecture was formally proposed through KIP-500, with the controller quorum and metadata log further specified through subsequent KIPs. ([Apache Cwiki][1])

---

# 80. The Most Important Terms to Know

| Term                     | Meaning                                                                       |
| ------------------------ | ----------------------------------------------------------------------------- |
| **KRaft**                | Kafka's Raft-based metadata quorum architecture                               |
| **Controller**           | Kafka process responsible for cluster metadata/control-plane work             |
| **Controller quorum**    | Group of controllers participating in metadata consensus                      |
| **Active controller**    | Current leader of the controller/metadata quorum                              |
| **Raft**                 | Consensus algorithm used by the controller quorum                             |
| **Metadata log**         | Ordered replicated log containing cluster metadata changes                    |
| **Metadata cache**       | Broker's local view of cluster metadata                                       |
| **Metadata snapshot**    | Point-in-time persisted metadata state used to avoid replaying the entire log |
| **Broker registration**  | Process by which a broker joins the KRaft cluster                             |
| **Fencing**              | Preventing a broker with stale/unusable metadata state from serving normally  |
| **High watermark**       | Committed point in the metadata quorum's log                                  |
| **Controller election**  | Raft process selecting a new metadata leader                                  |
| **Quorum**               | Majority needed for consensus                                                 |
| **Combined mode**        | Same process acts as broker and controller                                    |
| **Dedicated controller** | Controller runs separately from brokers                                       |

---

# 81. 🔥 What You Must Not Confuse

### 1. Controller ≠ Broker

```text
Controller → metadata/control plane
Broker → data plane
```

### 2. Controller Leader ≠ Partition Leader

```text
C1 → Active Controller

B1 → orders-P0 Leader
```

Different things.

### 3. Metadata Log ≠ Application Topic

```text
Metadata Log → cluster state

orders topic → application events
```

### 4. Metadata offset ≠ Consumer offset

```text
Metadata position → cluster metadata

Consumer offset → application event consumption
```

### 5. KRaft ≠ Kafka data replication

```text
KRaft → metadata quorum

Kafka replication → topic/partition data
```

---

# 82. Senior Platform Engineer Interview Answer

### "What is KRaft?"

> **"KRaft is Kafka's built-in metadata quorum architecture based on Raft consensus. It replaces the external ZooKeeper dependency by using a quorum of Kafka controllers to maintain a replicated metadata log. One controller acts as the active controller and leads metadata operations, while the other controllers replicate the metadata state and can take over during failure. Brokers register with the controller quorum and fetch metadata updates from it. This makes Kafka's control plane self-managed, simplifies deployment, and improves metadata scalability."**

---

# 83. "What happens when the KRaft controller dies?"

> **"If the active controller fails, the remaining controller quorum uses Raft to elect a new leader. Because the metadata log is replicated across the controller quorum, the new active controller already has the committed metadata state and can continue managing the cluster without depending on ZooKeeper."** ([Apache Cwiki][1])

---

# 84. "What happens when a Kafka broker dies?"

> **"The active controller detects the broker failure, updates cluster metadata, and coordinates partition leadership changes where necessary. The metadata change is recorded in the metadata quorum, and brokers update their local metadata state. The actual application data remains on the Kafka partition replicas."**

---

# 85. "Why do we need three controllers?"

> **"KRaft controllers form a quorum. With three controllers, two constitute a majority, so the cluster can tolerate one controller failure while still maintaining consensus. Five controllers can tolerate two failures. The important point is maintaining a majority rather than simply maximizing the number of nodes."**

---

# 86. "Where is Kafka metadata stored in KRaft?"

> **"Cluster metadata is maintained through the replicated metadata log managed by the KRaft controller quorum. Controllers materialize that state in memory and persist it, while metadata snapshots help avoid replaying an indefinitely growing log. Brokers maintain their own local metadata state/cache as they consume metadata updates."** ([Apache Cwiki][4])

---

# 87. "Why is KRaft better than ZooKeeper?"

Don't say:

> ❌ "KRaft is faster than ZooKeeper."

That's too simplistic.

Say:

> **"The primary advantage is architectural and operational: Kafka no longer needs a separate distributed coordination system. KRaft brings metadata management and consensus into Kafka itself, uses a replicated metadata log, provides a native controller quorum, reduces operational complexity, and was designed to scale Kafka metadata more effectively."** ([Apache Cwiki][1])

---

# 88. 🔥 Your Final KRaft Mental Model

If you remember only this, you're good:

```text
                         KRaft
                           │
                           ▼
                 Controller Quorum
                           │
                     Raft Consensus
                           │
                           ▼
                  Active Controller
                           │
                           ▼
                    Metadata Log
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
            C1            C2            C3
             │             │             │
             └─────────────┼─────────────┘
                           │
                    Metadata committed
                           │
                           ▼
                       Brokers
                           │
                    Metadata Cache
                           │
                           ▼
                 Kafka Data Plane
                           │
                           ▼
                  Topics / Partitions
                           │
                           ▼
                     Actual Events
```

### And the entire concept in one sentence:

> **KRaft is Kafka's internal control-plane architecture where a Raft-based controller quorum maintains an ordered, replicated metadata log, allowing Kafka to manage its own cluster metadata and controller elections without ZooKeeper.** ([Apache Cwiki][1])

---

## 🔥 Senior Platform Engineer Cheat Sheet

```text
KRaft
│
├── Why?
│   └── Remove ZooKeeper + simplify metadata management
│
├── Controllers
│   └── Manage cluster metadata
│
├── Controller Quorum
│   └── Multiple controllers
│
├── Raft
│   └── Consensus + leader election
│
├── Active Controller
│   └── Current metadata leader
│
├── Metadata Log
│   └── Ordered record of cluster metadata changes
│
├── Metadata Snapshot
│   └── Faster recovery/catch-up
│
├── Broker Registration
│   └── Broker joins controller-managed cluster
│
├── Broker Fencing
│   └── Prevent stale broker from serving
│
├── Metadata Fetch
│   └── Brokers receive metadata updates
│
├── Quorum
│   └── Majority required
│
├── 3 Controllers
│   └── Tolerate 1 failure
│
├── 5 Controllers
│   └── Tolerate 2 failures
│
├── Dedicated Controllers
│   └── Separate control plane
│
└── Combined Mode
    └── Broker + Controller in same process
```

### The one architecture you should visualize

```text
                 ┌──────────────────────────┐
                 │      KRaft Controllers   │
                 │                          │
                 │ C1   C2   C3             │
                 │  \    |    /             │
                 │   \ Raft /               │
                 │    Metadata Log           │
                 └──────────┬───────────────┘
                            │
                       Cluster Metadata
                            │
             ┌──────────────┼──────────────┐
             ▼              ▼              ▼
            B1             B2             B3
          Broker          Broker         Broker
             │              │              │
             └──────────────┼──────────────┘
                            │
                            ▼
                  Topics / Partitions
                            │
                            ▼
                     Actual Events
```

**That is KRaft from a Platform Engineer's perspective:**

**ZooKeeper's old external coordination role → Kafka Controller Quorum → Raft → Metadata Log → Metadata State → Brokers → Topics/Partitions.**

[1]: https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A%2BReplace%2BZooKeeper%2Bwith%2Ba%2BSelf-Managed%2BMetadata%2BQuorum?utm_source=chatgpt.com "KIP-500: Replace ZooKeeper with a Self-Managed Metadata Quorum - Apache Kafka - Apache Software Foundation"
[2]: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/217392707/KIP-859%2BAdd%2BMetadata%2BLog%2BProcessing%2BError%2BRelated%2BMetrics?utm_source=chatgpt.com "KIP-859: Add Metadata Log Processing Error Related Metrics - Apache Kafka - Apache Software Foundation"
[3]: https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=173082410&utm_source=chatgpt.com "KIP-631: The Quorum-based Kafka Controller - Apache Kafka - Apache Software Foundation"
[4]: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/158864763/KIP-630%2BKafka%2BRaft%2BSnapshot?utm_source=chatgpt.com "KIP-630: Kafka Raft Snapshot - Apache Kafka - Apache Software Foundation"
[5]: https://cwiki.apache.org/confluence/spaces/KAFKA/pages/421958889/KIP-1347%2BOverriding%2Bvoter%2Bset%2Bon%2Bstorage%2Bformatting?utm_source=chatgpt.com "KIP-1347: Overriding voter set on storage formatting - Apache Kafka - Apache Software Foundation"
[6]: https://kafka.apache.org/38/operations/hardware-and-os/?utm_source=chatgpt.com "Hardware and OS | Apache Kafka"
[7]: https://kafka.apache.org/37/operations/monitoring/?utm_source=chatgpt.com "Monitoring | Apache Kafka"
