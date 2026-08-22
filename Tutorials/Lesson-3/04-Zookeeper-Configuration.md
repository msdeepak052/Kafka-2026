# ZooKeeper Configuration

For this topic, focus on **how a ZooKeeper 3-node quorum is configured and what each important parameter does**.

---

## 1. Architecture

We'll assume:

```text
ZooKeeper Ensemble

ZK1 ───────┐
           │
ZK2 ───────┼── Quorum
           │
ZK3 ───────┘

Client port: 2181
Peer port:   2888
Election:    3888
```

Typical setup:

```text
ZK1 → myid=1
ZK2 → myid=2
ZK3 → myid=3
```

---

# 2. Main Configuration File

ZooKeeper uses:

```text
conf/zoo.cfg
```

Example 3-node configuration:

```properties
tickTime=2000

dataDir=/var/lib/zookeeper

clientPort=2181

initLimit=5

syncLimit=2

server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
```

Now understand each important parameter.

---

# 3. `tickTime`

```properties
tickTime=2000
```

Unit:

```text
milliseconds
```

So:

```text
2000 ms = 2 seconds
```

It's ZooKeeper's **basic time unit** used by other timing parameters.

Think:

```text
tickTime
   ↓
Base clock for ZooKeeper
```

---

# 4. `dataDir`

```properties
dataDir=/var/lib/zookeeper
```

Where ZooKeeper stores its persistent data.

Example:

```text
/var/lib/zookeeper
       │
       └── ZooKeeper state
```

### Platform Engineer point

This is persistent data.

So don't casually delete this directory on a production ZooKeeper node.

---

# 5. `clientPort`

```properties
clientPort=2181
```

Port used by clients such as Kafka brokers to connect to ZooKeeper.

```text
Kafka Broker
     │
     │ :2181
     ▼
ZooKeeper
```

Common port:

```text
2181 → Client connections
```

---

# 6. `server.X`

This defines the ZooKeeper ensemble members.

```properties
server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
```

Format:

```text
server.<ID>=<hostname>:<peer-port>:<election-port>
```

For example:

```text
server.1=zk1:2888:3888
          │     │     │
          │     │     └── Leader election
          │     └──────── Peer communication
          └────────────── ZooKeeper server
```

---

# 7. `2888` — Peer Communication

```text
2888
```

Used for ZooKeeper servers to communicate with each other.

```text
ZK1 ─────2888────► ZK2
 │                  │
 └──────2888────────► ZK3
```

This is **not the normal Kafka client connection port**.

---

# 8. `3888` — Leader Election

```text
3888
```

Used by ZooKeeper servers during leader election.

Example:

```text
ZK1 → Leader
ZK2 → Follower
ZK3 → Follower
```

If ZK1 dies:

```text
ZK1 ❌

ZK2 ◄────► ZK3
       │
       ▼
Leader election
```

---

# 9. `myid`

Each ZooKeeper server needs its own ID.

For ZK1:

```text
/var/lib/zookeeper/myid
```

contains:

```text
1
```

ZK2:

```text
2
```

ZK3:

```text
3
```

This must match:

```properties
server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
```

So:

```text
ZK1 → myid=1 → server.1
ZK2 → myid=2 → server.2
ZK3 → myid=3 → server.3
```

---

# 10. `initLimit`

Example:

```properties
initLimit=5
```

It is measured in `tickTime`.

With:

```properties
tickTime=2000
initLimit=5
```

approximately:

```text
5 × 2 seconds
= 10 seconds
```

It gives a follower time to connect to and synchronize with the leader during initialization.

---

# 11. `syncLimit`

Example:

```properties
syncLimit=2
```

With:

```text
tickTime = 2 seconds
```

approximately:

```text
2 × 2
= 4 seconds
```

It controls how far behind a follower can become relative to the leader before ZooKeeper considers the connection unhealthy.

Think:

```text
Leader
  │
  ├── Follower A → healthy
  │
  └── Follower B → too far behind
                         │
                         ▼
                    connection problem
```

---

# 12. Complete 3-Node Configuration

### ZK1

```properties
tickTime=2000
dataDir=/var/lib/zookeeper
clientPort=2181

initLimit=5
syncLimit=2

server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
```

```text
myid = 1
```

### ZK2

Same `zoo.cfg`:

```text
myid = 2
```

### ZK3

Same `zoo.cfg`:

```text
myid = 3
```

The important difference between the servers is their `myid`.

---

# 13. Startup Flow

```text
Start ZK1
   ↓
Start ZK2
   ↓
Start ZK3
   ↓
Leader election
   ↓
ZK1 → Leader
ZK2 → Follower
ZK3 → Follower
   ↓
2/3 quorum
   ↓
ZooKeeper operational
```

---

# 14. Kafka Connection

Historically, Kafka brokers used:

```properties
zookeeper.connect=zk1:2181,zk2:2181,zk3:2181
```

Architecture:

```text
             Kafka Cluster
          ┌────┬────┬────┐
          │ B1 │ B2 │ B3 │
          └────┴────┴────┘
                 │
                 │ :2181
                 ▼
        ZooKeeper Ensemble
          ┌────┬────┬────┐
          │ZK1 │ZK2 │ZK3 │
          └────┴────┴────┘
```

The Kafka broker connects to ZooKeeper for the historical coordination/metadata functionality we discussed earlier.

---

# 15. Important Ports to Remember

|     Port | Purpose                                  |
| -------: | ---------------------------------------- |
| **2181** | ZooKeeper client connections             |
| **2888** | ZooKeeper server-to-server communication |
| **3888** | ZooKeeper leader election                |

Easy memory:

```text
2181 → Client
2888 → Cluster/Peer
3888 → Election
```

---

# 16. Common Configuration Problems

### Wrong `myid`

```text
myid=2
```

but server configuration identifies it differently.

Result:

```text
❌ Ensemble problems
```

---

### Wrong hostname/DNS

```properties
server.1=zk1:2888:3888
```

but `zk1` cannot resolve.

Result:

```text
❌ ZK nodes cannot communicate
```

---

### Port blocked

If:

```text
2888 ❌
```

ZooKeeper peers can't communicate properly.

If:

```text
3888 ❌
```

leader election can fail.

---

### Only one ZK node available

```text
ZK1 ✅
ZK2 ❌
ZK3 ❌
```

```text
1/3
```

No quorum.

---

# 17. Senior Platform Engineer Mental Model

Don't memorize the entire `zoo.cfg`.

Remember this:

```text
                 ZooKeeper
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
        ZK1        ZK2        ZK3
       myid=1     myid=2     myid=3
          │          │          │
          └──────┬───┴──────────┘
                 │
              Quorum
                 │
        ┌────────┴────────┐
        ▼                 ▼
     2888               3888
     Peers             Election
                 │
                 ▼
               2181
              Clients
```

### The 6 parameters worth knowing first:

```text
tickTime
dataDir
clientPort
initLimit
syncLimit
server.X
```

And separately:

```text
myid
```

### One-line summary:

> **`zoo.cfg` defines how ZooKeeper stores its state, how servers communicate, how they elect a leader, and which servers belong to the quorum.**
