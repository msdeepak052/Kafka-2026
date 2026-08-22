# Kafka CLI — Complete Installation, Architecture & Usage Guide

Since you're learning Kafka **chronologically from a Senior Platform Engineer perspective**, this consolidated version combines the previous **installation guide + CLI architecture + how the CLI communicates with Kafka + important commands + KRaft relationship + hands-on lab**.

The key distinction throughout this guide is:

> **Kafka CLI is a client-side administration/testing tool. It is not a Kafka broker and it is not a KRaft controller.**

---

# 1. What is Kafka CLI?

Kafka provides a collection of command-line tools with the Kafka distribution.

These tools allow a Platform Engineer to:

* Create/manage topics
* Produce test messages
* Consume messages
* Inspect consumer groups
* Check consumer lag
* Manage configurations
* Manage ACLs
* Inspect KRaft metadata quorum
* Initialize Kafka storage
* Inspect broker capabilities
* Troubleshoot Kafka

Examples:

```text
kafka-topics.sh
kafka-console-producer.sh
kafka-console-consumer.sh
kafka-consumer-groups.sh
kafka-configs.sh
kafka-acls.sh
kafka-metadata-quorum.sh
kafka-storage.sh
kafka-broker-api-versions.sh
kafka-cluster.sh
```

---

# 2. Where Does Kafka CLI Run?

Kafka CLI runs on a **client machine**.

For your setup:

```text
Ubuntu Desktop
     │
     ├── Java
     │
     └── Kafka CLI
          │
          ├── kafka-topics.sh
          ├── kafka-console-producer.sh
          ├── kafka-console-consumer.sh
          ├── kafka-consumer-groups.sh
          └── ...
```

It then communicates with a Kafka cluster over the network.

---

# 3. Overall Kafka CLI Architecture

This is the architecture you should remember:

```text
                    YOUR UBUNTU MACHINE
                 ┌──────────────────────┐
                 │                      │
                 │      Kafka CLI       │
                 │                      │
                 │ kafka-topics.sh      │
                 │ kafka-console-       │
                 │ producer.sh          │
                 │ kafka-console-       │
                 │ consumer.sh         │
                 │ kafka-consumer-      │
                 │ groups.sh            │
                 │ kafka-configs.sh     │
                 │                      │
                 └──────────┬───────────┘
                            │
                            │ --bootstrap-server
                            ▼
                    ┌───────────────┐
                    │ Kafka Broker  │
                    │    :9092      │
                    └───────┬───────┘
                            │
                  Cluster Metadata
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
       Kafka Brokers                KRaft Controllers
       ┌─────────────┐             ┌─────────────────┐
       │ B1 B2 B3... │             │ C1 C2 C3        │
       └──────┬──────┘             └────────┬────────┘
              │                              │
              ▼                              ▼
       Topics / Partitions             Raft Quorum
       Actual Event Data               Metadata Log
```

### Three important layers:

```text
CLI
 ↓
Client

Broker
 ↓
Data Plane

KRaft Controller
 ↓
Control Plane
```

---

# 4. Very Important — CLI Is Not a Kafka Server

Installing Kafka CLI does **not** mean you have started a Kafka cluster.

You can have:

```text
Ubuntu
 └── Kafka CLI
```

without having:

```text
Kafka Broker
KRaft Controller
```

running.

For example:

```bash
kafka-topics.sh --help
```

can work even when Kafka is completely stopped.

But:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

requires a reachable Kafka broker.

---

# 5. How Does `--bootstrap-server` Fit?

You've already learned `--bootstrap-server`.

It is the **initial Kafka broker endpoint** the CLI uses to connect to the cluster and obtain metadata.

Example:

```bash
kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --list
```

Flow:

```text
Kafka CLI
    │
    │ initial connection
    ▼
Kafka Broker
    │
    │ metadata
    ▼
Kafka Cluster
```

---

# 6. Bootstrap Server Is NOT a Special Server

Suppose your cluster has:

```text
B1
B2
B3
```

You choose:

```text
B1:9092
```

as the bootstrap address.

B1 does **not** become a special "bootstrap broker."

It is still simply:

```text
Kafka Broker
```

You're telling the CLI:

> **"Start your connection here."**

---

# 7. Why Can We Specify Only One Broker?

Suppose:

```text
Kafka Cluster

B1
B2
B3
```

You execute:

```bash
--bootstrap-server B1:9092
```

The CLI initially connects to B1.

It can then obtain metadata such as:

```text
Broker 1
Broker 2
Broker 3

Topics
Partitions
Partition leaders
Replicas
```

Conceptually:

```text
CLI
 │
 ▼
B1
 │
 │ Metadata
 ▼
B1 ─ B2 ─ B3
```

Therefore you don't need to know every broker before starting.

---

# 8. Why Use Multiple Bootstrap Servers?

In production, you commonly provide multiple endpoints:

```bash
--bootstrap-server \
kafka-1:9092,kafka-2:9092,kafka-3:9092
```

Architecture:

```text
                    Kafka CLI
                  /     |     \
                 ▼      ▼      ▼
                B1     B2     B3
```

If:

```text
B1 ❌
```

the client has other initial connection options.

### Important:

Multiple bootstrap servers do **not** mean:

> "Send every request to all three."

They provide multiple possible **initial entry points**.

---

# 9. Bootstrap Server ≠ Partition Leader

This is an important distinction.

Suppose:

```text
orders-P0 → Leader B2
```

You run:

```bash
kafka-console-producer.sh \
  --bootstrap-server B1:9092 \
  --topic orders
```

The producer can initially connect to:

```text
B1
```

Then metadata tells it:

```text
orders-P0 → B2
```

So the producer can communicate with:

```text
B2
```

for that partition.

```text
Producer
    │
    ▼
B1
 │
 │ metadata
 ▼
P0 Leader = B2
 │
 ▼
B2
 │
 ▼
P0
```

---

# 10. Bootstrap Server ≠ KRaft Controller

With KRaft:

```text
Controllers:

C1
C2
C3
```

and:

```text
Brokers:

B1
B2
B3
```

For normal Kafka CLI commands:

```text
CLI
 ↓
Broker listener
```

not:

```text
CLI
 ↓
Controller listener
```

The controller is part of Kafka's **control plane**.

The broker provides the normal client-facing Kafka interface.

---

# 11. Kafka CLI and KRaft

Now connect this to what you learned about KRaft.

```text
                         Kafka Cluster
                              │
               ┌──────────────┴──────────────┐
               │                             │
               ▼                             ▼
          DATA PLANE                    CONTROL PLANE
               │                             │
               ▼                             ▼
          Kafka Brokers                KRaft Controllers
               │                             │
               ▼                             ▼
       Topics / Partitions              Raft Quorum
       Actual Events                    Metadata Log
```

CLI:

```text
Kafka CLI
    │
    ▼
Kafka Broker
```

The broker and controller work together behind the scenes to maintain the cluster.

---

# 12. Installing Kafka CLI on Ubuntu

## Step 1 — Check Java

Run:

```bash
java -version
```

If Java isn't installed:

```bash
sudo apt update
sudo apt install -y openjdk-21-jdk
```

Verify:

```bash
java -version
```

You should see Java 21.

---

# 13. Why Java?

Kafka is a JVM-based application.

The Kafka CLI tools are Java-based programs packaged with shell launchers.

For example:

```text
kafka-topics.sh
      │
      ▼
Kafka Java CLI implementation
      │
      ▼
Kafka cluster
```

---

# 14. Download Apache Kafka

Create a directory:

```bash
mkdir -p ~/kafka
cd ~/kafka
```

Download the Kafka version you want from the official Apache Kafka download page.

For example:

```bash
wget https://downloads.apache.org/kafka/4.0.1/kafka_2.13-4.0.1.tgz
```

Extract:

```bash
tar -xzf kafka_2.13-4.0.1.tgz
```

Then:

```bash
cd kafka_2.13-4.0.1
```

> Use the exact version you intend to run. For a real environment, keep your CLI/client version aligned with the Kafka cluster version where practical.

---

# 15. Kafka Installation Structure

Run:

```bash
ls
```

You'll see something similar to:

```text
LICENSE
NOTICE
README.md
bin
config
libs
licenses
```

The important directories:

```text
Kafka
│
├── bin/
│   └── CLI tools
│
├── config/
│   └── Kafka configuration
│
├── libs/
│   └── Kafka libraries
│
└── licenses/
```

---

# 16. The `bin/` Directory

Run:

```bash
ls bin/kafka*.sh
```

You'll find tools such as:

```text
kafka-topics.sh
kafka-console-producer.sh
kafka-console-consumer.sh
kafka-consumer-groups.sh
kafka-configs.sh
kafka-acls.sh
kafka-metadata-quorum.sh
kafka-storage.sh
kafka-broker-api-versions.sh
kafka-cluster.sh
kafka-dump-log.sh
```

---

# 17. Test the Installation

Run:

```bash
bin/kafka-topics.sh --help
```

If you get the command help:

```text
Create, delete, describe, or change a topic.
...
```

your CLI is installed correctly.

---

# 18. Make Kafka CLI Available Globally

Currently you might need:

```bash
~/kafka/kafka_2.13-4.0.1/bin/kafka-topics.sh
```

Instead, you want:

```bash
kafka-topics.sh
```

from anywhere.

Set:

```bash
echo 'export KAFKA_HOME=$HOME/kafka/kafka_2.13-4.0.1' >> ~/.bashrc
```

Then:

```bash
echo 'export PATH=$PATH:$KAFKA_HOME/bin' >> ~/.bashrc
```

Reload:

```bash
source ~/.bashrc
```

Test:

```bash
kafka-topics.sh --help
```

---

# 19. Verify Your PATH

Run:

```bash
which kafka-topics.sh
```

You should get something like:

```text
/home/<user>/kafka/kafka_2.13-4.0.1/bin/kafka-topics.sh
```

Also:

```bash
echo $KAFKA_HOME
```

should show:

```text
/home/<user>/kafka/kafka_2.13-4.0.1
```

---

# 20. If You're Using Fish Shell

If your shell is fish:

```bash
echo $SHELL
```

and it returns something like:

```text
/usr/bin/fish
```

use:

```fish
set -Ux KAFKA_HOME $HOME/kafka/kafka_2.13-4.0.1
fish_add_path $KAFKA_HOME/bin
```

Then:

```fish
kafka-topics.sh --help
```

---

# 21. Important Kafka CLI Commands

## Topic Administration

```text
kafka-topics.sh
```

Used for:

```text
Create
List
Describe
Alter
Delete
```

Example:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

---

# 22. Producer CLI

```text
kafka-console-producer.sh
```

Example:

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders
```

Architecture:

```text
CLI
 │
 │ Producer request
 ▼
Broker
 │
 ▼
Partitioner
 │
 ▼
Topic
 ├── P0
 ├── P1
 └── P2
```

---

# 23. Consumer CLI

```text
kafka-console-consumer.sh
```

Example:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning
```

Architecture:

```text
Topic
 ├── P0
 ├── P1
 └── P2
      │
      ▼
    Broker
      │
      ▼
Consumer CLI
```

---

# 24. Consumer Groups CLI

```text
kafka-consumer-groups.sh
```

Example:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

You can inspect:

```text
GROUP
TOPIC
PARTITION
CURRENT-OFFSET
LOG-END-OFFSET
LAG
```

This becomes extremely important for Platform Engineer troubleshooting.

---

# 25. Configuration CLI

```text
kafka-configs.sh
```

Used for Kafka configuration management.

For example, topic/broker-level configurations.

```bash
kafka-configs.sh \
  --bootstrap-server localhost:9092 \
  ...
```

You'll learn the exact configuration syntax later.

---

# 26. ACL CLI

```text
kafka-acls.sh
```

Used to manage Kafka authorization rules.

Conceptually:

```text
User
 │
 ▼
ACL
 │
 ├── Topic
 ├── Consumer Group
 └── Operation
```

Don't jump into ACL details yet—we'll cover them when they appear in your learning sequence.

---

# 27. KRaft Metadata Quorum CLI

This is particularly relevant to what you've just learned.

```text
kafka-metadata-quorum.sh
```

Example:

```bash
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

This helps inspect:

```text
Controller quorum
Current leader
Current state
Epoch
High watermark
```

And:

```bash
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --replication
```

can be used to inspect metadata replication.

---

# 28. Storage CLI

```text
kafka-storage.sh
```

Used for KRaft storage-related operations.

One important operation during a KRaft setup is generating a cluster ID:

```bash
kafka-storage.sh random-uuid
```

and formatting storage:

```bash
kafka-storage.sh format ...
```

This is part of the KRaft initialization process.

---

# 29. Broker API Versions

```text
kafka-broker-api-versions.sh
```

Useful for checking what APIs/version capabilities a broker supports.

Example:

```bash
kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092
```

Useful during:

* Version compatibility checks
* Upgrades
* Troubleshooting client/broker compatibility

---

# 30. CLI Tools — What You Should Learn First

Don't try to learn everything at once.

For your current sequence, prioritize:

```text
1. kafka-topics.sh
2. kafka-console-producer.sh
3. kafka-console-consumer.sh
4. kafka-consumer-groups.sh
5. kafka-configs.sh
6. kafka-metadata-quorum.sh
7. kafka-storage.sh
8. kafka-broker-api-versions.sh
```

Later:

```text
kafka-acls.sh
kafka-cluster.sh
kafka-dump-log.sh
```

---

# 31. Local KRaft Lab Architecture

If you want to practice locally on Ubuntu, you can run a single-node Kafka environment.

Conceptually:

```text
                 Ubuntu
                   │
       ┌───────────┴───────────┐
       │                       │
       ▼                       ▼
   Kafka CLI              Kafka Server
       │                       │
       │ localhost:9092        │
       └───────────────────────┘
                   │
                   ▼
          KRaft Controller
                   │
                   ▼
             Metadata Log
```

For a simple development setup:

```text
Kafka Node 1
├── Broker
└── Controller
```

This is **combined mode**.

---

# 32. Generate KRaft Cluster ID

For the corresponding Kafka KRaft configuration:

```bash
KAFKA_CLUSTER_ID="$(kafka-storage.sh random-uuid)"
```

Check:

```bash
echo $KAFKA_CLUSTER_ID
```

You'll get a generated ID.

---

# 33. Format KRaft Storage

Using the configuration appropriate to your Kafka version:

```bash
kafka-storage.sh format \
  -t "$KAFKA_CLUSTER_ID" \
  -c "$KAFKA_HOME/config/server.properties"
```

The exact configuration filename can differ depending on the Kafka distribution/version, so always check the `config/` directory of the version you installed.

---

# 34. Start Kafka

For a matching KRaft configuration:

```bash
kafka-server-start.sh \
  "$KAFKA_HOME/config/server.properties"
```

Keep that terminal running.

---

# 35. Test Kafka CLI

Open another terminal.

Run:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

If Kafka is running and there are no topics, you may get no output.

That is normal.

---

# 36. Create a Topic

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic test-topic \
  --partitions 3 \
  --replication-factor 1
```

Expected:

```text
Created topic test-topic.
```

---

# 37. List Topics

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Output:

```text
test-topic
```

---

# 38. Describe the Topic

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic test-topic
```

You may see:

```text
Topic: test-topic
PartitionCount: 3
ReplicationFactor: 1

Partition: 0
Leader: 1
Replicas: 1
Isr: 1

Partition: 1
Leader: 1
Replicas: 1
Isr: 1

Partition: 2
Leader: 1
Replicas: 1
Isr: 1
```

This lets you visually connect your previous learning:

```text
Topic
 ↓
Partitions
 ↓
Leader
 ↓
Replicas
 ↓
ISR
```

---

# 39. Produce Messages

```bash
kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic
```

Enter:

```text
hello kafka
hello platform engineer
hello kraft
```

Each line becomes a Kafka record.

Architecture:

```text
Console Producer
       │
       ▼
Kafka Broker
       │
       ▼
Partition
       │
       ▼
Kafka Log
```

---

# 40. Consume Messages

Open another terminal:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --from-beginning
```

You should see:

```text
hello kafka
hello platform engineer
hello kraft
```

You've now tested:

```text
Producer
   ↓
Broker
   ↓
Topic
   ↓
Partition
   ↓
Consumer
```

---

# 41. Test Consumer Group

Run:

```bash
kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic test-topic \
  --group test-group
```

Then:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group test-group
```

This connects your CLI practice to the concepts you've already learned:

```text
Consumer Group
      ↓
Partition Assignment
      ↓
Offsets
      ↓
Lag
```

---

# 42. Test KRaft Quorum

Run:

```bash
kafka-metadata-quorum.sh \
  --bootstrap-server localhost:9092 \
  describe --status
```

This lets you inspect the KRaft metadata quorum.

You're now connecting:

```text
KRaft
 ↓
Controller
 ↓
Metadata Quorum
 ↓
Metadata Log
 ↓
Kafka CLI
```

---

# 43. Important Architecture — End-to-End

Here's the diagram I would keep in your notes:

```text
                         PLATFORM ENGINEER
                                │
                                ▼
                         ┌──────────────┐
                         │  Kafka CLI   │
                         │              │
                         │ topics       │
                         │ producer     │
                         │ consumer     │
                         │ groups       │
                         │ configs      │
                         │ quorum       │
                         └──────┬───────┘
                                │
                       --bootstrap-server
                                │
                                ▼
                     ┌───────────────────┐
                     │   Kafka Broker    │
                     │     :9092         │
                     └─────────┬─────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                    ▼                     ▼
             DATA PLANE              CONTROL PLANE
                    │                     │
                    ▼                     ▼
             Kafka Brokers          KRaft Controllers
                    │                ┌────┬────┬────┐
                    │                │ C1 │ C2 │ C3 │
                    │                └────┴────┴────┘
                    │                     │
                    │                     ▼
                    │                Raft Quorum
                    │                     │
                    │                     ▼
                    │                Metadata Log
                    │
                    ▼
             Topics / Partitions
                    │
                    ▼
             Actual Kafka Events
```

---

# 44. The Most Important Mental Model

When you run:

```bash
kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --list
```

think:

```text
CLI
 │
 │ "I need to connect to Kafka."
 ▼
Bootstrap Broker
 │
 │ "Here is the cluster metadata."
 ▼
Kafka Metadata
 │
 ├── Brokers
 ├── Topics
 ├── Partitions
 ├── Leaders
 └── Replicas
 │
 ▼
CLI performs requested operation
```

And behind the scenes:

```text
KRaft Controllers
       │
       ▼
Raft Quorum
       │
       ▼
Metadata Log
       │
       ▼
Authoritative Cluster Metadata
```

---

# 45. CLI vs Broker vs Controller

This table is worth memorizing:

| Component            | Role                         | Example                 |
| -------------------- | ---------------------------- | ----------------------- |
| **Kafka CLI**        | Client/admin tool            | `kafka-topics.sh`       |
| **Kafka Broker**     | Data plane + client endpoint | `broker-1:9092`         |
| **KRaft Controller** | Metadata/control plane       | `controller-1:9093`     |
| **Topic**            | Logical event stream         | `orders`                |
| **Partition**        | Ordered log                  | `orders-0`              |
| **Metadata Log**     | Cluster metadata history     | KRaft internal metadata |

---

# 46. CLI Installation vs Cluster Installation

Don't mix these two.

### CLI installation

```text
Ubuntu
 ↓
Java
 ↓
Kafka distribution
 ↓
bin/*.sh
```

### Kafka cluster

```text
Kafka Brokers
+
KRaft Controllers
+
Storage
+
Network listeners
```

You can install the CLI without running a cluster.

---

# 47. Production Architecture

A real Platform Engineer environment may look like:

```text
                        Admin / Platform Engineer
                                  │
                                  ▼
                            Kafka CLI
                                  │
                     bootstrap-server
                                  │
                                  ▼
                    ┌───────────────────────┐
                    │    Kafka Brokers      │
                    │                       │
                    │ B1   B2   B3   B4...  │
                    └──────────┬────────────┘
                               │
                               │ Control Plane
                               ▼
                    ┌───────────────────────┐
                    │   KRaft Controllers   │
                    │                       │
                    │ C1   C2   C3          │
                    └──────────┬────────────┘
                               │
                               ▼
                         Raft Metadata
                            Quorum
                               │
                               ▼
                         Metadata Log
```

You don't need to expose the controller listener to your application clients just because you're using KRaft.

---

# 48. What a Senior Platform Engineer Should Use CLI For

Your daily troubleshooting toolkit eventually becomes:

```text
Problem
  │
  ├── Topic problem
  │      └── kafka-topics.sh
  │
  ├── Message testing
  │      ├── kafka-console-producer.sh
  │      └── kafka-console-consumer.sh
  │
  ├── Consumer lag
  │      └── kafka-consumer-groups.sh
  │
  ├── Configuration
  │      └── kafka-configs.sh
  │
  ├── Authorization
  │      └── kafka-acls.sh
  │
  ├── KRaft problem
  │      └── kafka-metadata-quorum.sh
  │
  ├── Storage initialization
  │      └── kafka-storage.sh
  │
  └── Broker compatibility
         └── kafka-broker-api-versions.sh
```

---

# 49. First Commands to Memorize

Don't memorize every option yet.

Start with:

```bash
# Topics
kafka-topics.sh

# Produce
kafka-console-producer.sh

# Consume
kafka-console-consumer.sh

# Consumer groups
kafka-consumer-groups.sh

# Configuration
kafka-configs.sh

# KRaft
kafka-metadata-quorum.sh

# Storage
kafka-storage.sh

# Broker capabilities
kafka-broker-api-versions.sh
```

Then learn their important options as you encounter them.

---

# 50. Your Kafka CLI Learning Flow

Since we're maintaining chronology, I recommend you understand the CLI in this order:

```text
1. CLI Architecture
        ↓
2. Installation
        ↓
3. --bootstrap-server
        ↓
4. kafka-topics.sh
        ↓
5. Create/List/Describe Topics
        ↓
6. console-producer
        ↓
7. console-consumer
        ↓
8. Consumer Groups CLI
        ↓
9. Consumer Lag
        ↓
10. kafka-configs.sh
        ↓
11. kafka-acls.sh
        ↓
12. KRaft Metadata Quorum CLI
        ↓
13. Kafka troubleshooting commands
```

That keeps the CLI concepts connected to the Kafka concepts you've already covered instead of jumping into future administration topics.

---

# 🔥 Final Cheat Sheet

```text
Kafka CLI
│
├── Runs on client/admin machine
│
├── NOT a broker
│
├── NOT a controller
│
└── Connects to Kafka
        │
        ▼
   --bootstrap-server
        │
        ▼
   Kafka Broker
        │
        ▼
   Cluster Metadata
        │
        ├── Brokers
        ├── Topics
        ├── Partitions
        ├── Leaders
        └── Replicas
```

And underneath the modern Kafka cluster:

```text
                 Kafka
                   │
        ┌──────────┴──────────┐
        ▼                     ▼
     Brokers              KRaft Controllers
        │                     │
        ▼                     ▼
   Actual Data            Raft Quorum
                              │
                              ▼
                         Metadata Log
```

### The one sentence to remember:

> **Kafka CLI is a collection of client-side administration and testing tools that connect through a Kafka broker using `--bootstrap-server`; the broker exposes the Kafka client interface, while KRaft controllers maintain the cluster's control-plane metadata.**

This is the **consolidated Kafka CLI foundation** you should keep before moving into individual CLI commands.
