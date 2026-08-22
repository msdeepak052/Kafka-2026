# End-to-End Hands-On: 3-Node ZooKeeper Quorum on AWS

We'll build a **real 3-node ZooKeeper ensemble on AWS EC2**, verify quorum, test leader election, simulate a node failure, and connect a Kafka broker to it.

> **Important:** ZooKeeper is the **legacy Kafka architecture**. Modern Kafka uses KRaft. We're doing this hands-on specifically so you understand the architecture you just learned.

Apache recommends an odd-sized ensemble; 3 servers can tolerate 1 failure while maintaining quorum. ([Apache ZooKeeper][1])

---

# 1. What We Are Building

```text
                    AWS VPC
                       │
          ┌────────────┼────────────┐
          │            │            │
        AZ-A         AZ-B         AZ-C
          │            │            │
       ┌──────┐      ┌──────┐     ┌──────┐
       │ ZK1  │      │ ZK2  │     │ ZK3  │
       │ ID=1 │      │ ID=2 │     │ ID=3 │
       └──┬───┘      └──┬───┘     └──┬───┘
          │             │            │
          └─────────────┼────────────┘
                        │
                  ZooKeeper Quorum
                     2 / 3
                        │
                 ┌──────┴──────┐
                 │             │
              Leader        Followers
```

### Ports

```text
2181 → Client/Kafka connections
2888 → ZooKeeper peer communication
3888 → Leader election
```

These are the classic ZooKeeper ensemble ports. ([Apache ZooKeeper][2])

---

# 2. AWS Infrastructure

For learning, create:

| Node  | Hostname | Private IP example | Role        |
| ----- | -------- | ------------------ | ----------- |
| EC2-1 | `zk1`    | `10.0.1.10`        | ZK Server 1 |
| EC2-2 | `zk2`    | `10.0.2.10`        | ZK Server 2 |
| EC2-3 | `zk3`    | `10.0.3.10`        | ZK Server 3 |

Ideally:

```text
zk1 → AZ-A
zk2 → AZ-B
zk3 → AZ-C
```

The important principle is that failures should be independent; Apache specifically recommends distributing ensemble members so a shared switch/power/failure domain doesn't take out the quorum. ([Apache ZooKeeper][1])

---

# 3. EC2 Recommendation

For this lab:

```text
OS          Ubuntu 24.04 LTS
Instance    t3.small
Disk        20 GB gp3
Network     Same VPC
Private DNS Enabled
```

You don't need powerful machines for this learning cluster.

For production, ZooKeeper should have dedicated, reliable resources and especially reliable storage.

---

# 4. Security Group

Create a security group:

```text
SG: zookeeper-sg
```

### Inbound

Allow **only your Kafka/client network** to:

```text
TCP 2181
```

Allow **only the ZooKeeper nodes themselves** to:

```text
TCP 2888
TCP 3888
```

Example:

```text
                     ZK Security Group

Kafka subnet ───────► 2181

ZK1 ────────────────► 2888,3888
ZK2 ────────────────► 2888,3888
ZK3 ────────────────► 2888,3888
```

### Don't do this in production

```text
0.0.0.0/0 → 2181
0.0.0.0/0 → 2888
0.0.0.0/0 → 3888
```

ZooKeeper should **never be exposed publicly**.

---

# 5. Connect to ZK1

```bash
ssh ubuntu@<zk1-public-ip>
```

Do the same for ZK2 and ZK3.

---

# 6. Install Java

ZooKeeper runs on Java.

On **all three nodes**:

```bash
sudo apt update
sudo apt install -y openjdk-17-jre-headless wget tar
```

Verify:

```bash
java -version
```

You should get something similar to:

```text
openjdk version "17..."
```

---

# 7. Install ZooKeeper

We'll use ZooKeeper 3.9.x for this lab.

On all nodes:

```bash
cd /tmp

wget https://dlcdn.apache.org/zookeeper/zookeeper-3.9.4/apache-zookeeper-3.9.4-bin.tar.gz

tar -xzf apache-zookeeper-3.9.4-bin.tar.gz

sudo mv apache-zookeeper-3.9.4-bin /opt/zookeeper
```

Check:

```bash
ls /opt/zookeeper
```

You should see:

```text
bin
conf
lib
logs
...
```

---

# 8. Create ZooKeeper User

Do this on all three nodes:

```bash
sudo useradd --system --home /opt/zookeeper --shell /bin/bash zookeeper
```

Give ownership:

```bash
sudo chown -R zookeeper:zookeeper /opt/zookeeper
```

---

# 9. Create Data Directory

On all nodes:

```bash
sudo mkdir -p /var/lib/zookeeper
sudo chown -R zookeeper:zookeeper /var/lib/zookeeper
```

This is important because ZooKeeper stores its persistent database/snapshots and transaction information in its data directory. ([Apache ZooKeeper][3])

---

# 10. Configure `zoo.cfg`

On **all three nodes**:

```bash
sudo nano /opt/zookeeper/conf/zoo.cfg
```

Use:

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

The same ensemble membership should be known by every server. Apache documents the `server.id=host:port:port` format for this purpose. ([Apache ZooKeeper][2])

---

# 11. Understand the Configuration

### `tickTime`

```properties
tickTime=2000
```

```text
2000 ms = 2 seconds
```

Basic ZooKeeper time unit.

---

### `dataDir`

```properties
dataDir=/var/lib/zookeeper
```

Persistent ZooKeeper data.

---

### `clientPort`

```properties
clientPort=2181
```

Kafka/client connections.

---

### `initLimit`

```properties
initLimit=5
```

With:

```text
tickTime = 2 sec

5 × 2 = 10 sec
```

Used during follower initialization/synchronization.

---

### `syncLimit`

```properties
syncLimit=2
```

With:

```text
2 × 2 sec = 4 sec
```

Controls how far a follower can fall behind the leader before being considered unhealthy. ([Apache ZooKeeper][3])

---

### `server.X`

```properties
server.1=zk1:2888:3888
```

Means:

```text
server ID = 1
hostname = zk1
2888 = peer communication
3888 = leader election
```

---

# 12. Create `myid`

This is where **each node differs**.

## On ZK1

```bash
echo 1 | sudo tee /var/lib/zookeeper/myid
```

## On ZK2

```bash
echo 2 | sudo tee /var/lib/zookeeper/myid
```

## On ZK3

```bash
echo 3 | sudo tee /var/lib/zookeeper/myid
```

Verify:

```bash
cat /var/lib/zookeeper/myid
```

Expected:

```text
ZK1 → 1
ZK2 → 2
ZK3 → 3
```

The `myid` value must correspond to the `server.X` entry and be unique within the ensemble. ([Apache ZooKeeper][2])

---

# 13. Verify Hostname Resolution

This is **very important on AWS**.

From ZK1:

```bash
getent hosts zk1
getent hosts zk2
getent hosts zk3
```

You should get private IPs:

```text
10.0.1.10 zk1
10.0.2.10 zk2
10.0.3.10 zk3
```

If this doesn't work, fix DNS/`/etc/hosts` before continuing.

---

# 14. Quick Lab Option: `/etc/hosts`

For a simple learning lab, you can put this on all three:

```bash
sudo nano /etc/hosts
```

Add:

```text
10.0.1.10 zk1
10.0.2.10 zk2
10.0.3.10 zk3
```

Then:

```bash
ping -c 2 zk1
ping -c 2 zk2
ping -c 2 zk3
```

---

# 15. Test Network Connectivity

From ZK1:

```bash
nc -zv zk2 2888
nc -zv zk3 2888
```

And:

```bash
nc -zv zk2 3888
nc -zv zk3 3888
```

If you see:

```text
succeeded
```

the security group/network path is working.

Do this between all nodes.

---

# 16. Create systemd Service

Instead of manually running ZooKeeper, we'll run it as a proper Linux service.

On all nodes:

```bash
sudo nano /etc/systemd/system/zookeeper.service
```

Use:

```ini
[Unit]
Description=Apache ZooKeeper
After=network.target

[Service]
Type=simple
User=zookeeper
Group=zookeeper

ExecStart=/opt/zookeeper/bin/zkServer.sh start-foreground /opt/zookeeper/conf/zoo.cfg

Restart=on-failure
RestartSec=5

LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

---

# 17. Enable Service

On all nodes:

```bash
sudo systemctl daemon-reload
```

Then:

```bash
sudo systemctl enable zookeeper
```

---

# 18. Start the Ensemble

Start them one at a time.

### ZK1

```bash
sudo systemctl start zookeeper
```

### ZK2

```bash
sudo systemctl start zookeeper
```

### ZK3

```bash
sudo systemctl start zookeeper
```

---

# 19. Check Service

On each node:

```bash
sudo systemctl status zookeeper
```

You want:

```text
Active: active (running)
```

---

# 20. Check Logs

Very important for Platform Engineers:

```bash
sudo journalctl -u zookeeper -f
```

Or:

```bash
sudo journalctl -u zookeeper --no-pager -n 100
```

Look for:

```text
LEADING
FOLLOWING
```

You should eventually have:

```text
ZK1 → LEADING
ZK2 → FOLLOWING
ZK3 → FOLLOWING
```

The actual leader can be any of the three.

---

# 21. Check ZooKeeper Status

Run:

```bash
/opt/zookeeper/bin/zkServer.sh status /opt/zookeeper/conf/zoo.cfg
```

You should get something similar to:

```text
ZooKeeper JMX enabled by default
Using config: /opt/zookeeper/conf/zoo.cfg
Mode: leader
```

On another node:

```text
Mode: follower
```

---

# 22. Verify the Quorum

Our expected state:

```text
             ZooKeeper Ensemble

                  ZK1
                LEADER
               /      \
              /        \
            ZK2        ZK3
         FOLLOWER    FOLLOWER

             2 / 3 quorum
```

Remember:

```text
3 nodes
quorum = 2
```

So:

```text
3/3 → Healthy
2/3 → Healthy
1/3 → No quorum
```

---

# 23. Test Client Connection

From any node:

```bash
/opt/zookeeper/bin/zkCli.sh -server zk1:2181
```

You should enter the ZooKeeper CLI.

Try:

```text
ls /
```

You may see something like:

```text
[zookeeper]
```

The official ZooKeeper guide uses `zkCli.sh -server host:2181` for client testing. ([Apache ZooKeeper][3])

---

# 24. Create a Test ZNode

Inside the CLI:

```text
create /platform "hello-kafka"
```

Then:

```text
get /platform
```

Expected:

```text
hello-kafka
```

Now:

```text
ls /
```

You should see:

```text
[zookeeper, platform]
```

---

# 25. Why Is This Important?

You just proved:

```text
Client
  │
  ▼
ZooKeeper
  │
  ▼
ZooKeeper data tree
```

Now let's prove that the data is replicated across the quorum.

---

# 26. Check From Another Node

Connect through ZK2:

```bash
/opt/zookeeper/bin/zkCli.sh -server zk2:2181
```

Run:

```text
get /platform
```

You should get:

```text
hello-kafka
```

Again through ZK3:

```bash
/opt/zookeeper/bin/zkCli.sh -server zk3:2181
```

```text
get /platform
```

You should also see:

```text
hello-kafka
```

That's the distributed state/replication concept in action.

---

# 27. Failure Test — Kill One Node

This is the **most important hands-on test**.

Suppose:

```text
ZK1 → Leader
ZK2 → Follower
ZK3 → Follower
```

Stop ZK1:

```bash
sudo systemctl stop zookeeper
```

Now:

```text
ZK1 ❌

ZK2 ✅
ZK3 ✅
```

Quorum:

```text
2 / 3
```

Therefore:

```text
QUORUM STILL EXISTS
```

---

# 28. Watch the New Leader

On ZK2:

```bash
/opt/zookeeper/bin/zkServer.sh status /opt/zookeeper/conf/zoo.cfg
```

On ZK3:

```bash
/opt/zookeeper/bin/zkServer.sh status /opt/zookeeper/conf/zoo.cfg
```

One should become:

```text
Mode: leader
```

and the other:

```text
Mode: follower
```

So:

```text
Before:

ZK1 → Leader
ZK2 → Follower
ZK3 → Follower


After ZK1 failure:

ZK1 ❌
ZK2 → Leader
ZK3 → Follower
```

The exact new leader could be ZK2 or ZK3.

---

# 29. Verify Data After Leader Failure

Connect to the surviving leader:

```bash
/opt/zookeeper/bin/zkCli.sh -server zk2:2181
```

Then:

```text
get /platform
```

Expected:

```text
hello-kafka
```

This demonstrates:

```text
Leader failure
      ↓
Leader election
      ↓
Quorum survives
      ↓
Data remains available
```

---

# 30. Now Stop Another Node

Suppose:

```text
ZK1 ❌
ZK2 → Leader
ZK3 → Follower
```

Stop ZK3:

```bash
sudo systemctl stop zookeeper
```

Now:

```text
ZK1 ❌
ZK2 ✅
ZK3 ❌
```

Only:

```text
1 / 3
```

remains.

Therefore:

```text
NO QUORUM
```

This is the key hands-on demonstration of quorum.

---

# 31. The Whole Failure Experiment

```text
START

ZK1 ✅
ZK2 ✅
ZK3 ✅

3/3
   ↓
Healthy


STOP ZK1

ZK1 ❌
ZK2 ✅
ZK3 ✅

2/3
   ↓
Healthy
   ↓
New leader elected


STOP ZK3

ZK1 ❌
ZK2 ✅
ZK3 ❌

1/3
   ↓
NO QUORUM
```

This is exactly why you deployed **3 nodes instead of 1**.

---

# 32. Recover the Cluster

Start ZK1:

```bash
sudo systemctl start zookeeper
```

Then start ZK3:

```bash
sudo systemctl start zookeeper
```

Check:

```bash
/opt/zookeeper/bin/zkServer.sh status /opt/zookeeper/conf/zoo.cfg
```

Eventually:

```text
1 Leader
2 Followers
```

again.

---

# 33. Connect Kafka to This ZooKeeper Ensemble

Now we connect this to what you've already learned.

Historical Kafka broker configuration:

```properties
zookeeper.connect=zk1:2181,zk2:2181,zk3:2181
```

Architecture becomes:

```text
                       Kafka
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
       Broker 1       Broker 2       Broker 3
          │              │              │
          └──────────────┼──────────────┘
                         │
                  ZooKeeper Clients
                         │
                         ▼
              ┌─────────────────────┐
              │ ZooKeeper Ensemble  │
              │                     │
              │ ZK1 → Leader        │
              │ ZK2 → Follower      │
              │ ZK3 → Follower      │
              └─────────────────────┘
```

---

# 34. Important: ZooKeeper Does NOT Store Kafka Messages

Your architecture is:

```text
Kafka
 │
 ├── Messages
 ├── Topics
 ├── Partitions
 ├── Segments
 └── Replicas
```

while ZooKeeper historically handled:

```text
ZooKeeper
 │
 ├── Coordination
 ├── Broker registration
 ├── Controller coordination
 └── Cluster metadata/state
```

Don't confuse the two.

---

# 35. Production AWS Architecture

For a real production design, I'd think:

```text
                     AWS Region
                         │
       ┌─────────────────┼─────────────────┐
       │                 │                 │
      AZ-A              AZ-B              AZ-C
       │                 │                 │
   ┌────────┐        ┌────────┐        ┌────────┐
   │   ZK1  │        │   ZK2  │        │   ZK3  │
   │ Leader │        │Follower│        │Follower│
   └────────┘        └────────┘        └────────┘
       │                 │                 │
       └─────────────────┼─────────────────┘
                         │
                   Private Network
                         │
                         ▼
                 Kafka Brokers
```

### Critical infrastructure considerations

```text
Separate AZs
      +
Private subnets
      +
Private DNS
      +
Restricted Security Groups
      +
Persistent storage
      +
Monitoring
```

---

# 36. What You Should Check as a Platform Engineer

Don't stop at:

```bash
systemctl status zookeeper
```

Your validation checklist should be:

```text
✓ Java installed
✓ ZooKeeper process running
✓ Correct myid
✓ DNS resolution
✓ Port 2181 reachable
✓ Port 2888 reachable between ZK nodes
✓ Port 3888 reachable between ZK nodes
✓ Leader elected
✓ 3-node quorum healthy
✓ Client connection works
✓ ZNode creation works
✓ Data visible through another node
✓ One-node failure tolerated
✓ Leader re-election works
✓ Two-node failure causes loss of quorum
✓ Recovery works
```

---

# 37. Troubleshooting Quick Map

| Problem                          | First thing to check                   |
| -------------------------------- | -------------------------------------- |
| ZK won't start                   | `journalctl -u zookeeper`              |
| No leader                        | 2888/3888 connectivity                 |
| Nodes can't see each other       | DNS / `/etc/hosts`                     |
| Wrong server identity            | `myid`                                 |
| Kafka can't connect              | TCP 2181 + SG                          |
| Quorum unavailable               | Number of healthy nodes                |
| Frequent follower loss           | `tickTime`, `syncLimit`, network       |
| Data directory errors            | `/var/lib/zookeeper` ownership/storage |
| Cluster starts but behaves oddly | Check all `server.X` definitions match |

---

# 38. The Architecture You Should Be Able to Draw in an Interview

```text
                         AWS VPC
                            │
              ┌─────────────┼─────────────┐
              │             │             │
             AZ-A          AZ-B          AZ-C
              │             │             │
           ┌──────┐      ┌──────┐      ┌──────┐
           │ ZK1  │      │ ZK2  │      │ ZK3  │
           │ ID=1 │      │ ID=2 │      │ ID=3 │
           └──┬───┘      └──┬───┘      └──┬───┘
              │             │             │
              └─────────────┼─────────────┘
                            │
                       3-node quorum
                            │
                       2 required
                            │
                    ┌───────┴───────┐
                    │               │
                  Leader         Followers
                    │               │
                    └───────┬───────┘
                            │
                         :2181
                            │
                            ▼
                     Kafka Brokers
```

### The key hands-on takeaway

```text
3 ZooKeeper nodes
       ↓
2 nodes = quorum
       ↓
Can tolerate 1 failure
       ↓
Leader automatically re-elected
       ↓
Lose 2 nodes
       ↓
1/3 = no quorum
```

That is the **core of the entire lab**. Once you understand and actually test that failure sequence, ZooKeeper quorum stops being a theoretical concept and becomes something you can reason about operationally.

[1]: https://zookeeper.apache.org/doc/r3.3.3/zookeeperAdmin.pdf?utm_source=chatgpt.com "ZooKeeper Administrator’s Guide"
[2]: https://zookeeper.apache.org/doc/r3.1.2/zookeeperAdmin.html?utm_source=chatgpt.com "ZooKeeper Administrator's Guide"
[3]: https://zookeeper.apache.org/doc/r3.7.2/zookeeperAdmin.html?utm_source=chatgpt.com "ZooKeeper: Because Coordinating Distributed Systems is a Zoo"
