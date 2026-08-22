# Apache ZooKeeper — Complete Hands-On AWS Guide

> **Scope:** Sections 12–23 from your course, consolidated into one practical document.
>
> **Goal:** Build a real 3-node ZooKeeper quorum on AWS, understand every important configuration, operate it, troubleshoot it, test failures, and understand the Docker/Web-tool alternatives.

Your previous document establishes the 3-node AWS quorum, ports, installation, `zoo.cfg`, `myid`, systemd, CLI, failure testing and Kafka connectivity. 

---

# 1. What We Are Building

## Architecture

```text
                         AWS REGION
                             │
              ┌──────────────┼──────────────┐
              │              │              │
             AZ-A           AZ-B           AZ-C
              │              │              │
          ┌───────┐      ┌───────┐      ┌───────┐
          │  ZK1  │      │  ZK2  │      │  ZK3  │
          │ ID=1  │      │ ID=2  │      │ ID=3  │
          └───┬───┘      └───┬───┘      └───┬───┘
              │              │              │
              └──────────────┼──────────────┘
                             │
                        QUORUM = 2/3
                             │
                   ┌─────────┴─────────┐
                   │                   │
                LEADER             FOLLOWERS
                   │
                   │ :2181
                   ▼
              Kafka Brokers
```

### Why 3 nodes?

```text
3 nodes
  ↓
Majority = 2
  ↓
Can lose 1 node
  ↓
Still operational
```

If:

```text
ZK1 ❌
ZK2 ✅
ZK3 ✅
```

you still have:

```text
2/3 → quorum
```

But:

```text
ZK1 ❌
ZK2 ❌
ZK3 ✅
```

gives:

```text
1/3 → no quorum
```

---

# 2. Important Ports

|     Port | Purpose                                  |
| -------: | ---------------------------------------- |
| **2181** | Client connections                       |
| **2888** | ZooKeeper server-to-server communication |
| **3888** | Leader election                          |
| **8080** | AdminServer HTTP interface               |

The official Docker image exposes these four ports as the standard client, follower/peer, election and AdminServer ports. ([Docker Hub][2])

### Easy memory

```text
2181 → Client
2888 → Peer
3888 → Election
8080 → Admin UI/API
```

---

# 3. AWS Infrastructure

For the learning environment:

| Node | Hostname | AZ   | Example private IP |
| ---- | -------- | ---- | ------------------ |
| ZK1  | `zk1`    | AZ-A | `10.0.1.10`        |
| ZK2  | `zk2`    | AZ-B | `10.0.2.10`        |
| ZK3  | `zk3`    | AZ-C | `10.0.3.10`        |

### EC2

For learning:

```text
Ubuntu 24.04
t3.small
20 GB gp3
Private networking
```

You don't need large instances for this lab.

### Production

Think:

```text
Separate AZs
+
Reliable storage
+
Private subnets
+
Private DNS
+
Restricted SG
+
Monitoring
```

The key is **failure-domain separation**.

If all three are in one AZ:

```text
AZ-A
 ├── ZK1
 ├── ZK2
 └── ZK3

AZ-A failure
     ↓
ALL 3 LOST
```

So the quorum design becomes useless against an AZ failure.

---

# 4. AWS Security Group + Actually Provisioning the 3 Nodes

The previous section described the *target* architecture. This section is
the part that was missing before: the actual commands that create it. This
uses the **AWS CLI v2** (`aws --version` to confirm it's installed and
`aws configure` to confirm credentials/region are set) — every step also has
a one-line AWS Console equivalent noted, if you'd rather click through the
UI.

For a **learning lab**, use your account's **default VPC** — it already has
one subnet per Availability Zone with auto-assigned public IPs, so you don't
need to build a custom VPC just to run this lab. (Production would use
private subnets — see the Production sections later in this doc.)

## Step 4.1 — Pick a region and find your default VPC's subnets (1 per AZ)

```bash
export AWS_REGION=ap-south-1   # or whichever region you're using
aws configure set region "$AWS_REGION"

aws ec2 describe-subnets \
  --filters "Name=default-for-az,Values=true" \
  --query 'Subnets[].[SubnetId,AvailabilityZone,CidrBlock]' \
  --output table
```

You should see 3 (or more) rows, one per AZ, e.g. `ap-south-1a`,
`ap-south-1b`, `ap-south-1c`. **Note down 3 different Subnet IDs from 3
different AZs** — you'll place ZK1/ZK2/ZK3 one per subnet in the next steps.
Console equivalent: **VPC → Subnets**, filter to your default VPC.

## Step 4.2 — Create the SSH key pair

```bash
aws ec2 create-key-pair \
  --key-name kafka-lab \
  --query 'KeyMaterial' \
  --output text > kafka-lab.pem

chmod 400 kafka-lab.pem
```

Console equivalent: **EC2 → Key Pairs → Create key pair** (download the
`.pem`, then `chmod 400` it locally the same way).

## Step 4.3 — Look up the current Ubuntu 24.04 LTS AMI ID

AMI IDs are region-specific and change over time as Canonical publishes
updates, so don't hardcode one — resolve it dynamically via AWS's public SSM
parameter for the current Ubuntu 24.04 (Noble) image:

```bash
AMI_ID=$(aws ssm get-parameters \
  --names /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
  --query 'Parameters[0].Value' --output text)

echo "$AMI_ID"
```

Canonical publishes and documents this exact SSM parameter path for
resolving the latest Ubuntu AMI in any region. ([Ubuntu on AWS docs][12])

## Step 4.4 — Create the Security Group

```bash
VPC_ID=$(aws ec2 describe-vpcs \
  --filters "Name=isDefault,Values=true" \
  --query 'Vpcs[0].VpcId' --output text)

SG_ID=$(aws ec2 create-security-group \
  --group-name zookeeper-sg \
  --description "ZooKeeper 3-node quorum lab" \
  --vpc-id "$VPC_ID" \
  --query 'GroupId' --output text)

echo "$SG_ID"
```

## Inbound rules

| Port | Source              | Why                      |
| ---: | ------------------- | ------------------------ |
|   22 | Your IP             | SSH                      |
| 2181 | Kafka/client SG     | Client connections       |
| 2888 | `zookeeper-sg`      | Peer communication       |
| 3888 | `zookeeper-sg`      | Leader election          |
| 8080 | Admin/monitoring SG | AdminServer, if required |

AWS Security Groups can reference another Security Group as the source, allowing private-IP communication between associated instances. ([AWS Documentation][3])

Apply the table above (for the lab, source SG for 2181/8080 can also just be
`$SG_ID` itself, since you don't have a separate Kafka/monitoring SG yet):

```bash
MY_IP=$(curl -s https://checkip.amazonaws.com)

aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
  --protocol tcp --port 22 --cidr "${MY_IP}/32"

aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
  --protocol tcp --port 2181 --source-group "$SG_ID"

aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
  --protocol tcp --port 2888 --source-group "$SG_ID"

aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
  --protocol tcp --port 3888 --source-group "$SG_ID"

aws ec2 authorize-security-group-ingress --group-id "$SG_ID" \
  --protocol tcp --port 8080 --source-group "$SG_ID"
```

Console equivalent: **EC2 → Security Groups → Create security group**, then
add each inbound rule (for 2181/2888/3888/8080, set **Source** to "Custom"
and pick the `zookeeper-sg` group itself — this is the "SG-to-SG
referencing" the table above describes).

### Recommended

```text
Kafka SG
   │
   └──────► ZK SG :2181

ZK SG
   │
   ├──────► ZK SG :2888
   └──────► ZK SG :3888
```

### NEVER

```text
0.0.0.0/0 → 2181
0.0.0.0/0 → 2888
0.0.0.0/0 → 3888
```

ZooKeeper is expected to operate in a trusted environment and Apache explicitly recommends deploying it behind a firewall. ([Apache ZooKeeper][4])

## Step 4.5 — Launch the 3 EC2 instances (one per AZ subnet)

Using the 3 Subnet IDs from Step 4.1 (`SUBNET_A`, `SUBNET_B`, `SUBNET_C`)
and the `$AMI_ID`, `$SG_ID`, `kafka-lab` key pair from above:

```bash
SUBNET_A=subnet-xxxxxxxxAZA   # paste your real subnet IDs here
SUBNET_B=subnet-xxxxxxxxAZB
SUBNET_C=subnet-xxxxxxxxAZC

for i in 1 2 3; do
  case $i in
    1) SUBNET=$SUBNET_A ;;
    2) SUBNET=$SUBNET_B ;;
    3) SUBNET=$SUBNET_C ;;
  esac

  aws ec2 run-instances \
    --image-id "$AMI_ID" \
    --instance-type t3.small \
    --key-name kafka-lab \
    --security-group-ids "$SG_ID" \
    --subnet-id "$SUBNET" \
    --associate-public-ip-address \
    --block-device-mappings 'DeviceName=/dev/sda1,Ebs={VolumeSize=20,VolumeType=gp3}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=zk${i}}]" \
    --count 1
done
```

Console equivalent: **EC2 → Launch instance**, repeated 3 times — Ubuntu
Server 24.04 LTS AMI, `t3.small`, 20 GiB gp3, the `kafka-lab` key pair, the
`zookeeper-sg` security group, one instance per AZ subnet, named `zk1`/
`zk2`/`zk3` respectively.

## Step 4.6 — Get each instance's public and private IPs

```bash
aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=zk1,zk2,zk3" "Name=instance-state-name,Values=running" \
  --query 'Reservations[].Instances[].[Tags[?Key==`Name`].Value|[0],PublicIpAddress,PrivateIpAddress]' \
  --output table
```

**Write these down** — you'll use the public IPs to SSH in (Section 5) and
the private IPs to build `/etc/hosts` on each node (Section 20). Unlike the
illustrative `10.0.1.10`-style addresses in the table under Section 3, your
actual private IPs will be whatever the default VPC subnet assigned — use
your real values, not the example ones, everywhere below.

---

# 5. SSH Into the Servers

`kafka-lab.pem` was already created and `chmod 400`'d in Step 4.2. Use the
**public IPs** you captured in Step 4.6 (replace `<PUBLIC-IP>` below with the
real value for each instance):

```bash
ssh -i kafka-lab.pem ubuntu@<PUBLIC-IP>
```

Do this for each node (open 3 terminals/tabs — one per node — since the
rest of this guide has you running commands on ZK1, ZK2, and ZK3
individually and it's easy to lose track of which shell is which).

> First connection to a fresh instance: SSH will ask you to confirm the
> host's fingerprint (`Are you sure you want to continue connecting
> (yes/no/[fingerprint])?`) — type `yes`. If you instead get
> `Connection timed out`, re-check Step 4.4's security group has port 22
> open from your current IP (your IP may have changed since `$MY_IP` was
> captured, especially on a home/mobile connection).

Architecture:

```text
Your Laptop
     │
     │ SSH :22
     ▼
 AWS EC2
 ┌───┼───┐
 ▼   ▼   ▼
ZK1 ZK2 ZK3
```

Once connected, immediately verify:

```bash
hostname
hostname -I
ip addr
```

You should know **which machine you are operating on** before changing ZooKeeper configuration.

---

# 6. Install Java

On all three nodes:

```bash
sudo apt update

sudo apt install -y \
  openjdk-17-jre-headless \
  wget \
  tar \
  netcat-openbsd
```

Verify:

```bash
java -version
```

Example:

```text
openjdk version "17..."
```

> **Why Java 17, not the JDK 8/11/12 the docs literally list:** ZooKeeper
> 3.9.5's own admin guide text names JDK 8, 11 LTS, and 12 as supported
> (explicitly excluding 9 and 10) — it doesn't mention 17 by name. In
> practice, Java 17 is a safe, widely-used choice anyway: Apache's own
> official ZooKeeper Docker image ships a dedicated `3.9.5-jre-17` tag, and
> Java 17 is the LTS release this whole curriculum already standardizes on
> for Kafka itself (Kafka 4.x requires Java 17+). ([Apache ZooKeeper Admin Guide][13], [Docker Hub][2])

---

# 7. Install ZooKeeper

For this current lab use:

```text
Apache ZooKeeper 3.9.5
```

Apache currently lists 3.9.5 as the current release. ([Apache ZooKeeper][1])

Download:

```bash
cd /tmp

wget https://dlcdn.apache.org/zookeeper/zookeeper-3.9.5/apache-zookeeper-3.9.5-bin.tar.gz
```

Extract:

```bash
tar -xzf apache-zookeeper-3.9.5-bin.tar.gz
```

Move:

```bash
sudo mv apache-zookeeper-3.9.5-bin /opt/zookeeper
```

Verify:

```bash
ls /opt/zookeeper
```

Expected:

```text
bin
conf
lib
logs
...
```

---

# 8. Create ZooKeeper User

Don't run ZooKeeper as root.

```bash
sudo useradd \
  --system \
  --home /opt/zookeeper \
  --shell /usr/sbin/nologin \
  zookeeper
```

Ownership:

```bash
sudo chown -R zookeeper:zookeeper /opt/zookeeper
```

---

# 9. ZooKeeper Data Directories

Create:

```bash
sudo mkdir -p /var/lib/zookeeper
sudo mkdir -p /var/lib/zookeeper/datalog

sudo chown -R zookeeper:zookeeper /var/lib/zookeeper
```

We will eventually use:

```text
/var/lib/zookeeper
        │
        ├── myid
        ├── snapshots
        └── ...
        
/var/lib/zookeeper/datalog
        │
        └── transaction logs
```

### Why separate the transaction log?

ZooKeeper's transaction log is latency-sensitive.

Apache recommends putting the transaction log on a dedicated device where possible because it can significantly improve throughput and stable latency. ([Apache ZooKeeper][5])

---

# 10. Single-Node ZooKeeper First

Before building the quorum, understand a standalone server.

Create:

```bash
sudo nano /opt/zookeeper/conf/zoo.cfg
```

For standalone:

```properties
tickTime=2000

dataDir=/var/lib/zookeeper

clientPort=2181
```

Start:

```bash
sudo -u zookeeper \
  /opt/zookeeper/bin/zkServer.sh start
```

Check:

```bash
/opt/zookeeper/bin/zkServer.sh status
```

Expected:

```text
Mode: standalone
```

### Important

```text
Standalone
   ↓
No quorum
   ↓
No HA
   ↓
Learning only
```

---

# 11. ZooKeeper Configuration — `zoo.cfg`

For the 3-node cluster:

```properties
tickTime=2000

dataDir=/var/lib/zookeeper

dataLogDir=/var/lib/zookeeper/datalog

clientPort=2181

initLimit=5

syncLimit=2

server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
```

This is the core configuration.

---

# 12. Configuration Parameters

## `tickTime`

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

It is ZooKeeper's basic time unit for heartbeats and timeouts. ([Apache ZooKeeper][5])

Think:

```text
tickTime
   ↓
ZooKeeper's clock
   ↓
heartbeats
timeouts
session timing
```

---

# 13. `dataDir`

```properties
dataDir=/var/lib/zookeeper
```

Stores ZooKeeper's persistent data/snapshots.

Think:

```text
ZooKeeper
   │
   └── dataDir
         │
         ├── myid
         ├── snapshots
         └── state
```

---

# 14. `dataLogDir`

```properties
dataLogDir=/var/lib/zookeeper/datalog
```

This separates:

```text
Snapshot/data
      vs
Transaction log
```

Why?

```text
Busy disk
   ↓
slow transaction log
   ↓
higher ZooKeeper latency
```

Dedicated storage:

```text
ZooKeeper data → Disk A
Transaction log → Disk B
```

Apache specifically recommends a dedicated transaction-log device for consistent performance. ([Apache ZooKeeper][5])

---

# 15. `clientPort`

```properties
clientPort=2181
```

Applications connect here.

Example:

```text
Kafka Broker
     │
     │ TCP 2181
     ▼
ZooKeeper
```

---

# 16. `initLimit`

```properties
initLimit=5
```

It is measured in `tickTime`.

With:

```text
tickTime = 2 sec
initLimit = 5
```

approximately:

```text
5 × 2 = 10 seconds
```

It gives a follower time to connect and synchronize with the leader during initialization. ([Apache ZooKeeper][6])

### If dataset is large / recovery is slow

You may need a higher value.

But don't blindly increase it.

Investigate:

```text
Disk latency?
Network?
Large snapshot?
Slow machine?
```

---

# 17. `syncLimit`

```properties
syncLimit=2
```

With:

```text
tickTime = 2 sec
```

roughly:

```text
2 × 2 = 4 seconds
```

It controls how far a follower may fall behind the leader before the connection is considered unhealthy. ([Apache ZooKeeper][6])

Example:

```text
Leader
  │
  ├── ZK2 → healthy
  │
  └── ZK3 → falling behind
                 │
                 ▼
            syncLimit exceeded
                 │
                 ▼
            connection dropped
```

---

# 18. `server.X`

Example:

```properties
server.1=zk1:2888:3888
```

Meaning:

```text
server ID = 1
hostname  = zk1
2888      = peer communication
3888      = leader election
```

Full:

```properties
server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
```

All three servers must know the ensemble membership.

---

# 19. `myid`

This is the **identity of each ZooKeeper server**.

## ZK1

```bash
echo 1 | sudo tee /var/lib/zookeeper/myid
```

## ZK2

```bash
echo 2 | sudo tee /var/lib/zookeeper/myid
```

## ZK3

```bash
echo 3 | sudo tee /var/lib/zookeeper/myid
```

Verify:

```bash
cat /var/lib/zookeeper/myid
```

Relationship:

```text
ZK1
 │
 ├── myid = 1
 └── server.1

ZK2
 │
 ├── myid = 2
 └── server.2

ZK3
 │
 ├── myid = 3
 └── server.3
```

The ID must uniquely identify the server in the ensemble.

---

# 20. AWS DNS / Hostname Configuration

`zoo.cfg`'s `server.1=zk1:2888:3888` line (Section 11) requires every node
to resolve the names `zk1`, `zk2`, and `zk3` to real IP addresses. **In the
default VPC used for this lab, that resolution does not exist
automatically** — EC2's built-in private DNS only gives you names like
`ip-10-0-1-23.ec2.internal`, never a short name like `zk1`. (A private
Route 53 hosted zone *can* provide real `zk1`/`zk2`/`zk3` DNS records, but
that's extra AWS setup not needed for this lab — see the note at the end of
this section.) So for this lab, **`/etc/hosts` is required, not optional.**

## Add the entries — on all 3 nodes

Using the **private IPs** you captured in Step 4.6, run this on **ZK1, ZK2,
and ZK3** (all three get the exact same 3 lines):

```bash
sudo tee -a /etc/hosts > /dev/null <<'EOF'
10.0.1.10 zk1
10.0.2.10 zk2
10.0.3.10 zk3
EOF
```

Replace `10.0.1.10` / `10.0.2.10` / `10.0.3.10` above with **your actual**
private IPs from Step 4.6 before running this — they will not match these
illustrative values from Section 3's table.

## Verify

```bash
getent hosts zk1
getent hosts zk2
getent hosts zk3
```

Expected: each prints back the private IP you just added. Then confirm
actual network reachability (not just name resolution):

```bash
ping -c 2 zk1
ping -c 2 zk2
ping -c 2 zk3
```

If `getent` resolves the name but `ping` hangs/fails, that's a Security
Group problem, not a DNS problem — recheck Step 4.4 (2888/3888 must allow
`zookeeper-sg` as the *source*, referencing itself).

> **Production note:** a private Route 53 hosted zone (e.g. `zk1.internal`,
> `zk2.internal`, `zk3.internal` as A records pointing at each node's
> private IP) is the production-grade replacement for hand-edited
> `/etc/hosts` — it survives instance replacement (where the private IP
> changes) without editing every node's `/etc/hosts` by hand. For a
> disposable learning lab, `/etc/hosts` is simpler and sufficient.

---

# 21. Test Network Before Starting ZooKeeper

This is a **very important operational habit**.

From ZK1:

```bash
nc -zv zk2 2888
nc -zv zk3 2888

nc -zv zk2 3888
nc -zv zk3 3888
```

From ZK2:

```bash
nc -zv zk1 2888
nc -zv zk3 2888

nc -zv zk1 3888
nc -zv zk3 3888
```

From ZK3:

```bash
nc -zv zk1 2888
nc -zv zk2 2888

nc -zv zk1 3888
nc -zv zk2 3888
```

If this fails:

```text
Don't troubleshoot ZooKeeper yet.

First fix:
DNS
SG
NACL
routing
ports
```

---

# 22. ZooKeeper systemd Service

Create:

```bash
sudo nano /etc/systemd/system/zookeeper.service
```

Use:

```ini
[Unit]
Description=Apache ZooKeeper
After=network-online.target
Wants=network-online.target

[Service]
Type=simple

User=zookeeper
Group=zookeeper

ExecStart=/opt/zookeeper/bin/zkServer.sh \
  start-foreground \
  /opt/zookeeper/conf/zoo.cfg

Restart=on-failure
RestartSec=5

LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Reload:

```bash
sudo systemctl daemon-reload
```

Enable:

```bash
sudo systemctl enable zookeeper
```

Start:

```bash
sudo systemctl start zookeeper
```

---

# 23. Service Verification

```bash
systemctl status zookeeper
```

Expected:

```text
Active: active (running)
```

Logs:

```bash
journalctl -u zookeeper -f
```

Last 100 lines:

```bash
journalctl -u zookeeper --no-pager -n 100
```

---

# 24. Start the 3-Node Ensemble

Start:

```text
ZK1
 ↓
ZK2
 ↓
ZK3
```

```bash
sudo systemctl start zookeeper
```

on each node.

Eventually:

```text
ZK1 → Leader
ZK2 → Follower
ZK3 → Follower
```

The exact leader can be any of the three.

---

# 25. Check ZooKeeper Mode

Run:

```bash
/opt/zookeeper/bin/zkServer.sh status
```

Possible:

```text
Mode: leader
```

or:

```text
Mode: follower
```

Across the cluster you want:

```text
1 × Leader
2 × Followers
```

---

# 26. ZooKeeper Quorum

For 3 nodes:

```text
N = 3

quorum = floor(3/2) + 1

quorum = 2
```

Therefore:

```text
3/3 → healthy
2/3 → healthy
1/3 → no quorum
```

### Mental model

```text
              3 Nodes
                 │
          ┌──────┴──────┐
          │             │
        Leader       Followers
          │
          └──── quorum ────┘

              2/3
```

---

# 27. ZooKeeper CLI

Connect:

```bash
/opt/zookeeper/bin/zkCli.sh -server zk1:2181
```

You should see:

```text
CONNECTED
```

Apache's CLI uses this `zkCli.sh -server host:2181` model. ([Apache ZooKeeper][1])

---

# 28. Basic ZNode Operations

## List

```text
ls /
```

Example:

```text
[zookeeper]
```

## Create

```text
create /platform "hello-kafka"
```

## Read

```text
get /platform
```

Output:

```text
hello-kafka
```

## Update

```text
set /platform "updated-value"
```

## Delete

```text
delete /platform
```

---

# 29. Understand the ZooKeeper Data Tree

Think:

```text
                    /
                    │
       ┌────────────┼─────────────┐
       │            │             │
   zookeeper      kafka        platform
                    │
              ┌─────┴─────┐
              │           │
           brokers      ...
```

A node in this tree is a:

> **znode**

ZooKeeper is designed for **small coordination data**, not large application payloads. Its data is maintained in memory and persisted through snapshots/transaction logs.

---

# 30. Persistent vs Ephemeral ZNodes

This is an important concept to understand before moving deeper.

### Persistent

```text
create /platform "hello"
```

The node remains until explicitly deleted.

### Ephemeral

An ephemeral node exists only while the client session exists.

Analogy:

```text
Application starts
     ↓
creates ephemeral znode
     ↓
Application dies
     ↓
session expires
     ↓
znode disappears
```

This is useful for concepts such as:

```text
service registration
leadership/ownership indicators
distributed coordination
```

---

# 31. Verify Replication

Create:

```text
create /platform "hello-kafka"
```

Connect through ZK2:

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

Do the same through ZK3.

This demonstrates that the ZooKeeper state is replicated across the ensemble.

---

# 32. Failure Test #1 — Kill the Leader

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

Therefore:

```text
2/3
```

Quorum still exists.

A new leader is elected among the surviving servers.

---

# 33. Verify New Leader

On ZK2:

```bash
/opt/zookeeper/bin/zkServer.sh status
```

On ZK3:

```bash
/opt/zookeeper/bin/zkServer.sh status
```

You should get:

```text
ZK2 → leader
ZK3 → follower
```

or vice versa.

---

# 34. Verify Data After Leader Failure

Connect to surviving node:

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

So:

```text
Leader failure
      ↓
Leader election
      ↓
Quorum survives
      ↓
New leader
      ↓
Service continues
```

---

# 35. Failure Test #2 — Lose Two Nodes

Now:

```text
ZK1 ❌
ZK2 ✅
ZK3 ❌
```

Only:

```text
1/3
```

exists.

Therefore:

```text
NO QUORUM
```

This is the most important practical demonstration of quorum.

---

# 36. Full Failure Experiment

```text
INITIAL

ZK1 ✅ Leader
ZK2 ✅ Follower
ZK3 ✅ Follower

3/3
 ↓
Healthy


STOP ZK1

ZK1 ❌
ZK2 ✅
ZK3 ✅

2/3
 ↓
Quorum
 ↓
New leader


STOP ZK3

ZK1 ❌
ZK2 ✅
ZK3 ❌

1/3
 ↓
NO QUORUM
```

This is why:

```text
1 node ≠ HA
2 nodes ≠ useful fault tolerance
3 nodes = tolerate 1 failure
5 nodes = tolerate 2 failures
```

---

# 37. Recovery

Start:

```bash
sudo systemctl start zookeeper
```

on ZK1 and ZK3.

Then:

```bash
/opt/zookeeper/bin/zkServer.sh status
```

Eventually:

```text
1 Leader
2 Followers
```

Again.

---

# 38. Four Letter Words

ZooKeeper provides diagnostic commands traditionally called **Four Letter Words (4LW)**.

Examples:

```text
ruok
stat
srvr
mntr
conf
cons
isro
```

Apache documents these commands and also provides the newer AdminServer HTTP interface. ([Apache ZooKeeper][7])

---

# 39. `ruok`

```bash
echo ruok | nc localhost 2181
```

Expected:

```text
imok
```

Meaning:

```text
ZooKeeper process is responding
```

### Important

`ruok` does **not** mean:

> "The entire quorum is healthy."

It tells you that this server is responding to the command.

For cluster health, check more.

---

# 40. `stat`

```bash
echo stat | nc localhost 2181
```

Useful information:

* Mode
* Connections
* Latency
* Packets
* Server information

---

# 41. `srvr`

```bash
echo srvr | nc localhost 2181
```

Server-level information.

Useful when troubleshooting a specific node.

---

# 42. `mntr`

```bash
echo mntr | nc localhost 2181
```

Very useful for monitoring.

You'll see metrics such as:

```text
zk_server_state
zk_num_alive_connections
zk_packets_received
zk_packets_sent
```

This is useful for:

```text
Prometheus
monitoring
alerting
dashboards
```

---

# 43. `conf`

```bash
echo conf | nc localhost 2181
```

Shows serving configuration.

---

# 44. `isro`

```bash
echo isro | nc localhost 2181
```

Checks whether the server is in read-only mode.

---

# 45. Enable 4LW Commands

Depending on the version/configuration, commands may need to be explicitly whitelisted.

Example:

```properties
4lw.commands.whitelist=ruok,stat,srvr,mntr,conf,isro
```

Restart:

```bash
sudo systemctl restart zookeeper
```

Don't blindly whitelist every command in production.

---

# 46. AdminServer — Modern Management Interface

ZooKeeper also has an embedded HTTP AdminServer.

Default:

```text
8080
```

Example:

```text
http://zk1:8080/commands
```

Individual command:

```text
http://zk1:8080/commands/stat
```

The AdminServer returns command responses in JSON and is enabled by default in the documented configuration. ([Apache ZooKeeper][7])

### Architecture

```text
Browser / Monitoring
       │
       │ HTTP :8080
       ▼
ZooKeeper AdminServer
       │
       ▼
ZooKeeper
```

### AWS

Do **not** expose 8080 publicly.

Use:

```text
Admin/Monitoring SG
       │
       └──► ZK SG :8080
```

---

# 47. ZooKeeper Internal File System

The important concept is:

```text
ZooKeeper
    │
    ├── Data tree
    │
    ├── Snapshots
    │
    └── Transaction logs
```

Example:

```text
/var/lib/zookeeper/
│
├── myid
├── snapshot.*
└── ...
```

And if we configured:

```properties
dataLogDir=/var/lib/zookeeper/datalog
```

then:

```text
/var/lib/zookeeper/datalog/
└── log.*
```

### Why transaction log?

Suppose:

```text
create /platform "hello"
```

Conceptually:

```text
Client
  ↓
Leader
  ↓
Transaction
  ↓
Quorum agreement
  ↓
Transaction log
  ↓
Committed state
```

### Why snapshots?

Without snapshots:

```text
Millions of transactions
       ↓
Replay everything
       ↓
Slow recovery
```

With snapshots:

```text
Snapshot
   +
Recent transaction logs
   ↓
Fast recovery
```

---

# 48. `myid` Is Also Part of the Internal State

Example:

```text
cat /var/lib/zookeeper/myid
```

returns:

```text
1
```

This identifies:

```text
This machine = server.1
```

Never accidentally clone a running ZooKeeper node's data directory and expect it to become another server without changing identity appropriately.

---

# 49. Performance — What Matters Most

For a Platform Engineer, remember:

```text
Disk
Network
Memory
CPU
Connections
Data size
```

## 1. Disk latency

Especially:

```text
Transaction log
```

Bad:

```text
Busy disk
   ↓
High fsync latency
   ↓
High ZK latency
```

Apache explicitly recommends a dedicated transaction-log device. ([Apache ZooKeeper][5])

---

# 50. Memory

ZooKeeper maintains its data tree in memory.

Bad:

```text
Insufficient RAM
      ↓
GC pressure
      ↓
Latency
      ↓
Potential session problems
```

And:

> **Do not allow ZooKeeper to swap.**

Apache explicitly warns that swapping severely degrades ZooKeeper performance. ([Apache ZooKeeper][5])

---

# 51. Network

ZooKeeper needs reliable low-latency communication:

```text
ZK1 ↔ ZK2
ZK1 ↔ ZK3
ZK2 ↔ ZK3
```

Problems:

```text
packet loss
high latency
security group issues
DNS failures
routing issues
```

can result in:

```text
follower falling behind
leader election
session problems
quorum loss
```

---

# 52. Too Many Connections

ZooKeeper has client-connection controls such as:

```properties
maxClientCnxns=60
```

The exact value should be chosen according to your environment rather than blindly using a default.

The official Docker image exposes this setting as `ZOO_MAX_CLIENT_CNXNS`, with a default of 60 in its image configuration. ([Docker Hub][8])

---

# 53. Don't Store Large Data in ZooKeeper

Bad:

```text
/config
   └── 500 MB JSON
```

ZooKeeper is designed for:

```text
small
coordination
metadata
configuration
naming
```

not:

```text
large application payloads
files
messages
databases
```

ZooKeeper's documented maximum znode size is around 1 MB by default, but it is designed for data on the order of kilobytes. ([Apache ZooKeeper][6])

---

# 54. Security — Important Missing Piece

A fresh ZooKeeper installation should **not** be treated as production-hardened.

Apache's security documentation says that:

* TLS is available for client/server and quorum traffic.
* SASL/Kerberos, digest, x509/mTLS are available for authentication.
* ACLs provide per-znode access control.
* These controls are not automatically enabled. ([Apache ZooKeeper][4])

So think:

```text
Network Security
       +
TLS
       +
Authentication
       +
ACL
```

---

# 55. ACLs

Example concept:

```text
/world
   │
   ├── anyone
   └── open access
```

This is dangerous for sensitive production data.

ACL schemes include:

```text
world
auth
digest
sasl
ip
x509
```

Apache documents these schemes and warns that ACLs are not automatically recursive. ([Apache ZooKeeper][4])

---

# 56. TLS

Production architecture can be:

```text
Kafka
  │
  │ TLS
  ▼
ZooKeeper
```

and quorum traffic can also be protected:

```text
ZK1 ═══TLS═══ ZK2
 │             │
 ╚════TLS═════ ZK3
```

ZooKeeper supports TLS for client-server and quorum communication, with hostname verification available. ([Apache ZooKeeper][4])

---

# 57. Dynamic Reconfiguration

Modern ZooKeeper supports dynamic ensemble configuration.

Historically:

```text
Edit zoo.cfg
   ↓
Restart
```

Modern dynamic reconfiguration allows ensemble membership changes at runtime.

Example:

```text
3-node ensemble

ZK1
ZK2
ZK3
```

could be changed through the reconfiguration mechanism.

However:

> **Don't casually enable/use this in production without understanding its security controls.**

Apache introduced `reconfigEnabled` because unauthorized reconfiguration could allow a malicious client to alter ensemble membership. ([Apache ZooKeeper][9])

---

# 58. AWS Storage Design

For learning:

```text
EC2
 └── gp3
      ├── ZooKeeper data
      └── transaction logs
```

For production:

```text
EC2
 │
 ├── OS volume
 │
 ├── ZooKeeper data volume
 │
 └── Dedicated transaction-log volume
```

Why?

```text
Transaction log
      ↓
latency sensitive
      ↓
dedicated storage
      ↓
more stable latency
```

Again, this is explicitly recommended in ZooKeeper's administration documentation. ([Apache ZooKeeper][5])

---

# 59. Docker ZooKeeper Setup

Docker is excellent for your **local learning environment**.

Architecture:

```text
Docker Host
    │
    ├── zk1
    ├── zk2
    └── zk3
```

The official ZooKeeper image currently provides `3.9.5` and `3.9.5-jre-17` tags. ([Docker Hub][10])

---

# 60. Docker Compose

```yaml
services:

  zk1:
    image: zookeeper:3.9.5
    hostname: zk1
    restart: unless-stopped
    environment:
      ZOO_MY_ID: 1
      ZOO_SERVERS: >
        server.1=zk1:2888:3888;2181
        server.2=zk2:2888:3888;2181
        server.3=zk3:2888:3888;2181

  zk2:
    image: zookeeper:3.9.5
    hostname: zk2
    restart: unless-stopped
    environment:
      ZOO_MY_ID: 2
      ZOO_SERVERS: >
        server.1=zk1:2888:3888;2181
        server.2=zk2:2888:3888;2181
        server.3=zk3:2888:3888;2181

  zk3:
    image: zookeeper:3.9.5
    hostname: zk3
    restart: unless-stopped
    environment:
      ZOO_MY_ID: 3
      ZOO_SERVERS: >
        server.1=zk1:2888:3888;2181
        server.2=zk2:2888:3888;2181
        server.3=zk3:2888:3888;2181
```

The official image documents `ZOO_MY_ID`, `ZOO_SERVERS`, and the replicated-mode configuration. ([Docker Hub][10])

Start:

```bash
docker compose up -d
```

Check:

```bash
docker compose ps
```

Logs:

```bash
docker compose logs -f zk1
```

---

# 61. Important Docker HA Warning

This:

```text
One Docker host

 ├── zk1
 ├── zk2
 └── zk3
```

is **not real infrastructure HA**.

If the host dies:

```text
Host ❌
 │
 ├── zk1 ❌
 ├── zk2 ❌
 └── zk3 ❌
```

The official ZooKeeper Docker image explicitly warns that multiple ZooKeeper containers on one machine do not provide redundancy against host failure. ([Docker Hub][10])

For real HA:

```text
Host/AZ-A → ZK1
Host/AZ-B → ZK2
Host/AZ-C → ZK3
```

---

# 62. Docker Persistent Storage

Don't run production ZooKeeper containers with only ephemeral container storage.

Use:

```text
/data
/datalog
```

The official image uses `/data` for snapshots/in-memory database data and `/datalog` for transaction logs. ([Docker Hub][10])

Conceptually:

```text
Docker
 │
 ├── /data
 │     └── snapshots
 │
 └── /datalog
       └── transaction logs
```

---

# 63. ZooNavigator

ZooNavigator provides a graphical interface for browsing ZooKeeper.

Architecture:

```text
Browser
   │
   │ :9000
   ▼
ZooNavigator
   │
   │ ZooKeeper connection
   ▼
ZK1 :2181
ZK2 :2181
ZK3 :2181
```

---

# 64. Install ZooNavigator

```bash
docker run -d \
  --name zoonavigator \
  --restart unless-stopped \
  -p 9000:9000 \
  -e HTTP_PORT=9000 \
  elkozmon/zoonavigator:latest
```

Open:

```text
http://localhost:9000
```

The ZooNavigator documentation uses port 9000 and accepts an ensemble connection string such as `zk1:2181,zk2:2181,zk3:2181`. ([ZooNavigator][11])

---

# 65. Connect ZooNavigator

Connection string:

```text
zk1:2181,zk2:2181,zk3:2181
```

Then browse:

```text
/
├── zookeeper
├── platform
└── kafka
```

You can inspect:

```text
Data
ACL
Metadata
```

ZooNavigator's current documentation shows these views and node-management operations. ([ZooNavigator][11])

---

# 66. ZooNavigator vs CLI

| Tool         | Best for                   |
| ------------ | -------------------------- |
| `zkCli.sh`   | Production troubleshooting |
| 4LW          | Health/diagnostics         |
| AdminServer  | HTTP/admin integration     |
| ZooNavigator | Visual inspection/learning |

For your Platform Engineer role:

```text
CLI
  ↓
Must know

4LW
  ↓
Must know

AdminServer
  ↓
Should know

ZooNavigator
  ↓
Nice operational tool
```

---

# 67. Troubleshooting Matrix

| Problem                      | First check                        |
| ---------------------------- | ---------------------------------- |
| ZooKeeper won't start        | `journalctl -u zookeeper`          |
| Wrong identity               | `cat /var/lib/zookeeper/myid`      |
| No leader                    | 2888 / 3888                        |
| Kafka can't connect          | 2181 + SG                          |
| DNS failure                  | `getent hosts zk1`                 |
| Quorum unavailable           | Number of healthy nodes            |
| Frequent leader elections    | Network / disk / timing            |
| Follower keeps disconnecting | `syncLimit`, latency               |
| Slow writes                  | Transaction-log disk               |
| Memory problems              | JVM heap / GC / swapping           |
| ZNode too large              | Application design                 |
| 4LW doesn't work             | `4lw.commands.whitelist`           |
| AdminServer unavailable      | 8080 / admin configuration         |
| Nodes cannot communicate     | SG / NACL / routing                |
| Data corruption concerns     | Disk + transaction logs + recovery |

---

# 68. Platform Engineer Troubleshooting Flow

Suppose:

> Kafka cannot connect to ZooKeeper.

Don't randomly restart everything.

Use:

```text
Kafka cannot connect
       │
       ▼
DNS resolution?
       │
       ├── NO → Fix DNS
       │
       ▼
TCP 2181?
       │
       ├── NO → SG/NACL/routing
       │
       ▼
ZooKeeper process?
       │
       ├── NO → systemd/logs
       │
       ▼
ZooKeeper responding?
       │
       ├── NO → ruok/stat
       │
       ▼
Quorum healthy?
       │
       ├── NO → check 2888/3888
       │
       ▼
Leader exists?
       │
       ▼
Kafka connection
```

This is much more valuable in an interview than simply knowing commands.

---

# 69. Production AWS Architecture

```text
                         AWS REGION

       ┌─────────────────┼─────────────────┐
       │                 │                 │
      AZ-A              AZ-B              AZ-C
       │                 │                 │
   ┌─────────┐       ┌─────────┐       ┌─────────┐
   │   ZK1   │       │   ZK2   │       │   ZK3   │
   │ Leader  │       │Follower │       │Follower │
   └────┬────┘       └────┬────┘       └────┬────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          │
                     Private Network
                          │
             ┌────────────┴────────────┐
             │                         │
        Kafka Brokers              Monitoring
```

### Production principles

```text
✓ Separate AZs
✓ Private subnets
✓ SG restricted
✓ No public 2181
✓ No public 2888
✓ No public 3888
✓ Persistent storage
✓ Dedicated transaction log where appropriate
✓ Monitoring
✓ Alerting
✓ TLS/authentication
✓ ACLs
✓ Backups/recovery strategy
✓ Capacity planning
```

---

# 70. What ZooKeeper Does NOT Store

This is important because you're learning Kafka.

ZooKeeper does **not** store:

```text
Kafka messages
Kafka partition data
Kafka segment files
Consumer message payloads
```

Those live on Kafka brokers.

Historically, ZooKeeper provided Kafka coordination/metadata functions.

Conceptually:

```text
Kafka
 │
 ├── Topics
 ├── Partitions
 ├── Messages
 ├── Segments
 └── Replicas
```

while:

```text
ZooKeeper
 │
 ├── Coordination
 ├── Broker registration
 ├── Controller coordination
 └── Kafka metadata/state
```

---

# 71. ZooKeeper → KRaft Context

This is particularly important for your learning path.

### Old Kafka

```text
Kafka Brokers
      │
      ▼
ZooKeeper Ensemble
      │
      └── Kafka metadata/coordination
```

### Modern Kafka

```text
Kafka Cluster
      │
      ▼
KRaft Controller Quorum
      │
      └── Kafka-native metadata management
```

So don't learn ZooKeeper thinking:

> "This is how I should deploy a new Kafka cluster."

Learn it as:

> **"This is the architecture I need to understand when supporting legacy Kafka environments and understanding why KRaft exists."**

---

# 72. Final Hands-On Checklist

After completing this lab, you should be able to perform all of these **without looking at notes**:

```text
AWS
□ Find default VPC subnets (1 per AZ)
□ Create a key pair
□ Look up the current Ubuntu AMI via SSM
□ Create the Security Group + rules
□ Create 3 EC2 nodes, one per AZ subnet
□ Retrieve public + private IPs
□ SSH into nodes
□ Verify private networking
□ Edit /etc/hosts on all 3 nodes (zk1/zk2/zk3)
□ Terminate instances + delete SG/key pair when done (cost control)

Installation
□ Install Java
□ Download ZooKeeper
□ Create zookeeper user
□ Create data directories

Configuration
□ Configure zoo.cfg
□ Understand tickTime
□ Understand initLimit
□ Understand syncLimit
□ Understand dataDir
□ Understand dataLogDir
□ Understand clientPort
□ Understand server.X
□ Configure myid

Operations
□ Start ZooKeeper
□ Stop ZooKeeper
□ Restart ZooKeeper
□ Check systemd
□ Check logs
□ Identify leader
□ Identify followers

CLI
□ Connect with zkCli.sh
□ ls
□ create
□ get
□ set
□ delete

Diagnostics
□ ruok
□ stat
□ srvr
□ mntr
□ conf
□ isro
□ AdminServer

HA
□ Kill leader
□ Observe leader election
□ Verify quorum
□ Kill second node
□ Observe quorum loss
□ Recover nodes

Storage
□ Understand snapshots
□ Understand transaction logs
□ Understand dataLogDir
□ Understand disk latency

AWS Production
□ Multi-AZ
□ Private subnet
□ SG-to-SG communication
□ Persistent storage
□ Monitoring
□ Security

Docker
□ Run standalone
□ Run 3-node ensemble
□ Configure ZOO_MY_ID
□ Configure ZOO_SERVERS
□ Persist /data
□ Persist /datalog

Management
□ ZooNavigator
□ Connect to ensemble
□ Browse znodes
□ Inspect ACL/data/metadata
```

---

# 73. Interview Architecture — Memorize This

If an interviewer asks:

> **"How would you deploy a 3-node ZooKeeper cluster on AWS?"**

Your answer should be:

```text
                         AWS Region
                             │
             ┌───────────────┼───────────────┐
             │               │               │
            AZ-A            AZ-B            AZ-C
             │               │               │
           ZK1             ZK2             ZK3
          ID=1            ID=2            ID=3
             │               │               │
             └───────┬───────┴───────┬───────┘
                     │               │
                 2888 peer       3888 election
                     │               │
                     └───────┬───────┘
                             │
                         2/3 quorum
                             │
                         :2181
                             │
                             ▼
                       Kafka Brokers
```

Then explain:

```text
3 nodes
→ quorum 2
→ tolerate 1 failure

2181
→ client traffic

2888
→ server-to-server

3888
→ leader election

dataDir
→ persistent state/snapshots

dataLogDir
→ transaction logs

myid
→ server identity

tickTime
→ basic timing unit

initLimit
→ follower initialization window

syncLimit
→ follower synchronization window
```

Then demonstrate:

```text
Kill leader
    ↓
New leader elected
    ↓
2/3 quorum survives

Kill second node
    ↓
1/3
    ↓
No quorum
```

That is the **core operational knowledge** I would expect you to retain from these sections.

### Current-reference note

For a fresh lab today, use **ZooKeeper 3.9.5** rather than the older 3.9.4 used in your previous document; Apache currently lists 3.9.5 as the current release, and its security page recommends upgrading earlier 3.9 releases to 3.9.5 for recent fixes. ([Apache ZooKeeper][1])

---

# 74. Cleanup — Avoid Ongoing AWS Costs

This is the step most tutorials skip, and the one that actually costs you
money if skipped. `t3.small` instances plus `gp3` EBS volumes are cheap but
not free — if you're done with the lab (or pausing for more than a day),
tear it down.

## Terminate the instances

```bash
IDS=$(aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=zk1,zk2,zk3" "Name=instance-state-name,Values=running,stopped" \
  --query 'Reservations[].Instances[].InstanceId' --output text)

aws ec2 terminate-instances --instance-ids $IDS
```

Console equivalent: **EC2 → Instances**, select `zk1`/`zk2`/`zk3` →
**Instance state → Terminate instance**.

## Confirm they're gone (don't skip this — a stuck instance still bills)

```bash
aws ec2 describe-instances \
  --filters "Name=tag:Name,Values=zk1,zk2,zk3" \
  --query 'Reservations[].Instances[].[InstanceId,State.Name]' --output table
```

Wait until every row shows `terminated`.

## Delete the Security Group (only after all 3 instances are terminated)

```bash
aws ec2 delete-security-group --group-id "$SG_ID"
```

If this errors with `DependencyViolation`, an instance is still using it —
double-check the previous step actually completed.

## Delete the key pair (optional — cheap to keep, but tidy if you're done)

```bash
aws ec2 delete-key-pair --key-name kafka-lab
rm -f kafka-lab.pem
```

## What you do NOT need to clean up

Nothing else was created outside your default VPC (no custom VPC, no
Elastic IPs, no Route 53 zone, no extra EBS volumes beyond each instance's
root volume, which terminates with the instance by default) — so once the 3
instances show `terminated` and the security group is deleted, this lab
leaves no ongoing cost behind.

---

[1]: https://zookeeper.apache.org/releases/?utm_source=chatgpt.com "Releases - Apache ZooKeeper"
[2]: https://hub.docker.com/_/zookeeper?tab=description%5D%C3%83%C6%92%C3%A2%E2%82%AC%C5%A1%C3%83%E2%80%9A%C3%82%C2%A0&utm_source=chatgpt.com "zookeeper - Official Image | Docker Hub"
[3]: https://docs.aws.amazon.com/pdfs/vpc/latest/userguide/vpc-ug.pdf?utm_source=chatgpt.com "Amazon Virtual Private Cloud - User Guide"
[4]: https://zookeeper.apache.org/security/?utm_source=chatgpt.com "Security - Apache ZooKeeper"
[5]: https://zookeeper.apache.org/doc/r3.9.3/zookeeperAdmin.html?utm_source=chatgpt.com "ZooKeeper: Because Coordinating Distributed Systems is a Zoo"
[6]: https://zookeeper.apache.org/doc/r3.1.2/zookeeperAdmin.html?utm_source=chatgpt.com "ZooKeeper Administrator's Guide"
[7]: https://zookeeper.apache.org/doc/r3.8.1/zookeeperAdmin.html?utm_source=chatgpt.com "ZooKeeper: Because Coordinating Distributed Systems is a Zoo"
[8]: https://hub.docker.com/layers/library/zookeeper/3.9.5/images/sha256-d3b0846aaf56635c22520d211615609937eddd2ad3782507d7d201b76d311fab?utm_source=chatgpt.com "Image Layer Details - sha256-d3b0846aaf56635c22520d211615609937eddd2ad3782507d7d201b76d311fab:3.9.5"
[9]: https://zookeeper.apache.org/doc/r3.9.4/zookeeperReconfig.html?utm_source=chatgpt.com "ZooKeeper: Because Coordinating Distributed Systems is a Zoo"
[10]: https://hub.docker.com/_/zookeeper?utm_source=chatgpt.com "zookeeper - Official Image | Docker Hub"
[11]: https://zoonavigator.elkozmon.com/_/downloads/en/latest/pdf/?utm_source=chatgpt.com "ZooNavigator Documentation"
[12]: https://documentation.ubuntu.com/aws/aws-how-to/instances/find-ubuntu-images/ "Find Ubuntu images on AWS - Ubuntu on AWS documentation"
[13]: https://zookeeper.apache.org/doc/r3.9.5/zookeeperAdmin.html "ZooKeeper Administrator's Guide (3.9.5) — Required Software"
