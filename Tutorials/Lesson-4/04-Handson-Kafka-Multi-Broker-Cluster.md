# Hands-On: Kafka Multi Broker (Cluster) Setup + Testing the Cluster

> **Scope:** Course items 30 ("Hands-On: Kafka Multi Broker (Cluster) Setup")
> and 31 ("Hands-On: Testing the Kafka Cluster").
>
> **Starting point:** `Lesson-4/03` already brought up a single broker,
> `kf1` (`broker.id=1`, Kafka 3.9.2 at `/opt/kafka`, systemd `kafka.service`),
> talking to the existing 3-node ZooKeeper ensemble from Lesson-3
> (`zk1`/`zk2`/`zk3`, ZooKeeper 3.9.5, client port 2181, chroot `/kafka`).
>
> **Goal of this document:** turn that single broker into a real 3-broker
> cluster (`kf1`, `kf2`, `kf3`), verify it actually formed a cluster, then
> prove it works — create a multi-partition/replicated topic, produce and
> consume with keys, and run a short broker-failure test.

---

# 1. What We Are Building

```text
                              AWS REGION
                                  │
                ┌─────────────────┼─────────────────┐
                │                 │                 │
               AZ-A              AZ-B              AZ-C
                │                 │                 │
            ┌───────┐         ┌───────┐         ┌───────┐
            │  ZK1  │         │  ZK2  │         │  ZK3  │
            │ ID=1  │         │ ID=2  │         │ ID=3  │
            └───┬───┘         └───┬───┘         └───┬───┘
                └────────────┬────┴────────────┬────┘
                              2181 (quorum = 2/3)
                              chroot: /kafka
                                     │
                ┌─────────────────┼─────────────────┐
                │                 │                 │
            ┌───────┐         ┌───────┐         ┌───────┐
            │  KF1  │         │  KF2  │         │  KF3  │
            │broker │         │broker │         │broker │
            │ id=1  │         │ id=2  │         │ id=3  │
            │(exists│         │ (NEW) │         │ (NEW) │
            │already│         │       │         │       │
            └───────┘         └───────┘         └───────┘
                │                 │                 │
                └────────────┬────┴────────────┬────┘
                              9092 (PLAINTEXT)
                                     │
                              Producers / Consumers
```

## What's already done (from `Lesson-4/03`) vs. what this file does

```text
Already done (03)                    This file (04)
──────────────────                   ───────────────
kafka-sg security group created      Launch kf2, kf3 (AWS Console)
zookeeper-sg rule for kafka-sg :2181 Update /etc/hosts on ALL 6 nodes
kf1 launched, Java installed         Install Kafka on kf2, kf3
Kafka installed on kf1               Configure broker.id=2 / broker.id=3
server.properties (broker.id=1)      systemd service on kf2, kf3
zookeeper.connect w/ /kafka chroot   Start + verify 3-broker cluster
kafka.service running on kf1         Create/describe a real topic
                                      Produce/consume with keys
                                      Kill-a-broker replication preview
```

## Why 3 brokers specifically

- Same reasoning as `Lesson-4/01`: replication factor 3 needs **at least 3
  brokers** to place 3 distinct replicas.
- 3 brokers across 3 AZs means the cluster tolerates **one broker (or one
  AZ) failing** and still has quorum-independent, per-partition
  availability (a partition still has a leader as long as at least one of
  its replicas survives).
- This mirrors exactly the ZooKeeper ensemble's own 3-node/AZ-per-node
  layout from `Lesson-3/05` — same failure-domain logic, applied one layer
  up the stack.

---

# 2. Instance Inventory (Fill In Your Real Values)

| Node | Hostname | Role                | AZ (example) | Private IP (example) | Status                        |
| ---- | -------- | ------------------- | ------------- | --------------------- | ------------------------------ |
| ZK1  | `zk1`    | ZooKeeper ID=1       | AZ-A          | `10.0.1.10`            | already running (Lesson-3)     |
| ZK2  | `zk2`    | ZooKeeper ID=2       | AZ-B          | `10.0.2.10`            | already running (Lesson-3)     |
| ZK3  | `zk3`    | ZooKeeper ID=3       | AZ-C          | `10.0.3.10`            | already running (Lesson-3)     |
| KF1  | `kf1`    | Kafka broker.id=1    | AZ-A          | `10.0.1.11`            | already running (Lesson-4/03)  |
| KF2  | `kf2`    | Kafka broker.id=2    | AZ-B          | `10.0.2.11`            | **launched in this file**      |
| KF3  | `kf3`    | Kafka broker.id=3    | AZ-C          | `10.0.3.11`            | **launched in this file**      |

- The IPs above are **illustrative**, following the same `x.y.10`
  (ZooKeeper) / `x.y.11` (Kafka) convention used across this series — your
  default VPC will hand out different real addresses. Use your actual
  values everywhere below.
- `kf1` already occupies one AZ subnet (from file 03) — this file places
  `kf2` and `kf3` in the two AZ subnets `kf1` is **not** in, so all three
  brokers land in three different AZs, same as the ZK nodes.

---

# 3. AWS Console — Launch `kf2` and `kf3`

`kafka-sg` and the `kafka-lab` key pair already exist (created in file 03),
so this is shorter than the very first EC2 launch in the series — no new
security group or key pair needed.

Repeat this whole step **twice** — once for `kf2`, once for `kf3`:

1. **EC2 console → Instances → Launch instance.**
2. **Name and tags → Name:** `kf2` (then `kf3` on the second run).
3. **Application and OS Images (Amazon Machine Image):** search `Ubuntu` →
   choose **Ubuntu Server 24.04 LTS (HVM), SSD Volume Type**, 64-bit (x86),
   Quick Start — same AMI search pattern as every other node in this
   series (AMI IDs are region-specific/rotate, which is why you search
   instead of hardcoding an AMI ID).
4. **Instance type:** `t3.small`.
5. **Key pair (login):** `kafka-lab`.
6. **Network settings → Edit:**
   - **VPC:** your default VPC (same one as every other node).
   - **Subnet:** pick the AZ subnet **not already used by `kf1`** — a
     different one for `kf2` than for `kf3` (see Section 2's inventory —
     e.g. if `kf1` is in AZ-A, put `kf2` in AZ-B and `kf3` in AZ-C).
   - **Auto-assign public IP:** **Enable**.
   - **Firewall (security groups):** **Select existing security group** →
     `kafka-sg`.
7. **Configure storage:** **20 GiB**, **gp3**.
8. Leave **Advanced details** at defaults.
9. Confirm **Number of instances** = `1` → **Launch instance**.
10. Repeat for the second node, picking the other remaining AZ subnet.

## Verify the security group is actually sufficient before moving on

`kafka-sg` (from file 03) should already have:

| Type       | Port | Source                        |
| ---------- | ---- | ------------------------------ |
| SSH        | 22   | My IP                          |
| Custom TCP | 9092 | **Custom** → `kafka-sg` itself |
| Custom TCP | 8080 | My IP                          |

- The 9092 self-reference rule is what lets `kf1`/`kf2`/`kf3` replicate to
  each other and lets any of them serve any producer/consumer on 9092 —
  same SG-to-SG referencing trick used for `zookeeper-sg` in Lesson-3.
- Double-check `zookeeper-sg` still has its inbound rule for port 2181 with
  **Source = `kafka-sg`** (created in file 03) — without it, `kf2`/`kf3`
  can resolve `zk1`/`zk2`/`zk3` by name but the TCP connection on 2181 will
  hang/refuse.
- **EC2 console → Security Groups →** select each SG to double check the
  rule list if anything below doesn't behave as expected.

---

# 4. Get `kf2` and `kf3`'s Public and Private IPs

1. **EC2 console → Instances.**
2. Wait until `kf2` and `kf3` show **Running**.
3. Select each instance's checkbox → read **Public IPv4 address** and
   **Private IPv4 addresses** from the details pane (add those columns via
   the column-settings gear icon if not already visible).
4. **Write both down for both nodes** — public IPs for SSH (Section 5),
   private IPs for `/etc/hosts` (Section 6).

```bash
ssh -i kafka-lab.pem ubuntu@<KF2-PUBLIC-IP>
ssh -i kafka-lab.pem ubuntu@<KF3-PUBLIC-IP>
```

Open one terminal per node — this file has you running commands on `kf2`
and `kf3` individually, plus revisiting `zk1`/`zk2`/`zk3`/`kf1` briefly in
Section 5, so six terminals total is the least error-prone setup.

---

# 5. Update `/etc/hosts` on ALL SIX Nodes

This is the step worth **not** assuming is already correct. File 03 should
have added `kf1`'s entry to the ZK nodes, but rather than trust that,
verify and complete the **full 6-node picture on every one of the 6
nodes** — every node needs to resolve the other five by short hostname.

## The block every node needs (using the example IPs from Section 2)

```text
10.0.1.10 zk1
10.0.2.10 zk2
10.0.3.10 zk3
10.0.1.11 kf1
10.0.2.11 kf2
10.0.3.11 kf3
```

Replace every IP with your **real** private IPs before running anything.

## Check what's already there, then append what's missing

On **every one of the 6 nodes** (`zk1`, `zk2`, `zk3`, `kf1`, `kf2`, `kf3`),
first check current state:

```bash
cat /etc/hosts
getent hosts zk1 zk2 zk3 kf1 kf2 kf3
```

- If `getent hosts` already resolves all six names correctly on a given
  node, that node needs no changes — move to the next.
- If any name is missing (typically `kf2`/`kf3` will be missing everywhere,
  since they didn't exist until Section 3), append the **full 6-line
  block** — duplicate lines for names already present are harmless for
  resolution, but for a clean file, append only the missing lines:

```bash
sudo tee -a /etc/hosts > /dev/null <<'EOF'
10.0.1.10 zk1
10.0.2.10 zk2
10.0.3.10 zk3
10.0.1.11 kf1
10.0.2.11 kf2
10.0.3.11 kf3
EOF
```

(Edit the file with `sudo nano /etc/hosts` instead if you want to avoid
duplicate lines from partial prior edits.)

## Verify on every node

```bash
getent hosts zk1
getent hosts zk2
getent hosts zk3
getent hosts kf1
getent hosts kf2
getent hosts kf3
```

Then confirm actual reachability, not just name resolution — from `kf2`
and `kf3` specifically (the two new nodes):

```bash
ping -c 2 zk1 && ping -c 2 zk2 && ping -c 2 zk3
ping -c 2 kf1
```

> **If `getent` resolves a name but `ping`/`nc` can't reach it**, that's a
> security group problem, not a DNS problem — recheck Section 3's SG
> table (9092 self-reference on `kafka-sg`, 2181 on `zookeeper-sg` sourced
> from `kafka-sg`).

---

# 6. Install Java on `kf2` and `kf3`

Same package set used on every other node in this series:

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

Expected:

```text
openjdk version "17..."
```

---

# 7. Install Kafka 3.9.2 on `kf2` and `kf3`

Same version and layout as `kf1` (from file 03) — `kf1` runs Kafka 3.9.2 at
`/opt/kafka`, so `kf2`/`kf3` must match exactly for a healthy cluster.

On **both `kf2` and `kf3`**:

```bash
cd /tmp

wget https://archive.apache.org/dist/kafka/3.9.2/kafka_2.13-3.9.2.tgz
```

> Note: `3.9.2` is no longer on Apache's "current releases" mirror
> (`dlcdn.apache.org`) since Kafka 4.x is now the current line — 3.9.2 lives
> permanently on the archive mirror instead (same as `kf1`'s install in
> file 03).

Extract:

```bash
tar -xzf kafka_2.13-3.9.2.tgz
```

Move:

```bash
sudo mv kafka_2.13-3.9.2 /opt/kafka
```

Verify:

```bash
ls /opt/kafka
```

Expected:

```text
bin
config
libs
licenses
site-docs
...
```

---

# 8. Create the `kafka` User and Data Directory on `kf2` and `kf3`

On **both `kf2` and `kf3`** (same pattern as `kf1`):

```bash
sudo useradd \
  --system \
  --home /opt/kafka \
  --shell /usr/sbin/nologin \
  kafka

sudo mkdir -p /var/lib/kafka/data

sudo chown -R kafka:kafka /opt/kafka
sudo chown -R kafka:kafka /var/lib/kafka/data
```

```text
/opt/kafka                 → binaries + config (owned by kafka)
/var/lib/kafka/data        → this broker's log segments (owned by kafka)
```

---

# 9. Configure `server.properties`

## On `kf2`

```bash
sudo nano /opt/kafka/config/server.properties
```

Key values (edit or add these lines — leave the rest of the shipped file
as-is unless you have a specific reason to change it):

```properties
broker.id=2

listeners=PLAINTEXT://kf2:9092
advertised.listeners=PLAINTEXT://kf2:9092

log.dirs=/var/lib/kafka/data

zookeeper.connect=zk1:2181,zk2:2181,zk3:2181/kafka
```

## On `kf3`

```bash
sudo nano /opt/kafka/config/server.properties
```

```properties
broker.id=3

listeners=PLAINTEXT://kf3:9092
advertised.listeners=PLAINTEXT://kf3:9092

log.dirs=/var/lib/kafka/data

zookeeper.connect=zk1:2181,zk2:2181,zk3:2181/kafka
```

## Why each of these matters

```text
broker.id
   ↓
must be unique per broker in the cluster
   ↓
kf1=1  kf2=2  kf3=3

listeners / advertised.listeners
   ↓
must point at THIS broker's own resolvable hostname
   ↓
a client that resolves "kf2" must actually reach kf2, not kf1

zookeeper.connect
   ↓
identical on all 3 brokers
   ↓
same 3 ZK nodes + same /kafka chroot znode
   ↓
this is what makes kf1/kf2/kf3 register as ONE cluster
instead of three unrelated single-broker clusters
```

- The `/kafka` chroot znode was already created via `zkCli.sh` in file 03 —
  do **not** recreate it; `kf2` and `kf3` just need to point at the same
  path so all three brokers register under the same ZooKeeper subtree
  (`/kafka/brokers/ids/1`, `/kafka/brokers/ids/2`, `/kafka/brokers/ids/3`).
- Getting `advertised.listeners` wrong (e.g. leaving it as `localhost` or
  pointing at the wrong hostname) is one of the most common real-world
  Kafka cluster bugs — a client can connect to the bootstrap broker fine,
  get redirected to a partition's actual leader, and then fail to connect
  to *that* address. Always match it to the node's own short hostname in
  this lab.

---

# 10. systemd Service on `kf2` and `kf3`

Same unit pattern used for `kf1`. On **both nodes**:

```bash
sudo nano /etc/systemd/system/kafka.service
```

```ini
[Unit]
Description=Apache Kafka Broker
After=network-online.target
Wants=network-online.target

[Service]
Type=simple

User=kafka
Group=kafka

ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh

Restart=on-failure
RestartSec=5

LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Reload, enable, start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka
sudo systemctl start kafka
```

---

# 11. Start Order and Service Verification

```text
ZK1, ZK2, ZK3      → must already be running (Lesson-3)
     ↓
KF1                → already running (Lesson-4/03)
     ↓
KF2, KF3           → start now
```

On `kf2` and `kf3`:

```bash
systemctl status kafka
```

Expected:

```text
Active: active (running)
```

Watch startup logs if anything looks wrong:

```bash
journalctl -u kafka -f
```

Last 100 lines if you need history instead of a live tail:

```bash
journalctl -u kafka --no-pager -n 100
```

A clean startup log ends with something like:

```text
[KafkaServer id=2] started (kafka.server.KafkaServer)
```

(`id=2` on `kf2`, `id=3` on `kf3` — confirming the broker started with the
identity you configured.)

---

# 12. Verify the Cluster Formed — via ZooKeeper

Connect to any ZK node:

```bash
/opt/zookeeper/bin/zkCli.sh -server zk1:2181
```

List registered broker IDs under the chroot:

```text
ls /kafka/brokers/ids
```

Expected once all three brokers are up:

```text
[1, 2, 3]
```

Inspect one broker's registration details:

```text
get /kafka/brokers/ids/2
```

Expected (abbreviated — real output includes a JSON blob):

```json
{"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},"endpoints":["PLAINTEXT://kf2:9092"],"jmx_port":-1,"host":"kf2","timestamp":"...","port":9092,"version":5}
```

- The `endpoints` field is the direct proof that `advertised.listeners`
  from Section 9 propagated correctly — it should say `kf2:9092`, not
  `localhost:9092` or `kf1:9092`.
- You can also check who the current controller is:

```text
get /kafka/controller
```

```json
{"version":2,"brokerid":1,"timestamp":"..."}
```

(`brokerid` here can legitimately be 1, 2, or 3 — whichever broker won
controller election; it does **not** have to be `kf1` just because `kf1`
started first historically.)

Exit the CLI:

```text
quit
```

---

# 13. Verify the Cluster Formed — via `kafka-broker-api-versions.sh`

This is the Kafka-native (not ZooKeeper-native) way to confirm all three
brokers are live and speaking the protocol, using the current
`--bootstrap-server` flag (the older `--zookeeper` flag was removed from
Kafka's admin tools in Kafka 3.0 — every command in this document uses
`--bootstrap-server`).

From any of the three broker nodes (or your laptop, if it can reach 9092):

```bash
/opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092
```

Expected — one block per broker, each listing its supported API key
versions:

```text
kf1:9092 (id: 1 rack: null) -> (
    Produce(0): 0 to 11 [usable: 11],
    Fetch(1): 0 to 17 [usable: 17],
    ...
)
kf2:9092 (id: 2 rack: null) -> (
    Produce(0): 0 to 11 [usable: 11],
    ...
)
kf3:9092 (id: 3 rack: null) -> (
    Produce(0): 0 to 11 [usable: 11],
    ...
)
```

- **Seeing all three `id:` blocks is the confirmation** — the tool
  connects to whichever bootstrap address responds first, discovers full
  cluster metadata from it, and then queries every broker it learned
  about. If only 1 or 2 blocks print, the missing broker(s) haven't joined
  the cluster yet (see Section 20).

---

# 14. Cluster Architecture Recap (What We Just Built)

```text
                     Kafka Cluster (3 brokers)

         ┌───────────────┬───────────────┬───────────────┐
         │               │               │               │
        KF1             KF2             KF3
     broker.id=1      broker.id=2      broker.id=3
      AZ-A              AZ-B              AZ-C
         │               │               │
         └───────────────┼───────────────┘
                          │
              zookeeper.connect (all 3)
             zk1:2181,zk2:2181,zk3:2181/kafka
                          │
                     ZooKeeper Ensemble
                     (already running)
```

---

# 15. Testing the Cluster — Create a Real Topic

Create a topic with 6 partitions and replication factor 3, so every one of
the 3 brokers ends up leading some partitions and replicating others —
this is the setup that actually exercises the cluster instead of just
proving one broker works.

```bash
/opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic orders \
  --partitions 6 \
  --replication-factor 3 \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092
```

Expected:

```text
Created topic orders.
```

### Why `--bootstrap-server`, not `--zookeeper`

- `kafka-topics.sh` (and every other Kafka admin CLI tool) used to accept a
  `--zookeeper` flag that talked to ZooKeeper directly. That flag was
  **removed in Kafka 3.0** — current tooling (including 3.9.2, what this
  cluster runs) only supports `--bootstrap-server`, which talks to the
  Kafka brokers themselves over the Kafka protocol. ZooKeeper is still in
  the picture underneath (this cluster's `zookeeper.connect` in Section
  9), but you no longer address it directly from the CLI.
- Passing all three broker addresses (`kf1:9092,kf2:9092,kf3:9092`) instead
  of just one gives the CLI a fallback if any single broker happens to be
  down at that exact moment — it only needs one to answer to discover the
  rest of the cluster's metadata.

---

# 16. Describe the Topic — Reading the Real Output

```bash
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092
```

Realistic expected output (exact Leader/Replicas numbers on your cluster
will vary — Kafka's replica placement algorithm spreads things out, but
not necessarily in numeric order):

```text
Topic: orders   TopicId: 5f3c... PartitionCount: 6  ReplicationFactor: 3  Configs: segment.bytes=1073741824
    Topic: orders   Partition: 0    Leader: 1   Replicas: 1,2,3   Isr: 1,2,3
    Topic: orders   Partition: 1    Leader: 2   Replicas: 2,3,1   Isr: 2,3,1
    Topic: orders   Partition: 2    Leader: 3   Replicas: 3,1,2   Isr: 3,1,2
    Topic: orders   Partition: 3    Leader: 1   Replicas: 1,3,2   Isr: 1,3,2
    Topic: orders   Partition: 4    Leader: 2   Replicas: 2,1,3   Isr: 2,1,3
    Topic: orders   Partition: 5    Leader: 3   Replicas: 3,2,1   Isr: 3,2,1
```

## What a healthy 6-partition / RF-3 spread across 3 brokers looks like

```text
                 orders  (6 partitions, RF=3)

   Partition    Leader    Replicas       Isr
   ─────────    ──────    ──────────     ──────────
      P0          B1      B1,B2,B3       B1,B2,B3
      P1          B2      B2,B3,B1       B2,B3,B1
      P2          B3      B3,B1,B2       B3,B1,B2
      P3          B1      B1,B3,B2       B1,B3,B2
      P4          B2      B2,B1,B3       B2,B1,B3
      P5          B3      B3,B2,B1       B3,B2,B1

Leadership count:   B1 → 2 partitions   B2 → 2 partitions   B3 → 2 partitions
```

Reading each column:

- **Leader** — the one broker (of the 3 replicas) currently serving all
  reads/writes for that partition. Producers/consumers for that partition
  actually talk to this broker (after being redirected there by whichever
  broker they first connected to).
- **Replicas** — the full assigned set (3 brokers, matching RF=3) — this
  list doesn't change just because a broker goes down; it's the *intended*
  placement.
- **Isr** (In-Sync Replicas) — the subset of `Replicas` that is currently
  caught up with the leader. **Healthy = Isr equals Replicas**, just
  possibly reordered.
- Healthy signs to look for:
  - `PartitionCount: 6` and `ReplicationFactor: 3` match what you asked
    for.
  - Every partition has a `Leader` that is **not** `-1` (a leader of `-1`
    means no broker is currently able to lead that partition — see
    Section 20).
  - Leadership is roughly balanced across `1`, `2`, `3` (Kafka's default
    replica-placement algorithm aims for this on a freshly created topic).
  - `Isr` has all 3 broker IDs for every partition, same members as
    `Replicas`.

---

# 17. Produce Messages With a Key — Demonstrate Partition Assignment

Kafka routes a keyed message deterministically: `partition = hash(key) %
partition_count` (for the default partitioner, when no explicit partition
is given). Same key → same partition, every time — this is what guarantees
ordering for all messages sharing a key.

Start a keyed producer:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --topic orders \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092 \
  --property "parse.key=true" \
  --property "key.separator=:"
```

Type these lines (key:value), one per line, then Ctrl+D or Ctrl+C to exit:

```text
customer-101:order-created
customer-102:order-created
customer-101:order-shipped
customer-103:order-created
customer-101:order-delivered
customer-102:order-shipped
```

- All three `customer-101` messages hash to the **same partition** — this
  is exactly the property you rely on when you need per-key ordering
  (e.g. a customer's order lifecycle events must be read in the order they
  were produced).
- `customer-102` and `customer-103` may land on the same or different
  partitions from `customer-101` and from each other — that's expected
  and fine; ordering is only guaranteed **within** a partition, i.e.
  **within** a key.

---

# 18. Consume Back — Prove Partition Assignment and Ordering

## Consume everything first, with keys visible

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092 \
  --from-beginning \
  --property print.key=true \
  --property key.separator=":" \
  --max-messages 6
```

Expected (order across different keys is not guaranteed globally — only
within a key/partition):

```text
customer-101:order-created
customer-103:order-created
customer-102:order-created
customer-101:order-shipped
customer-101:order-delivered
customer-102:order-shipped
```

## Find out which partition a key actually landed on

Use `kafka-console-consumer.sh`'s low-level `--partition`/`--offset` flags
to read one partition directly and confirm the key/ordering claim. First,
guess a partition (0 is a reasonable start) and read from its beginning:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092 \
  --partition 0 \
  --offset earliest \
  --property print.key=true \
  --property key.separator=":" \
  --max-messages 10
```

If `customer-101`'s three messages don't show up here, try the next
partition (`--partition 1`, `--partition 2`, ...) until you find them —
once found, **all three `customer-101` messages will be in that same
partition, in the exact order they were produced**:

```text
customer-101:order-created
customer-101:order-shipped
customer-101:order-delivered
```

Reading from a specific numeric offset instead of `earliest` proves the
same point at the log-position level:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092 \
  --partition 0 \
  --offset 0 \
  --property print.key=true \
  --property key.separator=":" \
  --max-messages 1
```

- `--offset earliest` (or a literal `0`) plus `--partition N` bypasses
  consumer-group offset tracking entirely and reads directly from a known
  position — this is the tool for "prove exactly what's at position X of
  partition Y," not for normal application consumption (which should use a
  consumer group instead).
- `--property` here is the still-supported (if being phased toward
  `--formatter-property` in newer tooling) way to pass formatter options
  like `print.key` and `key.separator` to the console consumer/producer.

---

# 19. Replication Proof — Kill a Broker (Preview of the Full Resiliency Lesson)

> **This is a short, focused preview only.** A full broker-failure /
> recovery / under-replication deep dive is a separate later lesson in
> this series — don't treat this section as that lesson. The only goal
> here is to show, in under 5 minutes, that the replication you just set
> up in Sections 15–16 actually does something when a broker disappears.

## Step 1 — Note current state

```bash
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server kf1:9092,kf3:9092
```

(Deliberately bootstrapping through `kf1`/`kf3` only here, since we're
about to take `kf2` down.) Find the partitions where `kf2` (broker id `2`)
is currently the **Leader** — from the Section 16 example, that's
partitions 1 and 4.

## Step 2 — Stop `kf2`

On `kf2`:

```bash
sudo systemctl stop kafka
```

## Step 3 — Describe again from a surviving broker

```bash
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server kf1:9092,kf3:9092
```

Expected changes:

```text
    Topic: orders   Partition: 0    Leader: 1   Replicas: 1,2,3   Isr: 1,3
    Topic: orders   Partition: 1    Leader: 3   Replicas: 2,3,1   Isr: 3,1
    Topic: orders   Partition: 2    Leader: 3   Replicas: 3,1,2   Isr: 3,1,2
    Topic: orders   Partition: 3    Leader: 1   Replicas: 1,3,2   Isr: 1,3
    Topic: orders   Partition: 4    Leader: 1   Replicas: 2,1,3   Isr: 1,3
    Topic: orders   Partition: 5    Leader: 3   Replicas: 3,2,1   Isr: 3,1
```

What changed:

```text
Before                          After kf2 stopped
──────                          ─────────────────
P1 Leader: 2                    P1 Leader: 3  (new election)
P4 Leader: 2                    P4 Leader: 1  (new election)
Isr always had 3 members        Isr for every partition drops
                                 to 2 members (kf2 removed)
```

- Every partition still has a **non-`-1` Leader** — the cluster kept
  serving reads/writes for `orders` the whole time, because RF=3 meant
  every partition had 2 surviving replicas even with `kf2` gone.
- Every `Isr` list **shrank from 3 to 2** — this is Kafka reporting
  "under-replicated": the `Replicas` list still says 3 brokers should have
  a copy, but only 2 are currently in sync.
- The two partitions `kf2` was leading (`P1`, `P4`) got **new leaders**
  elected from their remaining in-sync replicas — this happened
  automatically, with no manual intervention.

## Step 4 — Try producing/consuming while `kf2` is down

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --topic orders \
  --bootstrap-server kf1:9092,kf3:9092 \
  --property "parse.key=true" \
  --property "key.separator=:"
```

```text
customer-104:order-created
```

This should succeed normally — proving the cluster tolerated the broker
loss without producer/consumer-visible failure for this topic.

## Step 5 — Restart `kf2` and watch it recover

On `kf2`:

```bash
sudo systemctl start kafka
```

Wait a few seconds, then describe again:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092
```

Expected: every `Isr` list grows back to 3 members, including `2`, once
`kf2` finishes catching up (fetching the messages it missed while down,
including `customer-104`):

```text
    Topic: orders   Partition: 0    Leader: 1   Replicas: 1,2,3   Isr: 1,3,2
    Topic: orders   Partition: 1    Leader: 3   Replicas: 2,3,1   Isr: 3,1,2
    ...
```

- Note that `kf2` **rejoins the `Isr`** but does **not** automatically
  reclaim leadership of `P1`/`P4` just by coming back — by default Kafka
  only fails leadership back under specific conditions (e.g. automatic
  preferred-leader rebalancing, which is a separate topic covered when we
  get to the full resiliency lesson). Seeing `kf2` back in every `Isr`
  list is the recovery signal to look for here.

---

# 20. Troubleshooting Notes

## A broker hasn't joined the cluster yet

`--describe` output with a broker missing from a partition it should be
in, or a `Leader: -1`:

```text
    Topic: orders   Partition: 1    Leader: -1   Replicas: 2,3,1   Isr: 3,1
```

- `Leader: -1` means **no** in-sync replica is currently available to lead
  that partition — worse than the Section 19 example, where `kf1`/`kf3`
  could still take over. This shows up if, say, two of the three replica
  brokers for that partition are down/unreachable simultaneously.
- First checks: `systemctl status kafka` on the missing broker,
  `journalctl -u kafka -n 100` for startup errors, and
  `ls /kafka/brokers/ids` via `zkCli.sh` (Section 12) to confirm whether
  ZooKeeper even sees that broker as registered.

## `/etc/hosts` incomplete on one node

Symptoms look like the command hanging, then timing out, or connecting to
one broker but failing once redirected to another:

```text
[2026-08-22 ...] WARN [Producer clientId=console-producer] Connection to
node 2 (kf2/<unresolved>:9092) could not be established. Broker may not be
available.
```

or, from the CLI tools:

```text
org.apache.kafka.common.errors.TimeoutException: Topic orders not present
in metadata after 60000 ms.
```

- This is the classic symptom of a client resolving the **bootstrap**
  broker fine, getting told "partition X's leader is `kf2`," and then
  failing to resolve or reach `kf2` specifically — almost always an
  incomplete `/etc/hosts` on whichever machine is running the CLI command
  (could be your own broker node, if you're running commands from `kf1`
  but `kf1`'s `/etc/hosts` never got `kf2`'s entry added).
- Fix: re-run Section 5's `getent hosts` check on the node you're actually
  running commands from, not just the brokers.

## Replication factor larger than available brokers

If you ever try to create a topic with `--replication-factor` higher than
the number of live brokers:

```text
Error while executing topic command : Replication factor: 4 larger than
available brokers: 3.
```

This is Kafka enforcing the same rule from `Lesson-4/01` Section 8 —
`brokers >= replication factor` — at topic-creation time.

---

# 21. Hands-On Checklist

```text
AWS
□ Launch kf2 (correct AZ subnet, kafka-sg, kafka-lab key pair)
□ Launch kf3 (different AZ subnet from kf1 and kf2)
□ Confirm kafka-sg 9092 self-reference rule
□ Confirm zookeeper-sg 2181 rule sources from kafka-sg
□ Get public + private IPs for kf2, kf3

Networking
□ Re-verify /etc/hosts on zk1, zk2, zk3 (all 6 entries)
□ Add /etc/hosts on kf1 (all 6 entries)
□ Add /etc/hosts on kf2 (all 6 entries)
□ Add /etc/hosts on kf3 (all 6 entries)
□ getent hosts + ping verification on every node

Installation (kf2, kf3)
□ Install Java 17
□ Download/extract Kafka 3.9.2 to /opt/kafka
□ Create kafka system user
□ Create /var/lib/kafka/data

Configuration (kf2, kf3)
□ broker.id set uniquely (2, 3)
□ listeners / advertised.listeners point at own hostname
□ zookeeper.connect matches kf1 exactly (same 3 ZK nodes + /kafka chroot)
□ systemd kafka.service created, enabled, started

Cluster verification
□ zkCli.sh: ls /kafka/brokers/ids → [1, 2, 3]
□ zkCli.sh: get /kafka/brokers/ids/2 shows correct endpoint
□ kafka-broker-api-versions.sh shows all 3 broker ids

Testing (course item 31)
□ Create topic: 6 partitions, RF=3, --bootstrap-server
□ --describe: PartitionCount/ReplicationFactor correct
□ --describe: every Leader present, Isr == Replicas
□ Produce with key, same key groups onto one partition
□ Consume --from-beginning with print.key=true
□ Consume specific --partition/--offset, prove per-key ordering
□ Stop kf2 → Isr shrinks, leader re-elected for its partitions
□ Produce/consume still works with kf2 down
□ Restart kf2 → Isr recovers to 3 members
```
