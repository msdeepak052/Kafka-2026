# Hands-On: Kafka Broker Setup on AWS

> **Scope:** Course items **27. Hands-On: Kafka AWS Setup**, **28. Hands-On:
> Single Kafka Broker Setup**, and **29. Hands-On: Running Kafka Commands.**
>
> **Goal:** Stand up the first real Kafka broker (`kf1`) on top of the AWS
> infrastructure you already built in Lesson-3, wire it to the existing
> 3-node ZooKeeper ensemble, and run real `kafka-topics.sh` /
> `kafka-console-producer.sh` / `kafka-console-consumer.sh` commands against
> it end to end.

This file builds **on top of** Lesson-3's AWS lab, not instead of it. You
will **not** recreate the VPC, the `kafka-lab` key pair, or the ZooKeeper
nodes — they already exist and keep running exactly as Lesson-3 left them.

---

# 1. What We Are Building

## Recap — already exists (from Lesson-3, don't recreate)

* AWS account's **default VPC**, one subnet per AZ (the same 3 AZs used for
  `zk1`/`zk2`/`zk3`)
* SSH key pair **`kafka-lab`** (`kafka-lab.pem`, already downloaded and
  `chmod 400`'d)
* Security group **`zookeeper-sg`** with inbound 22 (My IP) and
  2181/2888/3888/8080 (source: `zookeeper-sg` itself)
* Three EC2 nodes `zk1`, `zk2`, `zk3` — Ubuntu 24.04 LTS, `t3.small`,
  ZooKeeper 3.9.5 at `/opt/zookeeper`, running as `zookeeper.service`, data
  in `/var/lib/zookeeper`
* `/etc/hosts` entries on the ZK nodes for `zk1`/`zk2`/`zk3`

## New in this file

* Security group **`kafka-sg`** (new)
* One new inbound rule added **back onto** `zookeeper-sg` (closing a loop
  left open in Lesson-3 — see Section 4)
* One new EC2 instance: **`kf1`**

> This file deliberately stops at a **single broker**. `kf2` and `kf3`
> follow the exact same pattern in the next file, once you're comfortable
> with the single-node flow — this mirrors the course's own ordering
> ("Single Kafka Broker Setup" before multi-broker).

## Target architecture for this file

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
                             │  :2181
                             ▼
                       ┌───────────┐
                       │    kf1    │
                       │broker.id=1│
                       └───────────┘
                             ▲
                             │ :9092
                             │
                    Producers / Consumers
                     (inside the VPC)
```

* `kf1` is the only broker in this file — it talks to **all three**
  ZooKeeper nodes (any ZK node can answer, per the quorum you built in
  Lesson-3), but only one broker exists on the Kafka side for now.
* Producers/consumers in this diagram means the CLI tools you'll run
  yourself in Section 16 — from inside the VPC.

## Node naming convention for the rest of Lesson-4

| Node | Hostname | AZ   | Example private IP | Status                |
| ---- | -------- | ---- | ------------------- | ---------------------- |
| KF1  | `kf1`    | AZ-A | `10.0.1.11`          | **Built in this file** |
| KF2  | `kf2`    | AZ-B | `10.0.2.11`          | Next file               |
| KF3  | `kf3`    | AZ-C | `10.0.3.11`          | Next file               |

This is the same `broker.id ↔ hostname` pattern as ZooKeeper's `myid ↔
server.X` from Lesson-3: `kf1` gets `broker.id=1`, and `kf2`/`kf3` will get
`broker.id=2` and `broker.id=3` when you build them. As with the ZK table in
Lesson-3, the IPs above are **illustrative** — use whatever your subnet
actually assigns.

---

# 2. Which Kafka Version — and Why 3.9.2

For this lab:

```text
Apache Kafka 3.9.2
Scala build: 2.13
```

## Why 3.9.2 specifically

* Kafka **4.0.0** (released March 18, 2025) removed ZooKeeper mode
  **entirely** — KRaft is the only supported metadata mode from 4.0
  onward. Apache's own 4.0 upgrade documentation states that broker
  upgrades to 4.0.0+ require KRaft mode, and any cluster still in ZooKeeper
  mode **must** be migrated to KRaft before it can move to 4.0.x at all.
* The **3.9.x** line (3.9.0 → 3.9.1 → 3.9.2) is the **last** release line
  that still runs in ZooKeeper mode — it's commonly referred to as the
  "bridge release" specifically because it's the version you use to
  migrate off ZooKeeper before eventually moving to 4.x.
* Since this course is deliberately teaching the ZooKeeper-based
  architecture first (before KRaft), **3.9.2** is the correct choice: it's
  the newest release that still speaks to a ZooKeeper ensemble the way
  Lesson-3 built one.
* Kafka 3.9.2 was itself a bug-fix release on top of 3.9 (its release
  announcement is scoped to fixes like KIP-1252's `AlterConfigPolicy`
  compatibility behavior between ZK and KRaft modes) — it's not a new
  feature line, just the most current, patched build of the ZK-capable
  3.9 series.

## Why the download comes from `archive.apache.org`, not `dlcdn.apache.org`

* Apache's official release-distribution policy: **`dlcdn.apache.org`
  (and the mirror network) only ever serves the current, recommended
  release** of a project. Every official release is also copied
  permanently to **`archive.apache.org`**, and stays there even after it's
  removed from the current-download mirrors.
* Since Kafka 4.x is now the current release line, **`dlcdn.apache.org`
  no longer has a `3.9.2` directory at all** — that release has aged out
  of the "current" mirror by policy, not by accident.
* `archive.apache.org` is Apache's permanent, always-available archive for
  every past release of every project — it's the correct, sanctioned place
  to get an older-but-still-supported release like 3.9.2.
* Verified: `https://archive.apache.org/dist/kafka/3.9.2/` is live and
  lists `kafka_2.13-3.9.2.tgz` (117 MB, with matching `.asc`/`.sha512`
  checksum files) — this is the exact URL used in Section 9 below.

```text
Current release (4.x)
   → dlcdn.apache.org / mirrors
        (only the newest release lives here)

Superseded release (3.9.2, still ZK-capable)
   → archive.apache.org
        (every release, forever)
```

## Java

* Reuse the **same `openjdk-17-jre-headless`** you already installed on
  the ZK nodes in Lesson-3 — no new Java install is needed on `kf1` beyond
  the standard `apt` package.
* Kafka 3.9.x runs fine on Java 17. This also keeps you forward-consistent:
  Kafka 4.x **requires** Java 17+, so nothing here needs to change later
  just because of a Java-version mismatch.

---

# 3. AWS Infrastructure We're Reusing

Before touching the console, confirm you still have all of this from
Lesson-3 (if any of it is gone, go back and rebuild it first — this file
assumes it's live):

```text
Default VPC
 ├── Subnet (AZ-A) ── zk1
 ├── Subnet (AZ-B) ── zk2
 └── Subnet (AZ-C) ── zk3

Key pair: kafka-lab (kafka-lab.pem, chmod 400)

Security group: zookeeper-sg
 ├── 22   ← My IP
 ├── 2181 ← zookeeper-sg (self)
 ├── 2888 ← zookeeper-sg (self)
 ├── 3888 ← zookeeper-sg (self)
 └── 8080 ← zookeeper-sg (self)
```

Nothing in this section is new — it's just the checklist of what must
already be running before you start Section 4.

---

# 4. Security Groups — Create `kafka-sg`, Fix Up `zookeeper-sg`

## Step 4.1 — Create the new security group `kafka-sg`

1. **EC2 console** → left navigation pane → **Network & Security** →
   **Security Groups**.
2. Choose **Create security group**.
3. **Security group name:** `kafka-sg`
4. **Description:** `Kafka broker lab (kf1/kf2/kf3)`
5. **VPC:** select your **default VPC** (same one everything else uses).
6. Under **Inbound rules**, choose **Add rule** for each row:

   | Type       | Port range | Source                                          |
   | ---------- | ---------- | ------------------------------------------------ |
   | SSH        | 22         | **My IP**                                         |
   | Custom TCP | 9092       | **Custom** → search/select `kafka-sg` itself       |
   | Custom TCP | 8080       | **My IP**                                         |

   * The **9092** rule (source = `kafka-sg` itself) is the same
     SG-self-reference pattern `zookeeper-sg` used in Lesson-3 — it lets
     any instance in this security group reach any other instance in this
     security group on 9092, without opening the port to the internet.
     9092 is used for **both** client traffic (producers/consumers) and
     broker-to-broker replication traffic in this lab's simple PLAINTEXT
     setup, so one rule covers both.
   * The **8080** rule (source = **My IP**, not a self-reference) is
     reserved now for the **AKHQ** Kafka management UI you'll install in a
     later file — it isn't used yet in this file, but creating the rule
     now avoids a detour later. Note this is a *different* 8080 than
     ZooKeeper's AdminServer 8080 — they live on different hosts/security
     groups, so there's no port conflict.
7. Leave **Outbound rules** at the default (all traffic allowed).
8. Choose **Create security group**.

## Step 4.2 — Close the loop on `zookeeper-sg`

In Lesson-3, the `zookeeper-sg` inbound rule for port 2181 had its
**source** set to `zookeeper-sg` itself — that was a placeholder at the
time, because no Kafka security group existed yet to reference. Now that
`kafka-sg` exists, add the **correct** rule: only actual Kafka brokers
(not just "any member of the ZK ensemble's own security group") should be
able to reach ZooKeeper's client port.

1. **EC2 console** → **Security Groups**.
2. Select **`zookeeper-sg`**.
3. **Inbound rules** tab → **Edit inbound rules**.
4. **Add rule**:

   | Type       | Port range | Source                                    |
   | ---------- | ---------- | -------------------------------------------- |
   | Custom TCP | 2181       | **Custom** → search/select `kafka-sg`         |
5. Choose **Save rules**.

You are **not** removing the old self-referencing 2181 rule — leave it in
place (it's still useful, e.g. for running `zkCli.sh` from one ZK node
against another). You're only **adding** the new, more precise rule
alongside it:

```text
zookeeper-sg inbound :2181
 ├── zookeeper-sg (self)   ← kept from Lesson-3 (ZK-node-to-ZK-node admin)
 └── kafka-sg               ← NEW: actual Kafka brokers
```

---

# 5. Launch `kf1`

1. **EC2 console** → **Launch instance**.
2. **Name and tags → Name:** `kf1`.
3. **Application and OS Images (Amazon Machine Image):** search `Ubuntu` →
   choose **Ubuntu Server 24.04 LTS (HVM), SSD Volume Type** — the same
   Quick Start AMI used for `zk1`/`zk2`/`zk3` in Lesson-3.
4. **Instance type:** `t3.small` (same as the ZK nodes — no need for
   anything bigger for this lab).
5. **Key pair (login):** choose the existing **`kafka-lab`** key pair — do
   **not** create a new one.
6. **Network settings** → **Edit**:
   * **VPC:** your default VPC.
   * **Subnet:** pick the **AZ-A** subnet (the same AZ as `zk1`) — see the
     table in Section 1.
   * **Auto-assign public IP:** **Enable**.
   * **Firewall (security groups):** **Select existing security group** →
     pick **`kafka-sg`** (not `zookeeper-sg` — `kf1` only needs the Kafka
     SG; it reaches ZooKeeper over the network, not by being *in*
     `zookeeper-sg`).
7. **Configure storage:** **20 GiB**, **gp3** (matches the ZK nodes).
8. Leave **Advanced details** at defaults.
9. Confirm **Number of instances** is `1` → **Launch instance**.

## Get the IPs

1. **EC2 console → Instances**.
2. Wait for `kf1` to show **Running**.
3. Note its **public IPv4 address** (for SSH) and **private IPv4 address**
   (for `/etc/hosts` in Section 7).

---

# 6. SSH In

```bash
ssh -i kafka-lab.pem ubuntu@<KF1-PUBLIC-IP>
```

Same key pair, same login pattern as `zk1`/`zk2`/`zk3` — nothing new here.

Verify you're on the right machine before doing anything else:

```bash
hostname
hostname -I
```

> If the connection times out, it's almost always the same cause as in
> Lesson-3: the SSH rule's **Source** was set to **My IP** at creation
> time and doesn't auto-refresh — edit `kafka-sg`'s inbound SSH rule and
> re-select **My IP** if your address has changed.

---

# 7. Extend `/etc/hosts` Across the Lab

`server.properties`'s `zookeeper.connect` (Section 12) and `listeners`
(also Section 12) both reference nodes by **short hostname**
(`zk1`, `zk2`, `zk3`, `kf1`) — and just like in Lesson-3, the default VPC's
built-in DNS does **not** resolve short names, only the auto-generated
`ip-10-0-x-y.ec2.internal` form. You're extending the same `/etc/hosts`
convention Lesson-3 established, now across **both** node types.

## On `kf1` — add all ZK nodes plus itself

Using the **private IPs** from Lesson-3 (ZK) and Section 5 above (`kf1`):

```bash
sudo tee -a /etc/hosts > /dev/null <<'EOF'
10.0.1.10 zk1
10.0.2.10 zk2
10.0.3.10 zk3
10.0.1.11 kf1
EOF
```

Replace every IP above with your **actual** values.

## On `zk1`, `zk2`, and `zk3` — add the new `kf1` entry

SSH into each of the three ZK nodes and append just the one new line
(their existing `zk1`/`zk2`/`zk3` entries from Lesson-3 stay as they are):

```bash
sudo tee -a /etc/hosts > /dev/null <<'EOF'
10.0.1.11 kf1
EOF
```

## Verify from `kf1`

```bash
getent hosts zk1
getent hosts zk2
getent hosts zk3
ping -c 2 zk1
```

If `getent` resolves but `ping`/TCP checks hang, that's Section 4's
security-group rules, not DNS — double check the `kafka-sg → 2181 →
zookeeper-sg` rule.

---

# 8. Verify Java

Java was already installed as part of the ZooKeeper setup pattern in
Lesson-3 — install it on `kf1` the same way (it's a fresh instance, so it
isn't there yet):

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

# 9. Install Kafka 3.9.2

```bash
cd /tmp

wget https://archive.apache.org/dist/kafka/3.9.2/kafka_2.13-3.9.2.tgz
```

Extract:

```bash
tar -xzf kafka_2.13-3.9.2.tgz
```

Move into place — mirrors ZooKeeper's `/opt/zookeeper` convention from
Lesson-3:

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
LICENSE
NOTICE
site-docs
```

---

# 10. Create the `kafka` System User and Data Directory

Don't run Kafka as root — same pattern as the `zookeeper` user in
Lesson-3.

```bash
sudo useradd \
  --system \
  --home /opt/kafka \
  --shell /usr/sbin/nologin \
  kafka

sudo chown -R kafka:kafka /opt/kafka
```

Data directory (`log.dirs` — this is where partition segment files live,
Kafka's equivalent of ZooKeeper's `dataDir`):

```bash
sudo mkdir -p /var/lib/kafka/data

sudo chown -R kafka:kafka /var/lib/kafka
```

---

# 11. Create the ZooKeeper Chroot Znode Before First Start

This step matters and is easy to skip by accident.

## Why a chroot at all

`zookeeper.connect` for this lab will be:

```text
zk1:2181,zk2:2181,zk3:2181/kafka
```

The trailing `/kafka` is a **chroot path** — every znode Kafka creates
(`/brokers`, `/controller`, `/config`, and so on) gets created **under**
`/kafka` instead of directly at the ZooKeeper root `/`.

* This same 3-node ZK ensemble could, in principle, be reused later by a
  second application (another Kafka cluster, or something else entirely).
  Without a chroot, every application's znodes would collide in the same
  flat root namespace.
* With a chroot, each application gets its own isolated subtree — Kafka's
  metadata lives entirely inside `/kafka` and can't collide with, or be
  accidentally browsed alongside, anything else in the ensemble.
* This is standard, widely recommended practice specifically for shared
  ZooKeeper ensembles — not something unique to this lab.

## Does Kafka create `/kafka` automatically?

Modern Kafka's ZooKeeper client code path (`KafkaZkClient.createTopLevelPaths()`
→ `makeSurePersistentPathExists()` → a recursive create call) is generally
understood to create its top-level znodes — including the chroot itself —
recursively if they don't already exist. So in most default lab setups,
Kafka *would* likely create `/kafka` on its own on first start.

That said:

* Older Kafka/ZooKeeper client behavior did **not** always do this
  reliably, and real-world reports of ACL-restricted ZooKeeper deployments
  (see `KAFKA-12866`) show cases where a broker fails at startup
  specifically because it can't create/reach a path it expected to
  already exist.
* Creating it explicitly costs one command and is correct **regardless**
  of which behavior your exact setup hits — so we do it explicitly here
  rather than relying on an implicit auto-create.

## Create it

`zkCli.sh` lives on the ZooKeeper nodes (`/opt/zookeeper/bin/zkCli.sh`
from Lesson-3) — it isn't installed on `kf1`. SSH into `zk1` (or reuse an
existing session there) and run:

```bash
/opt/zookeeper/bin/zkCli.sh -server zk1:2181
```

Inside the CLI session:

```text
create /kafka ""
```

Verify:

```text
ls /
```

Expected:

```text
[zookeeper, kafka]
```

Exit the CLI (`quit`) and go back to `kf1` for the rest of this file.

---

# 12. Configure `server.properties`

This is the file previewed conceptually in the previous lecture (Lesson-4,
file 02) — `/opt/kafka/config/server.properties` is where that
configuration actually lands.

```bash
sudo nano /opt/kafka/config/server.properties
```

Replace its contents with:

```properties
# ============================================================
# kf1 - server.properties (Kafka 3.9.2, ZooKeeper mode)
# ============================================================

# --- Broker identity ---
broker.id=1

# --- Network ---
listeners=PLAINTEXT://kf1:9092
advertised.listeners=PLAINTEXT://kf1:9092

num.network.threads=3
num.io.threads=8

socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600

# --- Log / data directory ---
log.dirs=/var/lib/kafka/data
num.partitions=1
num.recovery.threads.per.data.dir=1

# --- Single-broker replication defaults (revisit once kf2/kf3 join) ---
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

# --- Retention ---
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000

# --- ZooKeeper (chroot from Section 11) ---
zookeeper.connect=zk1:2181,zk2:2181,zk3:2181/kafka
zookeeper.connection.timeout.ms=18000

group.initial.rebalance.delay.ms=0
```

## Notes on the choices above

* **`broker.id=1`** — `kf1` ↔ `1`, `kf2` ↔ `2` (next file), `kf3` ↔ `3`
  (next file). Same identity pattern as ZooKeeper's `myid` from Lesson-3.
* **`listeners` / `advertised.listeners` — kept deliberately simple here.**
  Both are set to the same single `PLAINTEXT://kf1:9092` value. This only
  works for clients that can resolve/reach `kf1` on the private network —
  i.e., **from inside this VPC**. Connecting to this broker from outside
  AWS (your laptop, over the internet) does **not** work with this
  configuration and is **not** solved in this file — it's its own
  dedicated lesson later in the course ("32. Can I connect to my Kafka
  cluster?" and "33. advertised.listeners"). Don't chase that problem yet.
* **`offsets.topic.replication.factor=1`** etc. — these replication
  defaults are only valid because there's exactly **one** broker right
  now. Once `kf2`/`kf3` exist in the next file, revisit these (a
  single-broker replication factor of 1 offers no redundancy at all for
  Kafka's own internal topics).
* **`log.dirs=/var/lib/kafka/data`** plays the same role for Kafka that
  `dataDir` played for ZooKeeper in Lesson-3 — this is where the actual
  partition segment files live on disk.

---

# 13. Create the `kafka.service` systemd Unit

```bash
sudo nano /etc/systemd/system/kafka.service
```

```ini
[Unit]
Description=Apache Kafka Broker (kf1)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple

User=kafka
Group=kafka

Environment="KAFKA_HEAP_OPTS=-Xms512M -Xmx512M"

ExecStart=/opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties
ExecStop=/opt/kafka/bin/kafka-server-stop.sh

Restart=on-failure
RestartSec=5

LimitNOFILE=100000

[Install]
WantedBy=multi-user.target
```

## Two choices worth explaining

* **`KAFKA_HEAP_OPTS=-Xms512M -Xmx512M`** — a `t3.small` only has 2 GiB of
  RAM. Just like the memory principle from Lesson-4, file 01 (Section 18,
  "Don't allocate all available RAM to the JVM heap"), Kafka leans heavily
  on the **OS page cache** for read/write performance, not JVM heap. A
  small, fixed heap on this small lab instance leaves the rest of the RAM
  free for the page cache to do its job.
* **`LimitNOFILE=100000`** — noticeably higher than ZooKeeper's
  `LimitNOFILE=65536` from Lesson-3. Kafka opens a file handle per log
  segment **and** per client/replica network connection, so even a modest
  broker can accumulate far more open files than a ZooKeeper node doing
  pure coordination work.

Reload and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable kafka
sudo systemctl start kafka
```

---

# 14. Start Kafka and Verify

```bash
systemctl status kafka
```

Expected:

```text
Active: active (running)
```

Watch the logs for a clean startup:

```bash
journalctl -u kafka -f
```

Look for a line like:

```text
[KafkaServer id=1] started (kafka.server.KafkaServer)
```

That single line is the strongest signal that `kf1` came up cleanly,
connected to the ZooKeeper ensemble, and registered itself.

Last 100 lines (non-following):

```bash
journalctl -u kafka --no-pager -n 100
```

---

# 15. Verify Broker Registration in ZooKeeper

Back on `zk1` (or any ZK node), reconnect with `zkCli.sh`:

```bash
/opt/zookeeper/bin/zkCli.sh -server zk1:2181
```

Confirm Kafka's tree exists under the chroot:

```text
ls /kafka
```

Expected — roughly this set of top-level znodes (exact set can vary
slightly by version, but `brokers` and `controller` must be present):

```text
[admin, brokers, cluster, config, controller, controller_epoch, feature, isr_change_notification, latest_producer_id_block, log_dir_event_notification]
```

Confirm `kf1` registered as broker `1`:

```text
ls /kafka/brokers/ids
```

Expected:

```text
[1]
```

Inspect the registration payload:

```text
get /kafka/brokers/ids/1
```

Expected — roughly:

```json
{
  "listener_security_protocol_map": {"PLAINTEXT": "PLAINTEXT"},
  "endpoints": ["PLAINTEXT://kf1:9092"],
  "jmx_port": -1,
  "features": {},
  "host": "kf1",
  "port": 9092,
  "version": 5
}
```

Seeing `kf1:9092` here — matching `advertised.listeners` from Section 12
— confirms the broker registered itself correctly under the ensemble's
`/kafka` chroot.

```text
Architecture at this point

kf1 (broker.id=1)
   │
   │ zookeeper.connect=zk1,zk2,zk3/kafka
   ▼
ZK ensemble
   │
   └── /kafka
         ├── brokers/ids/1  → {"host":"kf1","port":9092,...}
         ├── controller
         └── ...
```

---

# 16. Running Kafka Commands (Course Item 29)

All commands below use `--bootstrap-server` (talking to the **broker**,
`kf1:9092`) — this is the modern way to administer topics, not
`--zookeeper` (that flag was removed from `kafka-topics.sh` well before
3.9.2; administration goes through the broker even while the cluster
itself is still running in ZooKeeper mode).

## Create a topic

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kf1:9092 \
  --create \
  --topic test-topic \
  --partitions 3 \
  --replication-factor 1
```

Expected:

```text
Created topic test-topic.
```

## List topics

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kf1:9092 --list
```

Expected:

```text
test-topic
```

## Describe the topic

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kf1:9092 \
  --describe \
  --topic test-topic
```

Expected:

```text
Topic: test-topic	TopicId: <some-uuid>	PartitionCount: 3	ReplicationFactor: 1	Configs:
	Topic: test-topic	Partition: 0	Leader: 1	Replicas: 1	Isr: 1
	Topic: test-topic	Partition: 1	Leader: 1	Replicas: 1	Isr: 1
	Topic: test-topic	Partition: 2	Leader: 1	Replicas: 1	Isr: 1
```

Every partition's `Leader`, `Replicas`, and `Isr` all point at broker `1`
— unsurprising, since `kf1` is currently the only broker.

## Produce some messages

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kf1:9092 \
  --topic test-topic
```

Type a few lines at the `>` prompt, then `Ctrl+C` to exit:

```text
>hello kafka
>this is broker kf1
>message three
>
```

## Consume the messages

In a second terminal/session:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kf1:9092 \
  --topic test-topic \
  --from-beginning
```

Expected:

```text
hello kafka
this is broker kf1
message three
```

`Ctrl+C` to exit (a "Processed a total of N messages" line on exit is
normal, not an error).

## Delete the topic

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kf1:9092 \
  --delete \
  --topic test-topic
```

* This command typically returns **silently** (no confirmation text) — deletion is asynchronous.
* Confirm it actually disappeared a few seconds later:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kf1:9092 --list
```

Expected: empty output.

---

# 17. Troubleshooting

| Symptom                                                                 | Likely cause                                                                                       | First check                                                                                 |
| ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `kf1` process looks "up" but `/kafka/brokers/ids` stays empty            | The `/kafka` chroot wasn't created before the first start, and it hit an environment where auto-create didn't happen | Section 11 — create `/kafka` via `zkCli.sh`, then `sudo systemctl restart kafka`               |
| `journalctl -u kafka` shows repeated connection/timeout errors to ZK    | `kafka-sg → 2181 → zookeeper-sg` rule from Section 4 is missing or mistyped                            | From `kf1`: `nc -zv zk1 2181` (and zk2/zk3) — if that hangs, it's the security group, not Kafka |
| Producer/consumer works from inside the VPC but a client elsewhere gets `Connection to node 1 could not be established` after the initial connect succeeds | `advertised.listeners=PLAINTEXT://kf1:9092` only resolves/routes inside the VPC — this is expected, not a bug, given Section 12's note | Not solved in this file — see the dedicated `advertised.listeners` lesson later                 |

General flow, same shape as Lesson-3's troubleshooting matrix:

```text
Kafka broker won't register
       │
       ▼
DNS resolution? (getent hosts zk1/zk2/zk3)
       │
       ├── NO → fix /etc/hosts (Section 7)
       ▼
TCP 2181 reachable? (nc -zv zk1 2181)
       │
       ├── NO → fix zookeeper-sg (Section 4.2)
       ▼
/kafka chroot exists? (zkCli.sh: ls /)
       │
       ├── NO → create it (Section 11)
       ▼
journalctl -u kafka shows "started (kafka.server.KafkaServer)"?
       │
       ├── NO → read the actual error, don't guess
       ▼
/kafka/brokers/ids shows [1]
```

---

# 18. What's Next

* **`kf2` and `kf3`** — built the same way as `kf1` in the next file, each
  in their own AZ (`AZ-B`, `AZ-C`), completing the 3-broker cluster and
  giving you real replication (`replication-factor > 1`) to work with.
* **AKHQ** — the management UI the `kafka-sg` 8080 rule was pre-created
  for in Section 4 gets installed and connected in a later file.
* **`advertised.listeners` / external connectivity** — deliberately left
  unsolved here (Section 12). It gets its own focused treatment in
  "32. Can I connect to my Kafka cluster?" and "33. advertised.listeners."

---

# 19. Hands-On Checklist

```text
AWS
□ Create kafka-sg (22, 9092 self-ref, 8080 reserved for AKHQ)
□ Add zookeeper-sg inbound rule: 2181 ← kafka-sg
□ Launch kf1 (Ubuntu 24.04, t3.small, existing kafka-lab key pair, kafka-sg)
□ Retrieve kf1's public + private IP
□ SSH into kf1

Networking
□ Add zk1/zk2/zk3/kf1 to kf1's /etc/hosts
□ Add kf1 to zk1's, zk2's, and zk3's /etc/hosts
□ Verify DNS + reachability (getent, ping, nc)

Installation
□ Verify Java 17 on kf1
□ Download Kafka 3.9.2 from archive.apache.org
□ Move to /opt/kafka
□ Create kafka system user
□ Create /var/lib/kafka/data

ZooKeeper
□ Create the /kafka chroot znode via zkCli.sh before first start

Configuration
□ Set broker.id, listeners, advertised.listeners
□ Set log.dirs
□ Set zookeeper.connect with the /kafka chroot
□ Understand why single-broker replication defaults are temporary

Operations
□ Create kafka.service systemd unit
□ Start Kafka, verify systemctl status
□ Confirm "started (kafka.server.KafkaServer)" in journalctl
□ Confirm broker 1 registered under /kafka/brokers/ids

CLI
□ Create a topic
□ List topics
□ Describe a topic
□ Produce messages
□ Consume messages
□ Delete a topic
```

---

## References

* [Apache Kafka 4.0.0 Release Announcement](https://kafka.apache.org/blog/2025/03/18/apache-kafka-4.0.0-release-announcement/) — confirms Kafka 4.0 ships with ZooKeeper mode fully removed, KRaft-only.
* [Apache Kafka — Upgrading to 4.0](https://kafka.apache.org/40/getting-started/upgrade/) — confirms broker upgrades to 4.0.0+ require KRaft mode, and ZK-mode clusters must migrate first.
* [Apache Kafka 3.9.2 Release Announcement](https://kafka.apache.org/blog/2026/02/21/apache-kafka-3.9.2-release-announcement/) — scope of the 3.9.2 bug-fix release (KIP-1252 ZK/KRaft `AlterConfigPolicy` compatibility).
* [Apache Release Distribution Policy](https://infra.apache.org/release-distribution.html) — confirms `dlcdn.apache.org`/mirrors serve only current releases, while every release is archived permanently on `archive.apache.org`.
* [Apache Kafka 3.9.2 archive directory](https://archive.apache.org/dist/kafka/3.9.2/) — verified live, lists `kafka_2.13-3.9.2.tgz` (117 MB) with checksum files, confirming the download URL used in Section 9.
* [KAFKA-12866 — Kafka requires ZK root access even when using a chroot](https://issues.apache.org/jira/browse/KAFKA-12866) — real-world case informing the explicit-chroot-creation recommendation in Section 11.
