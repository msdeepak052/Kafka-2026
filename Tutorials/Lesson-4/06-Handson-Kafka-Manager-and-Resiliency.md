# Hands-On: Kafka Cluster Management (AKHQ) and Kafka Resiliency

> **Scope:** Course items **34. Hands-On: Kafka Manager (Cluster Management)**
> and **35. Hands-On: Demonstrating Kafka Resiliency.**
>
> **Builds on:** the 3-broker Kafka cluster (`kf1`/`kf2`/`kf3`, Kafka 3.9.2,
> `kafka.service`, security group `kafka-sg`) running against the Lesson-3
> ZooKeeper ensemble (`zk1`/`zk2`/`zk3`, security group `zookeeper-sg`),
> with the dual internal (`9092`)/external (`9094`) listener setup already
> completed in an earlier Lesson-4 hands-on file.

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
            │  KF1  │         │  KF2  │         │  KF3  │
            │ id=1  │         │ id=2  │         │ id=3  │
            │ :9092 │         │ :9092 │         │ :9092 │  internal
            │ :9094 │         │ :9094 │         │ :9094 │  external
            └───┬───┘         └───┬───┘         └───┬───┘
                │                 │                 │
                └────────────┬────┴────────┬────────┘
                             │              │
                        Kafka cluster    ZooKeeper :2181
                             │              │
                        ┌────┴────┐    ┌────┴────┐
                        │  AKHQ   │    │ZK1 ZK2 ZK3│
                        │(Docker  │    │  quorum   │
                        │ on KF1) │    └───────────┘
                        └────┬────┘
                             │ :8080
                             ▼
                       Your Browser
```

Two things happen in this lesson:

```text
34. Kafka Manager (Cluster Management)
       ↓
Stand up a visual management UI (AKHQ) against the real cluster

35. Demonstrating Kafka Resiliency
       ↓
Kill brokers on purpose, watch leader election / ISR / quorum,
tie it back to the RF=3 math from "01 - Kafka Cluster Size"
```

---

# 2. Recap: The Cluster We're Operating On

| Node | Hostname | AZ   | `broker.id` | Internal listener | External listener |
| ---- | -------- | ---- | ----------- | ------------------ | ------------------ |
| KF1  | `kf1`    | AZ-A | 1           | `kf1:9092`          | `kf1:9094` (public IP) |
| KF2  | `kf2`    | AZ-B | 2           | `kf2:9092`          | `kf2:9094` (public IP) |
| KF3  | `kf3`    | AZ-C | 3           | `kf3:9092`          | `kf3:9094` (public IP) |

```text
INTERNAL listener :9092
   ↓
broker-to-broker + anything running inside the VPC (like AKHQ on kf1)

EXTERNAL listener :9094
   ↓
your laptop's producer/consumer CLI, over the public IP
```

Security groups already in place:

```text
kafka-sg
   ├── 22   ← My IP           (SSH)
   ├── 8080 ← My IP           (opened earlier for a web UI — reused below)
   ├── 9092 ← kafka-sg itself (broker-to-broker, internal)
   └── 9094 ← My IP           (external client access)

zookeeper-sg
   ├── 22   ← My IP
   ├── 2181 ← kafka-sg        (Kafka → ZooKeeper client port)
   ├── 2888 ← zookeeper-sg itself
   └── 3888 ← zookeeper-sg itself
```

Nothing new needs to be opened for this file — that's called out explicitly
in Section 8 below.

---

# 3. Course Note — "Kafka Manager" Actually Means CMAK, and CMAK Is Not a Good Choice Today

This section is a deliberate, transparent correction, the same way an
earlier document in this series flagged the Java 8/11/12 vs Java 17
mismatch in the official ZooKeeper docs rather than silently working around
it.

The course calls this lecture **"Kafka Manager (Cluster Management)."**
Historically, **"Kafka Manager"** was a specific project:

```text
Yahoo "Kafka Manager"
        ↓
   renamed/forked
        ↓
   CMAK — Cluster Manager for Apache Kafka
   (github.com/yahoo/CMAK)
```

This is documented directly in Apache's own Kafka ecosystem wiki, which
records the rename from "Kafka Manager" to "CMAK." ([Apache Jira][1])

### What research shows about CMAK's current state

* **Latest release: `3.0.0.6`, published 2022-04-29.** That's the newest
  tagged release on the project's own GitHub Releases page — over four
  years old as of this lab. ([GitHub — CMAK Releases][2])
* **No further releases since**, and the repository's last code push was
  August 2023 (a dependency bump, not a feature release) — checked directly
  against the GitHub repository API at the time of writing.
* **522 open issues** sitting on the repository with no indication of
  active maintainer triage. ([GitHub — yahoo/CMAK][3])
* **Hard ZooKeeper dependency, no KRaft support** — a third-party review
  published in 2026 specifically flags that CMAK cannot manage Kafka 4.0+
  clusters because it has no support for KRaft-mode clusters, only the
  legacy ZooKeeper-based controller. ([Factor House — CMAK review][4])

```text
CMAK status, honestly stated
       │
       ├── Last release: April 2022
       ├── 500+ open issues, unattended
       ├── No KRaft support
       └── Not recommended for a fresh lab in 2026
```

None of this means CMAK is *forbidden* to try — it still runs, and the
cluster in this lab is ZooKeeper-mode, so it's technically compatible. The
point is narrower: **it is not a responsible tool to teach as "the" way to
manage Kafka today**, because a learner who adopts it walks away with
skills tied to an abandoned project.

---

# 4. What This File Uses Instead — AKHQ

```text
AKHQ
   ↓
https://akhq.io
   ↓
Actively maintained, open-source Kafka management UI
```

AKHQ (formerly "KafkaHQ") covers the same practical goal the course lecture
is actually after — **visually managing and inspecting a Kafka cluster**:
browsing brokers, topics, partitions, consumer groups, and messages through
a web UI, without needing a large monitoring stack. Its own documentation
describes exactly this scope, and its GitHub project shows continuous
recent activity, unlike CMAK's multi-year silence. ([AKHQ docs][5])

```text
                Same lecture goal
                       │
        ┌──────────────┴──────────────┐
        │                              │
   Course says:                  This lab uses:
   "Kafka Manager"                    AKHQ
   (→ CMAK, abandoned)          (actively maintained)
```

Everything from here on (Sections 5–19) is the AKHQ hands-on. The
resiliency demo (course item 35) picks up in Section 20.

---

# 5. AKHQ Architecture for This Lab

The simplest place to run AKHQ for a 3-node lab cluster is **on `kf1`
itself** — no new EC2 instance required, and it sits inside the VPC where
it can reach all three brokers on their fast internal listener.

```text
                      kf1 (EC2 instance)
                      ┌───────────────────────────┐
                      │                            │
                      │  kafka.service  :9092/9094 │
                      │                            │
                      │  ┌──────────────────────┐  │
                      │  │  Docker container     │  │
                      │  │  tchiotludo/akhq      │  │
                      │  │  :8080                │  │
                      │  └──────────┬───────────┘  │
                      └─────────────┼──────────────┘
                                    │
                     bootstrap.servers = kf1:9092,kf2:9092,kf3:9092
                                    │
                         (internal listener — AKHQ
                          runs inside the VPC, so it
                          never needs the external one)
```

```text
Your Laptop
     │
     │ HTTP :8080 (public IP of kf1)
     ▼
kf1 → AKHQ container
     │
     │ Kafka protocol :9092 (internal listener)
     ▼
kf1 / kf2 / kf3 brokers
```

---

# 6. Install Docker on `kf1`

SSH into `kf1` (same pattern as the ZooKeeper nodes — public IP, the
`kafka-lab.pem` key):

```bash
ssh -i kafka-lab.pem ubuntu@<kf1-PUBLIC-IP>
```

Ubuntu 24.04 ships Docker in the default repos as `docker.io`, which is the
simplest install path for a lab (the official Docker APT repo is the
production-grade alternative, but adds more setup than this lab needs):

```bash
sudo apt update

sudo apt install -y docker.io
```

Enable and start it:

```bash
sudo systemctl enable --now docker
```

Verify:

```bash
sudo systemctl status docker
```

Expected:

```text
● docker.service - Docker Application Container Engine
     Loaded: loaded (/usr/lib/systemd/system/docker.service; enabled; ...)
     Active: active (running) since ...
```

Confirm the engine itself works:

```bash
sudo docker run --rm hello-world
```

Expected (trimmed):

```text
Hello from Docker!
This message shows that your installation appears to be working correctly.
```

---

# 7. AKHQ Configuration File — `application.yml`

AKHQ's default config path inside its container is `/app/application.yml`,
and the minimal way to point it at a Kafka cluster is the
`akhq.connections.<cluster-name>.properties.bootstrap.servers` key —
this is documented directly on AKHQ's own configuration pages. ([AKHQ —
Installation][5]) ([AKHQ — Cluster configuration][6])

Create a working directory and the config file on `kf1`:

```bash
mkdir -p ~/akhq

nano ~/akhq/application.yml
```

Contents:

```yaml
akhq:
  connections:
    kafka-lab:
      properties:
        bootstrap.servers: "kf1:9092,kf2:9092,kf3:9092"
```

```text
akhq.connections.kafka-lab
       │
       └── properties.bootstrap.servers
                  │
                  └── kf1:9092,kf2:9092,kf3:9092
                              │
                        internal listener,
                        because AKHQ itself
                        lives inside the VPC
```

> **Don't** point this at `kf1:9094,kf2:9094,kf3:9094` (the external
> listener). That listener advertises each broker's **public IP** for
> clients outside the VPC — AKHQ is running *inside* the VPC on `kf1`, so
> the internal listener is both simpler and faster.

---

# 8. Run AKHQ via Docker on `kf1`

The official image is `tchiotludo/akhq`, and the documented run pattern
mounts the config file into the container at its default path and exposes
port 8080. ([AKHQ — Installation][5])

```bash
sudo docker run -d \
  --name akhq \
  --network host \
  -v ~/akhq/application.yml:/app/application.yml \
  tchiotludo/akhq:latest
```

```text
--network host
       ↓
container shares kf1's own network namespace
       ↓
"kf1", "kf2", "kf3" resolve exactly the same way
inside the container as they do in your SSH session
(kf1 already has /etc/hosts entries for all three
 from the broker setup — no extra config needed)
```

> **Why `--network host` instead of `-p 8080:8080`:** with the default
> Docker bridge network, the AKHQ container gets its own isolated network
> stack and does **not** inherit `kf1`'s `/etc/hosts` entries — so it would
> fail to resolve `kf2`/`kf3` by hostname even though `bootstrap.servers`
> looks correct. `--network host` (fully supported on Linux, not just a
> Docker Desktop feature) sidesteps this entirely: the container binds
> directly to `kf1`'s own network interfaces, using the exact same hosts
> file and reaching port 8080 on `kf1` itself with no port mapping needed.

---

# 9. Verify the AKHQ Container Is Running

```bash
sudo docker ps
```

Expected:

```text
CONTAINER ID   IMAGE                    COMMAND       STATUS         PORTS   NAMES
7a2f9c1e4b3d   tchiotludo/akhq:latest   "..."         Up 8 seconds           akhq
```

Logs:

```bash
sudo docker logs akhq
```

Expected (trimmed):

```text
INFO  i.m.r.Micronaut - Startup completed in ... : Server Running: http://kf1:8080
INFO  o.a.k.clients.admin.AdminClientConfig - AdminClientConfig values:
        bootstrap.servers = [kf1:9092, kf2:9092, kf3:9092]
INFO  io.micronaut.context.env.DefaultEnvironment - loaded configuration from application.yml
```

---

# 10. Security Group — Port 8080 Is Already Open

```text
kafka-sg
   └── 8080 ← My IP
```

This rule was already added to `kafka-sg` in an earlier Lesson-4 file for
a different tool — it's reused here as-is. No AWS Console changes are
needed for this section; just confirm the rule is still scoped to **My IP**
(re-check/refresh it the same way the SSH rule is refreshed in the
ZooKeeper doc if your home/mobile IP has changed since then).

---

# 11. Access AKHQ From Your Browser

```text
http://<kf1-PUBLIC-IP>:8080
```

```text
Your Browser
     │
     │ HTTP :8080
     ▼
kf1 (public IP, kafka-sg allows your IP on 8080)
     │
     ▼
AKHQ container (--network host)
     │
     │ :9092 internal listener
     ▼
kf1 / kf2 / kf3
```

If the SG rule and `bootstrap.servers` are both correct, you land on the
AKHQ dashboard with a cluster selector in the top-left showing
**`kafka-lab`** (the connection name from `application.yml`).

---

# 12. AKHQ Walkthrough — Cluster Overview

The landing dashboard for the `kafka-lab` connection shows summary tiles:

```text
┌─────────────┬─────────────┬─────────────┬─────────────┐
│  Brokers: 3 │  Topics: N  │ Partitions  │ Consumer     │
│             │             │ (topic sum) │ Groups: N    │
└─────────────┴─────────────┴─────────────┴─────────────┘
```

This is the fastest "is my cluster healthy" glance an operator gets — 3
brokers should be visible from a first login, matching `kf1`/`kf2`/`kf3`.

---

# 13. AKHQ Walkthrough — Broker List & Configs

Navigate to **Nodes** (sometimes labeled **Brokers**) in the left nav.

Expected table:

```text
ID   Host   Port   Rack   Controller
1    kf1    9092              
2    kf2    9092              
3    kf3    9092    ✓ (controller)
```

* All 3 brokers should be listed, matching `broker.id` 1/2/3 for
  `kf1`/`kf2`/`kf3`.
* Exactly **one** broker shows as the active controller (elected via
  ZooKeeper) — this is a real-time read of cluster state, not a static
  config file.
* Clicking a broker opens its **Configs** tab — a live dump of that
  broker's running configuration (`log.dirs`, `num.network.threads`,
  `log.retention.hours`, `listeners`, and everything else in its
  `server.properties`, resolved to actual effective values).

---

# 14. Create a Topic to Explore — `orders`

This uses the same running example topic from "01 - Kafka Cluster Size"
(`orders`, 6 partitions, RF 3) so the AKHQ walkthrough and the resiliency
demo in the rest of this file share one topic.

From `kf1` (or any broker, using the CLI already installed for Kafka
itself):

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

If you already created and used an `orders` (or similarly-purposed) topic
in an earlier Lesson-4 hands-on file, reuse it instead of creating a new
one — just make sure it has 6 partitions and RF 3, since the resiliency
math in Section 21 depends on that shape.

---

# 15. AKHQ Walkthrough — Topics List & Partition/Replica Visualization

Navigate to **Topics**.

```text
Topic     Partitions   Replication   Size
orders    6            3             ~0 B
```

Click into `orders`. The **Partitions** tab renders each partition as a
row with a small colored bar per replica:

```text
Partition 0   Leader: 1   [■ 1][■ 2][■ 3]   ISR: 1,2,3
Partition 1   Leader: 2   [■ 2][■ 3][■ 1]   ISR: 2,3,1
Partition 2   Leader: 3   [■ 3][■ 1][■ 2]   ISR: 3,1,2
Partition 3   Leader: 1   [■ 1][■ 2][■ 3]   ISR: 1,2,3
Partition 4   Leader: 2   [■ 2][■ 3][■ 1]   ISR: 2,3,1
Partition 5   Leader: 3   [■ 3][■ 1][■ 2]   ISR: 3,1,2
```

* The leader replica is visually distinguished (typically bold/highlighted)
  from the follower replicas in the same row.
* This is the exact same information `kafka-topics.sh --describe` gives
  you on the CLI (Section 19 below) — AKHQ just renders it.

---

# 16. Produce Test Messages

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --topic orders \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092
```

Type a few lines and `Ctrl+D` to exit:

```text
>order-1001 created
>order-1002 created
>order-1003 shipped
```

---

# 17. AKHQ Walkthrough — Browsing Messages

Back in the `orders` topic in AKHQ, open the **Data** tab.

```text
Partition   Offset   Timestamp             Key   Value
4           0        2026-08-22 10:14:02   -     order-1001 created
1           0        2026-08-22 10:14:05   -     order-1002 created
2           0        2026-08-22 10:14:09   -     order-1003 shipped
```

* Messages land on whichever partition the default partitioner picked
  (round-robin/sticky, since no key was supplied) — don't expect them all
  on partition 0.
* Clicking a row expands the full message (headers, key, value, and
  partition/offset metadata) — this is the fastest way to eyeball actual
  payloads without hand-building a console consumer command.

---

# 18. AKHQ Troubleshooting

| Symptom                                   | Check first                                                                 |
| ------------------------------------------ | ---------------------------------------------------------------------------- |
| Browser can't reach `:8080` at all         | `kafka-sg` inbound rule for 8080 — is it still scoped to your **current** IP? |
| AKHQ loads but shows "cluster unreachable" / brokers as down | `bootstrap.servers` in `application.yml` — internal listener (`9092`), correct hostnames |
| AKHQ can't resolve `kf2`/`kf3`             | Did you use `--network host`? Bridge networking won't see `kf1`'s `/etc/hosts` |
| AKHQ shows 0 or 1 broker instead of 3      | Are all 3 `kafka.service` units actually running? `systemctl status kafka` on each node |
| Config changes to `application.yml` not taking effect | Container needs a restart: `sudo docker restart akhq` (it doesn't hot-reload the mounted file) |
| `docker: permission denied`                | Either `sudo docker ...` or add your user to the `docker` group and re-login |

---

# 19. Baseline — Describe `orders` Before Any Failure

This is where course item 35 (resiliency) begins.

```bash
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092
```

Expected:

```text
Topic: orders  TopicId: 3f2504e0-4f89-11d3-9a0c-0305e82c3301  PartitionCount: 6  ReplicationFactor: 3  Configs: min.insync.replicas=1,segment.bytes=1073741824
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: orders	Partition: 1	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: orders	Partition: 3	Leader: 1	Replicas: 1,2,3	Isr: 1,2,3
	Topic: orders	Partition: 4	Leader: 2	Replicas: 2,3,1	Isr: 2,3,1
	Topic: orders	Partition: 5	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
```

Record it in a simple table before moving on:

```text
Broker (id)   Leads partitions
kf1 (1)       P0, P3
kf2 (2)       P1, P4
kf3 (3)       P2, P5
```

Every broker is a **replica** for every partition (RF 3 on a 3-broker
cluster), but leadership is spread round-robin — this matters for
picking which broker to kill first in the next section.

---

# 20. What We're Proving

This entire section is a live demonstration of the math already worked out
in **"01 - Kafka Cluster Size"** — specifically:

```text
Replication Factor 3
        +
3 brokers
        ↓
Can tolerate a broker failure
```

and:

```text
Number of brokers >= Replication Factor
```

Nothing here is a new rule — it's the same RF=3/3-broker reasoning from
that file, now demonstrated for real against a running cluster instead of
worked out on paper.

---

# 21. Failure Test 1 — Stop a Broker That Isn't Leading Everything

Per the baseline table above, `kf2` leads exactly 2 of the 6 partitions
(P1, P4) — not "everything." Stopping it demonstrates a **partial, clean
failover**: the partitions it doesn't lead keep their original leader
completely undisturbed.

On `kf2`:

```bash
sudo systemctl stop kafka
```

Confirm:

```bash
systemctl status kafka
```

Expected:

```text
● kafka.service - Apache Kafka Broker
     Loaded: loaded (/etc/systemd/system/kafka.service; enabled; ...)
     Active: inactive (dead) since ...
```

---

# 22. `--describe` After the Failure

From `kf1` (or `kf3` — any surviving broker):

```bash
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server kf1:9092,kf3:9092
```

Expected:

```text
Topic: orders  TopicId: 3f2504e0-4f89-11d3-9a0c-0305e82c3301  PartitionCount: 6  ReplicationFactor: 3  Configs: min.insync.replicas=1,segment.bytes=1073741824
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,3
	Topic: orders	Partition: 1	Leader: 3	Replicas: 2,3,1	Isr: 3,1
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1
	Topic: orders	Partition: 3	Leader: 1	Replicas: 1,2,3	Isr: 1,3
	Topic: orders	Partition: 4	Leader: 3	Replicas: 2,3,1	Isr: 3,1
	Topic: orders	Partition: 5	Leader: 3	Replicas: 3,1,2	Isr: 3,1
```

What changed:

```text
Every partition
       ↓
broker 2 dropped out of Isr
       (2 was in Replicas for all 6 — RF3 on 3 brokers
        means every broker replicates every partition)

P1, P4 specifically
       ↓
Leader moved 2 → 3
       (kf2 was leading these; kf3 took over from the
        surviving ISR members)

P0, P2, P3, P5
       ↓
Leader unchanged
       (kf2 was never leading these — only ISR shrank)
```

This is the same shape as "01 - Kafka Cluster Size" Section 25
("Under-Replication After Failure"): `orders` is now under-replicated
(2 of 3 replicas in sync) on every partition, but every partition still
has a leader and is fully readable/writable.

---

# 23. Producer/Consumer Keep Working

Produce a few more messages, targeting only the survivors:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --topic orders \
  --bootstrap-server kf1:9092,kf3:9092
```

```text
>order-1004 created
>order-1005 created
```

Both lines are accepted — no errors. Consume them back:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders \
  --from-beginning \
  --bootstrap-server kf1:9092,kf3:9092
```

```text
order-1001 created
order-1002 created
order-1003 shipped
order-1004 created
order-1005 created
```

A client that was already connected using the full bootstrap list
(`kf1:9092,kf2:9092,kf3:9092`) sees at most a brief pause while it
refreshes metadata and reroutes to the new leaders for P1/P4 — it does not
need `kf2` to be listed for the connection itself to keep functioning, since
`bootstrap.servers` is only used for the initial connection, not for every
request.

---

# 24. Watch It Live in AKHQ

Refresh the AKHQ **Nodes** page in your browser:

```text
ID   Host   Port   Rack   Controller   Status
1    kf1    9092                        ● Online
2    kf2    9092                        ○ Unreachable
3    kf3    9092    ✓ (controller)      ● Online
```

* `kf2` shows as down/unreachable (its row typically flags in AKHQ once it
  can't be reached for a metadata/config fetch).
* Reopen the `orders` topic's **Partitions** tab — the leader column for
  P1 and P4 now shows `3` instead of `2`, matching the CLI `--describe`
  output exactly.

---

# 25. Failure Test 2 — Discussion: Losing a Second Node

Don't actually leave the cluster in this state for long, but reason through
two possible "one more failure" scenarios, both of which map directly onto
math already established earlier in this series.

## Scenario A — a second Kafka broker also goes down

```text
kf2 ❌  (already down)
kf3 ❌  (stop this one too)
kf1 ✅  (only survivor)
```

This is **exactly** the RF=3-tolerates-1-failure scenario from "01 - Kafka
Cluster Size" Section 8 and Section 12, now demonstrated for real:

```text
Number of brokers >= Replication Factor
       ↓
3 >= 3   → satisfied with all 3 up
2 >= 3   → NOT satisfied once 2 are down
```

`--describe` afterward would show every partition's ISR collapsed to just
`kf1`:

```text
Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1
Topic: orders	Partition: 1	Leader: 1	Replicas: 2,3,1	Isr: 1
...
```

`kf1` can still serve reads/writes for its own leadership on every
partition (it's now leading all 6), but with `ISR = 1` the cluster has
**zero** remaining redundancy — one more failure loses the partition
entirely. This is precisely why "01" recommends never sizing a cluster to
exactly the failure-tolerance minimum with no headroom.

## Scenario B — a third ZooKeeper node also goes down (instead of a second broker)

This cluster is ZooKeeper-mode (per the Lesson-3 ensemble), so it's worth
separating **"a Kafka broker is down"** from **"ZooKeeper quorum is lost"**
— they degrade the cluster differently:

```text
zk1 ✅  zk2 ❌  zk3 ❌
       ↓
1/3 → NO QUORUM
(same math as Lesson-3's ZooKeeper failure test)
```

* Already-elected partition leaders (like `kf1` above) keep serving
  existing reads/writes — they don't need to re-contact ZooKeeper for
  every request.
* What breaks: anything that needs the ZooKeeper-elected **controller** to
  act — new leader elections, broker registration/deregistration, topic
  creation, partition reassignment, ISR shrink/grow bookkeeping. If `kf1`
  (the sole survivor from Scenario A) failed *while* ZooKeeper had no
  quorum, no new leader could be elected for its partitions at all.
* This lines up with "05 - Handson-Zookeeper-AWS" Section 70: ZooKeeper
  never stores Kafka's actual message data, only coordination/controller
  state — but losing quorum still freezes the cluster's ability to react
  to further failures.

```text
Kafka broker down          ZooKeeper quorum down
      ↓                            ↓
Existing leaders          Existing leaders keep
keep serving               serving, but NO new
their partitions           elections/reassignment
      ↓                    can happen until quorum
Under-replication          returns
```

---

# 26. `min.insync.replicas` Tie-In

Set it on the topic (RF 3, so 2 is the standard "tolerate 1 failure and
still get durability guarantees on write" setting):

```bash
/opt/kafka/bin/kafka-configs.sh \
  --alter \
  --entity-type topics \
  --entity-name orders \
  --add-config min.insync.replicas=2 \
  --bootstrap-server kf1:9092,kf3:9092
```

Expected:

```text
Completed updating config for topic orders.
```

## One broker down (current state: kf2 down, kf1/kf3 up) — works fine

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --topic orders \
  --bootstrap-server kf1:9092,kf3:9092 \
  --producer-property acks=all
```

```text
>order-2001 created
>
```

Succeeds — ISR for the target partition is 2 (e.g. `1,3`), which meets
`min.insync.replicas=2`.

## Two brokers down (also stop kf3) — fails

```bash
sudo systemctl stop kafka   # run on kf3
```

Now only `kf1` is alive. Try producing again:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --topic orders \
  --bootstrap-server kf1:9092 \
  --producer-property acks=all
```

```text
>order-2002 created
[2026-08-22 10:41:17,884] ERROR Error when sending message to topic orders with key: null, value: 16 bytes with error: (org.apache.kafka.clients.producer.internals.ErrorLoggingCallback)
org.apache.kafka.common.errors.NotEnoughReplicasException: The size of the current ISR Set(1) is insufficient to satisfy the min.isr requirement of 2 for partition orders-4
```

* With `acks=all`, the broker checks the current ISR size against
  `min.insync.replicas` **before** accepting the write; if ISR (`1`, just
  `kf1`) is below the configured minimum (`2`), it rejects the write with
  `NotEnoughReplicasException` rather than accepting a write it can't
  durably guarantee.
* This is the concrete, hands-on version of the durability trade-off
  `min.insync.replicas` exists for: it converts "silent reduced
  durability" into a loud, explicit producer error.
* Note the distinction from Scenario A in Section 25 above: without
  `min.insync.replicas=2` set, that same 2-broker-down state would have
  kept accepting writes (with reduced durability) instead of rejecting
  them — `min.insync.replicas` is what turns the RF-math failure into an
  enforced guarantee instead of a silent risk.

---

# 27. Recovery — Restart the Stopped Brokers

Start `kf3` back up first, then `kf2`:

```bash
sudo systemctl start kafka   # on kf3
```

```bash
sudo systemctl start kafka   # on kf2
```

Check each:

```bash
systemctl status kafka
```

Expected:

```text
Active: active (running) since ...
```

---

# 28. Verify ISR Grows Back to 3

```bash
/opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092
```

Expected, once both rejoin and catch up:

```text
Topic: orders  TopicId: 3f2504e0-4f89-11d3-9a0c-0305e82c3301  PartitionCount: 6  ReplicationFactor: 3  Configs: min.insync.replicas=2,segment.bytes=1073741824
	Topic: orders	Partition: 0	Leader: 1	Replicas: 1,2,3	Isr: 1,3,2
	Topic: orders	Partition: 1	Leader: 3	Replicas: 2,3,1	Isr: 3,1,2
	Topic: orders	Partition: 2	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
	Topic: orders	Partition: 3	Leader: 1	Replicas: 1,2,3	Isr: 1,3,2
	Topic: orders	Partition: 4	Leader: 3	Replicas: 2,3,1	Isr: 3,1,2
	Topic: orders	Partition: 5	Leader: 3	Replicas: 3,1,2	Isr: 3,1,2
```

```text
ISR
1 → 2 → 3
(catch-up replication takes a moment — don't expect all 3 instantly)
```

---

# 29. Leadership Does Not Snap Back Automatically

Notice in the output above: `kf2` (broker `2`) is back **in the ISR** for
every partition, but it is **not** leading P1 or P4 again — `kf3` still is.

```text
Broker rejoins
       ↓
ISR grows back
       ↓
Leadership does NOT move back by itself
       (not immediately, at least)
```

* `auto.leader.rebalance.enable` **defaults to `true`** in current Kafka —
  a background thread does periodically check for leader imbalance and
  will move leadership back to each partition's **preferred replica**
  (the first broker listed in `Replicas`) once that broker is back in the
  ISR. ([Conduktor Kafka config reference][7])
* But this check only runs on an interval —
  `leader.imbalance.check.interval.seconds` defaults to 300 seconds (5
  minutes) — and only triggers a rebalance once the imbalance ratio
  crosses `leader.imbalance.per.broker.percentage` (default 10%). So
  immediately after `kf2` rejoins, `--describe` still shows `kf3` leading
  P1/P4 — that's expected, not a bug.
* To force it immediately instead of waiting for the periodic check, run a
  **preferred replica election** by hand:

```bash
/opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server kf1:9092,kf2:9092,kf3:9092 \
  --election-type preferred \
  --all-topic-partitions
```

Expected:

```text
Successfully completed leader election (PREFERRED) for partitions orders-0, orders-1, orders-2, orders-3, orders-4, orders-5
```

Re-run `--describe` and P1/P4 should now show `Leader: 2` again, matching
the original baseline in Section 19.

---

# 30. Resiliency Troubleshooting

| Symptom                                    | Check first                                                         |
| -------------------------------------------- | --------------------------------------------------------------------- |
| Broker won't rejoin after `systemctl start`  | `systemctl status kafka`, then `journalctl -u kafka -n 100 --no-pager` |
| `--describe` still shows old ISR/leader after restart | Give it a few seconds — replica catch-up and controller propagation aren't instant |
| Producer hangs instead of erroring under 2-broker-down | Check `request.timeout.ms`/`delivery.timeout.ms` — it will eventually surface `NotEnoughReplicasException` or a timeout, not hang forever |
| Leadership never moves back even after 5+ minutes | Confirm `auto.leader.rebalance.enable` wasn't explicitly set to `false` in `server.properties` |
| AKHQ still shows a broker as down after recovery | Refresh the page — AKHQ's own broker-list poll interval means it may lag a few seconds behind reality |
| Kafka can't reach ZooKeeper mid-test         | Not a Kafka problem — walk the ZooKeeper troubleshooting flow from "05 - Handson-Zookeeper-AWS" Section 68 |

---

# 31. Full Failure Experiment (Timeline)

```text
BASELINE

kf1 ✅ leads P0,P3
kf2 ✅ leads P1,P4
kf3 ✅ leads P2,P5

RF3 / 3 brokers → healthy, full ISR


STOP kf2

kf1 ✅
kf2 ❌
kf3 ✅

ISR shrinks everywhere (kf2 was a replica for all 6 partitions)
P1,P4 leadership: kf2 → kf3
Producer/consumer: uninterrupted


STOP kf3 too  (Scenario A)

kf1 ✅ (only survivor, now leads all 6)
kf2 ❌
kf3 ❌

ISR = 1 on every partition
acks=all + min.insync.replicas=2 → NotEnoughReplicasException


RESTART kf3, then kf2

ISR: 1 → 2 → 3
Leadership: stays with whoever holds it — NOT auto-restored instantly
kafka-leader-election.sh --election-type preferred → restores original leaders
```

This is why:

```text
RF 1        → no fault tolerance at all
RF 2        → tolerates 1 failure, no durability margin under acks=all + min.insync.replicas=2
RF 3        → tolerates 1 failure cleanly (this lab)
RF 3 + 2 down → below the tolerated threshold — exactly what "01" warned against
```

---

# 32. Final Hands-On Checklist

```text
AKHQ
□ Install Docker on kf1
□ Write application.yml with akhq.connections.<name>.properties.bootstrap.servers
□ Run AKHQ with --network host and understand why
□ Confirm port 8080 was already open on kafka-sg (no new SG rule)
□ Open http://<kf1-PUBLIC-IP>:8080 and see 3 brokers
□ Browse a broker's live Configs tab
□ Create/reuse the "orders" topic (6 partitions, RF 3)
□ See the topic's partition/replica visualization
□ Produce messages via CLI, browse them in AKHQ's Data tab
□ Explain, out loud, why CMAK was not used

Resiliency
□ Record baseline leader/ISR from --describe
□ Stop a broker that isn't leading everything
□ Observe ISR shrink + leader reassignment
□ Confirm producer/consumer keep working
□ See the down broker reflected live in AKHQ
□ Reason through a second-failure scenario (Kafka broker OR ZooKeeper quorum)
□ Set min.insync.replicas=2 and see acks=all succeed with 1 broker down
□ See acks=all fail with NotEnoughReplicasException with 2 brokers down
□ Restart brokers, watch ISR grow back
□ Confirm leadership does NOT snap back immediately
□ Run kafka-leader-election.sh --election-type preferred to restore it
```

---

# 33. Interview Answer

If asked:

> **"How would you demonstrate Kafka's fault tolerance to someone who
> doesn't believe replication actually works?"**

A strong answer:

> "I'd stand up a real 3-broker cluster with RF 3, create a topic with
> multiple partitions, and record which broker leads which partition with
> `--describe`. Then I'd stop one broker via systemd — not kill -9, a clean
> stop — and show that the partitions it led get a new leader from the
> remaining ISR, the ones it didn't lead are untouched, and a producer/
> consumer using the full bootstrap list barely notices. I'd also show that
> in AKHQ or any UI, the down broker shows as unreachable in real time. Then
> I'd push it further: stop a second broker, and show that with RF 3 and 2
> down, ISR collapses to 1, and if `min.insync.replicas=2` is set, `acks=all`
> producers start getting `NotEnoughReplicasException` — which is the
> RF-tolerates-(RF-1)-failures math actually enforced, not just theoretical.
> Finally I'd restart the stopped brokers and point out that leadership
> doesn't automatically move back until either the periodic
> `auto.leader.rebalance.enable` check runs or you trigger a preferred
> replica election by hand."

That ties cluster-sizing theory directly to an operational demonstration —
exactly what a Senior Platform Engineer is expected to be able to do live.

---

[1]: https://issues.apache.org/jira/browse/KAFKA-12720 "Ecosystem wiki page: Kafka Manager renamed CMAK (Cluster Manager for Apache Kafka) - ASF Jira"
[2]: https://github.com/yahoo/CMAK/releases/tag/3.0.0.6 "CMAK 3.0.0.6 release"
[3]: https://github.com/yahoo/CMAK "GitHub - yahoo/CMAK: CMAK is a tool for managing Apache Kafka clusters"
[4]: https://factorhouse.io/articles/cmak/ "CMAK: Review, pricing, and best alternatives in 2026 | Factor House"
[5]: https://akhq.io/docs/installation.html "Installation | AKHQ"
[6]: https://akhq.io/docs/configuration/brokers.html "Cluster configuration | AKHQ"
[7]: https://kafka-options-explorer.conduktor.io/config/auto-leader-rebalance-enable/ "auto.leader.rebalance.enable — Kafka Broker Configuration | Conduktor"
[8]: https://www.conduktor.io/kafka/kafka-topic-configuration-min-insync-replicas "Kafka min.insync.replicas Explained | Conduktor"
