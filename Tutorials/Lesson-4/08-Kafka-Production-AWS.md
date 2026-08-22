# Kafka in Production on AWS

For a **Senior Platform Engineer**, don't think of "running Kafka in production on AWS" as:

> "The lab, but bigger."

Think of it as:

> **A different set of design decisions on almost every axis — networking, compute, storage, security, monitoring, and DR — where the lab's shortcuts were reasonable for learning and would be liabilities in production.**

This file is purely conceptual, like file `01`. No new hands-on build happens here — it's the "what would actually be different" reference you hold up against everything built so far in this series.

---

# 1. What "Production on AWS" Actually Means

- It does **not** mean:
  - A bigger instance type.
  - More brokers.
  - "Turn on some AWS best-practice checkbox."
- It means re-deciding, deliberately, on every axis this lab quietly simplified:

```text
Learning Lab Decision              Production Decision
        │                                  │
   "make it work,                   "make it survive
    make it visible,                 failure, be secure,
    make it cheap"                   be operable, cost
                                      what it should"
```

- Every simplification in this lab was a **conscious trade-off for learning speed**, not a mistake. The goal of this file is to name each trade-off explicitly and say what replaces it in production.

---

# 2. Recap — What This Lab Actually Built

- Before contrasting, be precise about what "this lab" means, because vague contrasts are worthless.
- Across this series so far, the lab has consistently used:
  - **Compute:** `t3.small` EC2 instances — a *burstable* instance family (CPU credits, not sustained performance).
  - **Networking:** the **default VPC**, with instances in **public subnets**.
  - **Node identity:** short, memorable hostnames (`zk1`/`zk2`/`zk3` for ZooKeeper, and the equivalent `kf1`/`kf2`/`kf3` pattern for Kafka brokers) resolved via **hand-edited `/etc/hosts`** on every node, because the default VPC gives every instance an ugly `ip-10-x-x-x.ec2.internal` DNS name, not a short one.
  - **AZ placement:** effectively **one node's worth of failure domain per node**, placed wherever the console defaulted to, not deliberately spread and rack-labeled — sufficient to demonstrate quorum/replication mechanics, not to model a real AZ outage at scale.
  - **Client reachability:** brokers get a **public IP**, and `advertised.listeners` is pointed at that public IP so a producer/consumer running on your laptop can reach the cluster directly over the internet.
  - **Storage:** a single `gp3` root volume per node (e.g. 20 GiB), sized for "the lab works," not for a real retention/throughput budget.
  - **Coordination:** **ZooKeeper 3.9.5** — this cluster has not moved to KRaft.
  - **Security:** **PLAINTEXT** everywhere. No TLS, no SASL, no ACLs. Anything that can open a TCP connection to the broker port can produce and consume anything.
  - **Observability:** `kafka-topics.sh --describe`, `zkCli.sh`, and reading terminal output by eye.

```text
This Lab (as built so far)

Default VPC
  └── Public subnet (single AZ, mostly)
        ├── zk1 / kf1   t3.small   public IP   /etc/hosts
        ├── zk2 / kf2   t3.small   public IP   /etc/hosts
        └── zk3 / kf3   t3.small   public IP   /etc/hosts

PLAINTEXT everywhere · ZooKeeper 3.9.5 · gp3 root volume, unsized
```

- None of this is "wrong." It is **correctly scoped for a disposable learning environment** — cheap, fast to build, easy to SSH into, easy to reason about with three terminals side by side.
- The point of this file is: **do not carry any of it into production unexamined.**

---

# 3. The Production Delta — the Spine of This File

- Every remaining section in this file expands one row of this table.

```text
Dimension        Lab                          Production
─────────────────────────────────────────────────────────────────────────
Networking        Default VPC, public subnet   Private subnets, no public IP
Reachability       Direct public IP              Bastion / SSM / VPN / Direct Connect
DNS                /etc/hosts                    Private Route 53 hosted zone
Compute            t3.small (burstable)          Sustained-performance family (e.g. m7g)
Storage            Unsized gp3 root volume       gp3 with sized/provisioned IOPS+throughput
AZ placement       Ad hoc, mostly one AZ         Deliberate multi-AZ + broker.rack
Security           PLAINTEXT, no auth            TLS + SASL + ACLs
Monitoring         --describe by eye             JMX → Prometheus/Grafana or CloudWatch
DR                 None                          Cross-region replication (MirrorMaker 2)
Ops model          You run everything by hand    Self-managed EC2, or Amazon MSK
```

- Read this table now, then treat every section below as "click into this row."

---

# 4. Networking, Part 1 — Private Subnets Instead of the Default VPC

- The lab's default VPC is a shared, pre-existing, loosely-controlled network that every new AWS account gets for free — it is designed for "get something running in five minutes," not for hosting a system that stores payment events or PII.
- Production networking for Kafka should look like:
  - A **dedicated VPC** (not the default one) with an explicit CIDR block you control.
  - **Private subnets** for the brokers — no route to an Internet Gateway, no public IP assigned.
  - Public subnets, if they exist at all in this VPC, hold only edge resources (a NAT gateway, a bastion, a load balancer) — never a broker.
- Why this matters concretely:
  - A broker in a private subnet **cannot be reached directly from the public internet**, full stop — not "is protected by a security group," but "has no routable path in."
  - Security groups are still your enforcement layer for *east-west* traffic (broker-to-broker, app-to-broker within the VPC) — private subnets remove the *north-south* exposure entirely, they don't replace security groups.

```text
Learning Lab                         Production
─────────────────                    ──────────────────────────
Default VPC                          Dedicated VPC
Public subnet                        Private subnets (brokers)
Broker has public IP                 Broker has no public IP
Reachable from the internet          Reachable only from inside the VPC
                                      (or a peered/VPN'd network)
```

---

# 5. Networking, Part 2 — Reaching Brokers Without Public IPs

- If brokers have no public IP, "how do I even SSH in, or point a producer at the cluster" is the immediate next question. Production answers it a few ways, layered:
  - **AWS Systems Manager (SSM) Session Manager** — the modern default for *administrative* access. Gives you a shell on the instance with **no open inbound SSH port at all**, no bastion host to patch, and every session is logged in CloudTrail. This is generally preferred over a classic bastion host where it's available.
  - **A bastion / jump host** — still common, especially in older environments or where SSM isn't set up: a small instance in a public subnet, locked down to a narrow IP range, that you SSH into first and then hop from into the private subnet.
  - **AWS Client VPN** — lets engineers connect their laptop directly into the VPC over an encrypted tunnel, so private-subnet resources look like they're on the local network. Good for a distributed team that needs regular access.
  - **Site-to-Site VPN or AWS Direct Connect** — for connecting an entire corporate network (office, data center) into the VPC persistently, rather than per-engineer. Direct Connect is the private, dedicated-line option when VPN's public-internet path isn't acceptable (latency, throughput, or compliance reasons).
- The **client-application** side of this matters just as much as the human-admin side:
  - Producers/consumers running as **applications inside AWS** (other EC2 instances, ECS/EKS workloads, Lambda in a VPC) reach the brokers over private networking directly — no special tooling needed, they're just another resource inside (or peered with) the same VPC.
  - Producers/consumers running **outside AWS** (your laptop, an on-prem service) need one of the VPN/Direct Connect paths above — there is no equivalent of the lab's "point `advertised.listeners` at a public IP and connect from anywhere" shortcut.

```text
                    Production Access Paths

  Ops engineer's laptop ──▶ AWS Client VPN ──▶ private subnet ──▶ broker
                        or
  Ops engineer's laptop ──▶ Bastion (public subnet) ──▶ broker
                        or
  Ops engineer's laptop ──▶ SSM Session Manager ──▶ broker (no bastion)

  App in the VPC (EC2/ECS/EKS) ──▶ broker            (direct, private)
  On-prem service ──▶ Direct Connect / Site-to-Site VPN ──▶ broker
```

- **This directly replaces the lab's public-IP `advertised.listeners` hack.** Pointing `advertised.listeners` at a broker's public IP so a laptop can reach it over the open internet is a deliberate, explicitly-labeled **learning-lab-only** convenience — it trades security for the ability to run a producer from your laptop without setting up a VPN on day one. In production, `advertised.listeners` points at a **private** DNS name or IP, and reachability is solved by the network paths above, not by exposing the broker.

---

# 6. Networking, Part 3 — DNS via Private Route 53 Instead of `/etc/hosts`

- This lab's earlier ZooKeeper build already flagged this as a forward-pointer:
  - `/etc/hosts` works for a 3-node disposable lab because you edit it once, by hand, and never replace an instance.
  - It does **not** survive instance replacement: AWS assigns a **new private IP** to a replacement instance, and nothing updates `/etc/hosts` automatically. If a broker dies and you launch a replacement, every other node in the cluster (and every client) still has the *old* IP baked into a static file.
- Production replaces this with a **private Route 53 hosted zone**:
  - Create records like `kf1.kafka.internal`, `kf2.kafka.internal`, `kf3.kafka.internal` pointing at each broker's private IP.
  - The hosted zone is attached to the VPC, so only resources inside that VPC (or peered/VPN'd into it) can resolve those names — it's not public DNS.
  - When a broker is replaced, you update **one DNS record** (or automate that update as part of instance replacement), and every other node and client picks up the change on their next lookup — no fleet-wide file edits.
  - `advertised.listeners` in production points at the **private DNS name**, not a raw IP and not a public IP — this is what makes broker replacement transparent to clients.

```text
Lab                                    Production
──────────────────────                 ─────────────────────────────
/etc/hosts on every node                Private Route 53 hosted zone
Manually edited, once                   kf1.kafka.internal → 10.0.1.11
Breaks on instance replacement          Updates independently of the node
```

---

# 7. Compute — Why `t3.small` Doesn't Belong in Production

- `t3` is a **burstable** instance family: it earns CPU credits while idle and spends them under load. That model is a fine trade for a lab where brokers sit mostly idle between demo commands.
- It is a poor fit for a production Kafka broker because:
  - Kafka brokers are frequently **not** idle — replication, compaction, and steady producer/consumer traffic can keep CPU utilization elevated for sustained periods.
  - Once a `t3` instance exhausts its CPU credit balance, it's throttled to a low **baseline** performance level — exactly when you can least afford it (under sustained load).
  - AWS's own guidance for right-sizing Kafka clusters explicitly calls out that CPU-credit monitoring ("CPUCreditBalance") is "only relevant for brokers of the T3 family" — a strong signal that T3 is treated as the *exception* to watch for, not the production default. ([AWS Big Data Blog — right-sizing Apache Kafka clusters][1])
  - Amazon MSK itself only offers `T3` for its smallest broker tier and steers Standard/Express production clusters toward `M5`/`M7g`. ([Amazon MSK broker instance types][2])
- The fix is not "a bigger `t3`" — it's a **sustained-performance** family, sized by actual benchmarked throughput per broker, the same way file `01` sizes brokers by MB/sec rather than by gut feeling.

---

# 8. Compute — Choosing a Current Instance Family

- As of this writing (August 2026), AWS's own current guidance for sizing Apache Kafka clusters on EC2 centers on the **`m7g` family** — general-purpose, Graviton3-based, EBS-optimized instances (`m7g.large` through `m7g.16xlarge`) — as the reference family used in their own broker-sizing benchmarks. ([AWS Big Data Blog — right-sizing Apache Kafka clusters][1])
- This lines up with what Amazon MSK itself runs on: MSK's Standard brokers support `T3`, `M5`, and the Graviton3-based `M7g` family, and MSK's newer **Express** broker type (higher elasticity/throughput, less manual tuning) supports **only `M7g`** — AWS has moved its own managed offering toward Graviton for new capacity. ([Amazon MSK broker instance types][2])
- Concrete, currently-published benchmark numbers worth anchoring on (a 3-broker cluster, replication factor 3, two consumer groups, EBS-backed storage):
  - `m7g.large` — roughly **63 MB/sec** sustained safe throughput per broker.
  - `m7g.2xlarge` and `m7g.4xlarge` — roughly **200 MB/sec** sustained safe throughput per broker (the `4xlarge` doesn't beat the `2xlarge` here without also provisioning more EBS throughput — CPU/memory stop being the bottleneck and disk throughput becomes the ceiling). ([AWS Big Data Blog — right-sizing Apache Kafka clusters][1])
- Why this matters for sizing (ties directly into file `01`'s formula): **"safe throughput per broker" is not a constant** — it's a property of the instance family/size you pick, and you should benchmark it for your own workload rather than trust a number from someone else's benchmark, even a recent one. Use published numbers as a *starting estimate*, not a final answer.
- `M5` (Intel/AMD, non-Graviton) remains a valid, currently-supported alternative where Graviton (ARM) compatibility of your tooling/agents is a concern — it's still offered by MSK and still commonly used for self-managed clusters, just no longer AWS's lead recommendation.
- What does **not** change from a sizing-methodology standpoint: you still apply file `01`'s reasoning — throughput requirement, plus failure headroom, plus growth — you're just plugging in a sustained-performance family's benchmarked numbers instead of a burstable family's.

```text
Compute Decision
        │
        ├── Lab:        t3.small   (burstable, cheap, fine for demos)
        └── Production: m7g.<size> (sustained, EBS-optimized, benchmarked)
                         or m5.<size> where Graviton isn't an option
```

---

# 9. Storage — `gp3` Baseline vs Provisioned IOPS/Throughput

- This lab has already used `gp3` (the ZooKeeper nodes' 20 GiB root volumes) — the volume *type* doesn't change for production. What changes is **treating its performance as a dial you set deliberately, not a default you ignore.**
- Current `gp3` specifications (verified against AWS's EBS documentation):
  - **Baseline IOPS:** 3,000 IOPS, included in the price of the volume, regardless of size. ([Amazon EBS — General Purpose SSD volumes][3])
  - **Baseline throughput:** 125 MiB/s, also included by default. ([Amazon EBS — General Purpose SSD volumes][3])
  - **Provisioned IOPS:** you can provision up to **80,000 IOPS** for an extra cost, at a ratio of 500 IOPS per GiB of volume size — the maximum (80,000) requires a volume of at least **160 GiB**. ([Amazon EBS — General Purpose SSD volumes][3])
  - **Provisioned throughput:** you can provision up to **2,000 MiB/s**, at a ratio of 0.25 MiB/s per provisioned IOPS — the maximum (2,000 MiB/s) requires at least **8,000 IOPS provisioned** and a volume of at least **16 GiB**. ([Amazon EBS — General Purpose SSD volumes][3])
  - **Max volume size:** 64 TiB (a recent increase — this cap used to be 16 TiB). ([Amazon EBS — General Purpose SSD volumes][3])
  - `gp3` does **not** burst — whatever you provision (baseline or above), you get *consistently*, not "until credits run out" like `gp2`. ([Amazon EBS — General Purpose SSD volumes][3])
- Why the lab never had to think about this: a 20 GiB ZooKeeper volume doing occasional small transaction-log writes never gets close to 3,000 IOPS / 125 MiB/s. A production Kafka broker doing sustained sequential writes at 100+ MB/sec, plus replication reads/writes, plus consumer fetches, absolutely can.
- The production mental model: **size the volume for capacity (retention × ingress × RF), then separately provision IOPS/throughput for the sustained read+write load that volume will actually see** — the two are independently dialed, which is the entire point of `gp3` over `gp2`.
- AWS's own Kafka guidance names `gp3`, `io2`, and `st1` as the recommended volume types for self-managed brokers, and specifically calls out `gp3`'s advantage over instance storage: **the volume's lifecycle is independent of the broker** — if a broker instance fails and is replaced, the same `gp3` volume can be reattached to the replacement instead of losing the data. ([AWS Big Data Blog — right-sizing Apache Kafka clusters][1])

---

# 10. Storage — EBS vs Instance Store (NVMe)

- The lab never had to make this choice because a 20 GiB root `gp3` volume was always the obvious answer. Production forces the choice explicitly.

```text
                    EBS (gp3)                  Instance store (NVMe)
────────────────────────────────────────────────────────────────────
Durability          Independent of instance     Tied to the instance
Broker replacement  Reattach the same volume    Data is gone — must
                     to the new instance          rebuild via replication
Latency/throughput  Very good, network-backed    Lower latency, higher
                                                   raw throughput (local disk)
Resize in place      Yes                          No — fixed at launch
Cost model            Pay for size + IOPS +        Bundled into instance
                       throughput separately        price
```

- **Instance store's failure mode is the one to internalize:** if the EC2 instance underneath a broker is replaced (hardware failure, AZ maintenance, a `stop`/`start` on certain instance types), the local NVMe storage — and every byte of Kafka log data on it — is gone. Kafka's replication is what saves you: the partition's other replicas on other brokers still have the data, and the cluster re-replicates onto the new broker. That re-replication itself costs network and disk bandwidth, and until it completes those partitions are **under-replicated** (the same state file `01`, section 25, described happening after any broker failure — it's just guaranteed to happen on *every* instance-store broker replacement, not just some failures).
- EBS-backed brokers avoid that entirely for a simple hardware swap: the *instance* can be replaced while the *volume* (and its data) survives, reattached to the new instance — no re-replication needed for that specific failure mode.
- AWS's own current Kafka guidance leads with EBS (`gp3`/`io2`/`st1`) for exactly this reason, and treats instance store as the alternative you reach for only when raw local-disk throughput/latency is the dominant requirement and you've explicitly accepted the replacement-data-loss trade-off. ([AWS Big Data Blog — right-sizing Apache Kafka clusters][1])
- Default answer for most production Kafka clusters: **EBS `gp3`, sized and provisioned deliberately.** Reach for instance store only when you've benchmarked a real gap `gp3` (even with provisioned IOPS/throughput) can't close.

---

# 11. Storage — Worked Sizing Example

- This reuses file `01`'s storage formula (`retention × ingress × replication factor`, plus headroom) but carries it one step further: from "how much total storage" to "how do I provision the `gp3` volume(s) that hold it."

```text
Cluster:
  Average ingress = 100 MB/sec
  Retention        = 3 days
  RF               = 3
  Brokers          = 5
```

- **Step 1 — total replicated bytes:**

```text
100 MB/sec × 86,400 sec/day = 8,640 GB/day

8,640 GB/day × 3 days = 25,920 GB raw

25,920 GB × RF 3 = 77,760 GB total replicated bytes (cluster-wide)
```

- **Step 2 — per broker, assuming even partition distribution:**

```text
77,760 GB / 5 brokers = 15,552 GB per broker
```

- **Step 3 — headroom.** Don't provision to 100% capacity (file `01`, section 26). Targeting ~70% steady-state utilization:

```text
15,552 GB / 0.7 ≈ 22,217 GB per broker  →  provision ~23 TiB per broker
```

- **Step 4 — provision throughput/IOPS, not just capacity.** Each broker is doing disk writes for its own leader partitions **and** for the follower replicas it hosts, plus disk reads to serve replication fetches and consumers. A rough sustained-write estimate: with RF 3 spread across 5 brokers, total cluster-wide disk-write bytes/sec ≈ ingress × RF = 100 × 3 = 300 MB/sec, so per broker ≈ 60 MB/sec average sustained write, before counting reads.
  - `gp3` baseline (125 MiB/s) already covers that average comfortably — but you still provision **above** the peak (not the average), and you still add margin for replication catch-up traffic after a broker failure, which is bursty and can be much higher than steady-state.
  - This is a **benchmark-and-provision** decision, not a pure-formula one: watch real `VolumeReadBytes`/`VolumeWriteBytes`/`VolumeQueueLength` CloudWatch metrics on a representative broker, then provision `gp3` IOPS/throughput with headroom above the observed peak — using the documented ratios (500 IOPS/GiB, 0.25 MiB/s per provisioned IOPS) to know what a given provisioned level actually costs you in volume size. ([Amazon EBS — General Purpose SSD volumes][3])
- **Step 5 — sanity-check against the volume size cap.** `gp3` tops out at 64 TiB per volume ([Amazon EBS — General Purpose SSD volumes][3]) — the ~23 TiB/broker figure above fits comfortably in a single volume, but it's worth checking this every time: if per-broker storage math ever exceeds ~64 TiB, the fix is either more brokers (which also helps throughput and failure headroom) or multiple `gp3` volumes per broker across separate `log.dirs` (Kafka supports more than one log directory per broker) — not one oversized volume.

---

# 12. Multi-AZ Placement Done Properly

- File `01` (section 13) already made the conceptual point: don't put every broker in one AZ, because an AZ failure then takes out the whole cluster. This lab's actual build has mostly been single-AZ-per-node-set, which was fine for demonstrating quorum/replication mechanics on 3 nodes, but doesn't model a real AZ outage.
- Production makes this concrete with real subnet/AZ structure:

```text
                         AWS Region (e.g. us-east-1)

        ┌───────────────────┬───────────────────┬───────────────────┐
        │                   │                   │                   │
      AZ-A                AZ-B                AZ-C
        │                   │                   │
  Private subnet       Private subnet       Private subnet
  10.0.1.0/24           10.0.2.0/24          10.0.3.0/24
        │                   │                   │
    kf1, kf4            kf2, kf5            kf3, kf6
   broker.rack=az-a    broker.rack=az-b    broker.rack=az-c
```

- Each AZ gets its **own subnet**, its own CIDR block, and hosts a deliberate share of the brokers — not "whichever AZ the console defaulted to."
- `broker.rack` (file `01`, section 14) is set per broker to its AZ, so Kafka's replica placement actively spreads replicas of the same partition across AZs rather than clustering them in one.
- Amazon MSK **enforces** this for you if you go the managed route: it requires brokers to be spread across either 2 or 3 subnets in different AZs (2 in `us-west-1` specifically; 2 or 3 elsewhere for Standard brokers; **3 AZs mandatory** for Express brokers), and balances broker placement evenly across whatever subnets you give it. ([AWS docs — Create an MSK Provisioned cluster][4], [AWS Big Data Blog — right-sizing Apache Kafka clusters][1])
- If self-managing on EC2, the same discipline has to be applied by hand: launch brokers into specific subnets per AZ, and don't let the count drift lopsided (e.g. 5 brokers as "3 in AZ-A, 1 in AZ-B, 1 in AZ-C" defeats the purpose — losing AZ-A still takes out 3/5 of the cluster).

---

# 13. Security — The PLAINTEXT Gap

- This is the single biggest gap between this lab and any real production deployment, and it's worth naming plainly rather than glossing over: **this lab runs PLAINTEXT everywhere, with no authentication and no authorization.** Any process that can open a TCP connection to a broker's port can produce to, or consume from, any topic.
- Production requires, at minimum:
  - **TLS** for encryption in transit — both client-to-broker and broker-to-broker (inter-broker replication traffic is *not* automatically encrypted just because client traffic is; it's configured separately).
  - **SASL** (or mutual TLS) for **authentication** — proving *who* is connecting (a specific application/service identity), not just encrypting the pipe.
  - **ACLs** for **authorization** — once you know *who* is connecting, deciding *what* they're allowed to do (produce to this topic, consume from that group, administer this cluster).
- This file deliberately does **not** walk through implementing TLS/SASL/ACLs — keystore/truststore setup, choosing a SASL mechanism, and designing an ACL model each have enough surface area to deserve their own dedicated lesson, and cramming them in here would do the topic a disservice.
- What matters for this file: **flag it as non-negotiable, not optional.** A production Kafka cluster carrying anything sensitive (payment events, PII, internal business data) with PLAINTEXT and no ACLs is not "production with a gap" — it's a data breach that hasn't happened yet.

---

# 14. Monitoring — From `--describe` to a Real Stack

- The lab's approach to observability — running `kafka-topics.sh --describe`, `zkCli.sh`, and reading terminal output — is genuinely fine for a lab: you're at the keyboard, the cluster is small, and you're specifically trying to *see* the mechanics (a partition going under-replicated, a leader election happening).
- It does not scale to production, where:
  - Nobody is sitting at a terminal running `--describe` in a loop.
  - You need to know about a problem (under-replicated partitions, ISR shrinking, disk filling up, request latency spiking) **before** it becomes an outage, not after a customer reports one.
  - You need history — "was this broker's CPU already trending up over the last week" — not just a point-in-time snapshot.
- Production monitoring for Kafka is built on:
  - **JMX metrics** — Kafka brokers expose a large set of metrics over JMX (request rates, latencies, under-replicated partition counts, ISR shrink/expand rates, disk usage, and more) — this is the actual data source underneath any dashboard or alert.
  - **A scrape/export layer** — commonly a JMX Exporter sidecar (or agent) that turns JMX metrics into a Prometheus-scrapeable endpoint.
  - **Prometheus + Grafana** — the common self-managed stack: Prometheus scrapes and stores the metrics, Grafana dashboards and alerts on them.
  - **CloudWatch** — the AWS-native alternative (and what Amazon MSK integrates with directly, alongside optional Prometheus-compatible metrics export for MSK clusters too) — useful if the rest of your monitoring/alerting already lives in CloudWatch.
- The point isn't "which tool" — it's the shift from **pull, manual, point-in-time** (`--describe`, eyeballed) to **push, automated, continuous, alertable** (JMX → time-series store → dashboards + alerts).

---

# 15. Backup/DR — Cross-Region Replication

- This lab has no disaster-recovery story at all — it's a single cluster in a single region, and that's appropriate for learning.
- Production needs an answer to: **"what happens if this entire AWS region has a problem?"** Multi-AZ (section 12) protects against an *AZ* failure; it does not protect against a *region*-level event.
- At a conceptual level (this file does not implement it — that's its own topic):
  - **MirrorMaker 2** (or an equivalent replication tool) continuously replicates topics from a cluster in one region to a cluster in another region.
  - This can be set up **active-passive** (one region serves production traffic, the other is a warm/cold standby you fail over to) or **active-active** (both regions serve traffic, with the added complexity of handling data that originates in either region).
  - Cross-region replication is not free or instantaneous — there's replication lag to account for, and a failover plan has to define an acceptable data-loss window (RPO) and time-to-recover (RTO), not just "we have a second cluster somewhere."
- For this file, the goal is just to name this as a required piece of a real production design — not to design it.

---

# 16. The Managed Alternative: Amazon MSK

- Everything above describes running Kafka **yourself** on EC2 — you provision the instances, you install Kafka, you patch the OS, you replace failed brokers.
- **Amazon MSK (Managed Streaming for Apache Kafka)** is AWS's managed alternative: you describe the cluster you want (broker type, count, storage, VPC/subnets, Kafka version), and AWS operates the underlying infrastructure.
- Important framing before the tradeoff table: MSK is **managed infrastructure**, not a **managed data platform**. It removes a real and specific set of operational burdens — it does not remove your design responsibilities.

---

# 17. What MSK Actually Manages vs What You Still Own

```text
MSK Manages For You                    You Still Decide/Own
──────────────────────────────────    ──────────────────────────────────
Broker provisioning                    Broker instance type & count
OS patching on broker hosts            Number of partitions per topic
Broker replacement on failure          Replication factor per topic
Multi-AZ subnet balancing (enforced)   Which topics exist, retention per topic
Kafka software installation            TLS/SASL/ACL configuration & design
Some infra-level monitoring hooks      Application-level monitoring/alerting
                                        Producer/consumer application design
                                        Capacity planning (you still size it)
```

- Concretely, on MSK you still choose:
  - **Broker instance type and count** — MSK's Standard brokers support `T3` (small/dev), `M5`, and `M7g`; Express brokers support `M7g` only. ([Amazon MSK broker instance types][2])
  - **Storage** — per-broker EBS volume size and (for provisioned clusters) throughput, following the same sizing discipline as section 11 above.
  - **Subnets/AZs** — you supply 2–3 subnets across different AZs; MSK enforces even distribution across them, it doesn't design your VPC for you. ([AWS docs — Create an MSK Provisioned cluster][4])
  - **Security configuration** — TLS, SASL mechanism, and ACLs are still something *you* configure on the cluster; MSK gives you the knobs, not the decisions.
  - **Topic/partition design, monitoring dashboards, and DR strategy** — all still yours.
- Where MSK genuinely removes work: you're not SSHing into a broker to apply an OS security patch, and you're not manually detecting and replacing a dead broker at 2 a.m. — that automation is real and valuable.
- One current, specific fact worth knowing before choosing MSK: **MSK trails upstream Apache Kafka releases.** As of this writing, MSK's actively supported versions run up to **4.2.x**, while the latest upstream Apache Kafka release is **4.3.1** — MSK is roughly one release train behind. Also relevant to this lab specifically: Apache Kafka **4.0 removed ZooKeeper mode entirely** (KRaft-only) upstream, and MSK's own 4.0+ clusters no longer support ZooKeeper either. ([Amazon MSK | endoflife.date][5], [Apache Kafka blog — releases][6])
- A related operational fact: if a cluster is left running a version past its MSK end-of-support date, AWS **auto-upgrades it with no advance notice** — version lifecycle on MSK is not something you can indefinitely ignore. ([Amazon MSK | endoflife.date][5])

---

# 18. Self-Managed EC2 vs MSK — Honest Tradeoff

```text
Dimension            Self-Managed on EC2                 Amazon MSK
──────────────────────────────────────────────────────────────────────────────
Broker provisioning   You launch/configure every node     Console/API, AWS provisions
OS patching            Your responsibility                 AWS's responsibility
Broker replacement     You detect + rebuild                Automated
Version currency       You choose exactly, any version     Bounded by MSK's supported
                        (incl. very latest OSS release)      list — currently ~1 release
                                                              train behind upstream
Control/flexibility    Full — any config, any plugin,      Bounded by what MSK exposes;
                        any OS-level tuning                  less low-level control
Multi-AZ                You build and enforce it            Enforced by the service
Inter-broker/replication  You pay standard cross-AZ data     MSK does not charge for
  traffic cost              transfer rates                    inter-broker replication
                                                                traffic within the region
Operational burden      Highest — you own patching,         Lowest of the two — AWS
                          upgrades, failure response           owns the infra layer
Cost model               EC2 + EBS, priced like any          Broker-hour + storage,
                          other compute you run                generally at a premium
                                                                over raw EC2 for the
                                                                same instance class
Security/topic design    Entirely on you either way          Entirely on you either way
```

- On the pricing note specifically: AWS does not charge for MSK's inter-broker (replication) traffic within the same region, which is a real, cited advantage over self-managed EC2 — where that same cross-AZ replication traffic is billed at standard AWS data-transfer rates and can become a meaningful share of the bill at scale. ([AWS MSK pricing discussion][7]) Broker-hour pricing itself, however, generally carries a premium over renting the equivalent raw EC2 instance yourself — you're paying for the management layer.
- **Honest read:** MSK is usually the right default for a team that doesn't already have deep, dedicated Kafka operational expertise, or that would rather spend that engineering time elsewhere. Self-managed EC2 remains the right call when you need OS-level control, a Kafka/KRaft version MSK hasn't caught up to yet, non-standard plugins, or you're operating at a scale where the broker-hour premium meaningfully outweighs the operational savings.

---

# 19. Full Target Production Architecture

```text
                              AWS Region
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
                  AZ-A            AZ-B            AZ-C
                    │              │              │
             Private subnet  Private subnet  Private subnet
                    │              │              │
               kf1, kf4        kf2, kf5        kf3, kf6
             (m7g.2xlarge,   (m7g.2xlarge,   (m7g.2xlarge,
              gp3, no          gp3, no          gp3, no
              public IP)       public IP)       public IP)
                    │              │              │
                    └──────────────┼──────────────┘
                                   │
                    Private Route 53 hosted zone
                    kf1.kafka.internal ... kf6.kafka.internal
                                   │
                    TLS + SASL + ACLs on every listener
                                   │
                 ┌─────────────────┼─────────────────┐
                 │                                    │
        AWS Client VPN /                     Apps inside the VPC
        Bastion / SSM                        (direct, private access)
                 │
        Ops engineers, external
        producers/consumers

        JMX ──▶ Prometheus/Grafana or CloudWatch  (continuous monitoring)
        MirrorMaker 2 ──▶ standby cluster, second region  (DR, conceptual)
```

- Every element in this diagram is one of the sections above. Nothing here is new information — it's the lab's diagram (section 2) with every simplification replaced.

---

# 20. Case Study — FinPay: Sizing a Production Payments Cluster

- **Scenario:** FinPay, a mid-size fintech company, needs a Kafka cluster to carry payment events — authorizations, captures, refunds, ledger updates.
- **Requirements:**
  - Average ingress: **300 MB/sec**.
  - Retention: **7 days** (payment data needs an audit trail, not just a short buffer).
  - Must **survive a full AZ failure**, not just a single broker failure.
  - Strict compliance requirements (data residency, encryption, access control — the TLS/SASL/ACL work from section 13 is mandatory here, not optional).
- This section reasons through the design decisions out loud, the same way file `01` walks through its e-commerce sizing example.

---

# 21. Case Study — Throughput and Broker Count

- Assume peak ingress is 2× average, matching file `01`'s convention:

```text
Average = 300 MB/sec
Peak    = 600 MB/sec
```

- Choose `m7g.2xlarge` as the candidate broker size — using AWS's own currently-published benchmark of ~200 MB/sec sustained safe throughput per broker for that size. ([AWS Big Data Blog — right-sizing Apache Kafka clusters][1])

```text
600 MB/sec (peak) / 200 MB/sec (per broker) = 3 brokers
```

- Same as file `01`'s reasoning: 3 brokers covers peak throughput with **zero** failure headroom. That's not acceptable here — FinPay needs to survive a full AZ failure, which can remove more than one broker at once.

---

# 22. Case Study — AZ Failure Tolerance

- If exactly 3 brokers are deployed one-per-AZ, losing one AZ removes exactly 1 of the 3 brokers (1/3 of capacity) — but the remaining 2 brokers would then need to absorb 600 MB/sec between them (300 MB/sec each), which is already above the 200 MB/sec benchmarked safe limit per broker.
- Scale up so that losing a **whole AZ** still leaves enough capacity: deploy **6 brokers, 2 per AZ, across 3 AZs**.

```text
6 brokers, 2 per AZ

AZ-A failure  →  lose 2 brokers  →  4 brokers remain

600 MB/sec (peak) / 4 brokers = 150 MB/sec per broker

150 MB/sec < 200 MB/sec (benchmarked safe limit)  →  headroom intact
```

- This is the same throughput-after-failure check as file `01` section 22, just applied against an AZ-level failure instead of a single-broker failure, because that's FinPay's actual stated requirement.

---

# 23. Case Study — Storage Sizing

- Apply file `01`'s formula, then run the `gp3`-specific check from section 11 above.

```text
Average ingress = 300 MB/sec
Retention        = 7 days
RF               = 3
Brokers          = 6
```

- **Total replicated bytes:**

```text
300 MB/sec × 86,400 sec/day = 25,920 GB/day

25,920 GB/day × 7 days = 181,440 GB raw

181,440 GB × RF 3 = 544,320 GB total replicated bytes (cluster-wide)
```

- **Per broker (even distribution across 6 brokers):**

```text
544,320 GB / 6 brokers = 90,720 GB per broker
```

- **With headroom** (targeting ~70% steady-state utilization):

```text
90,720 GB / 0.7 ≈ 129,600 GB per broker  →  ~129.6 TB provisioned
```

- **Reality check against the `gp3` ceiling:** a single `gp3` volume tops out at **64 TiB**. ([Amazon EBS — General Purpose SSD volumes][3]) At ~129.6 TB per broker, a single volume doesn't fit.
  - The tempting fix is "add more brokers until the per-broker number fits under 64 TiB" — but that would push broker count well past what throughput and AZ-failure tolerance actually require (section 21–22 already justified 6 brokers; forcing the count up further purely to dodge a storage cap conflates two different sizing dimensions, the same mistake file `01` section 27 warns against).
  - The correct fix: keep the 6-broker count (it's correctly sized for throughput and failure tolerance) and give each broker **multiple `gp3` volumes** across separate `log.dirs` — e.g. 3 volumes of ~45 TiB each per broker, comfortably covering the ~129.6 TB requirement, each independently provisioned for IOPS/throughput per section 9.
- Storage sizing drove a *volume layout* decision here (multiple `log.dirs`), not a broker-count decision — that distinction is the whole point of doing the math instead of guessing.

---

# 24. Case Study — Instance Family and Self-Managed vs MSK

- **Instance family:** `m7g.2xlarge` — chosen in section 21 from AWS's current published Kafka benchmark numbers, and it's also one of the families Amazon MSK's own Standard brokers run on, which keeps the self-managed-vs-MSK decision below from also requiring a re-benchmark. ([AWS Big Data Blog — right-sizing Apache Kafka clusters][1], [Amazon MSK broker instance types][2])
- **Self-managed EC2 vs Amazon MSK — reasoning it out:**
  - FinPay is "mid-size" — the framing implies a platform team that is not solely dedicated to running Kafka infrastructure full-time.
  - The compliance requirement means TLS/SASL/ACLs and multi-AZ are mandatory either way (section 13, 18) — that work doesn't go away on MSK, so MSK's advantage here isn't "less security work," it's less *infrastructure* work (patching, broker replacement, AZ-balancing enforcement).
  - MSK enforcing even distribution across the 3 AZs (section 12, 17) directly matches FinPay's stated AZ-failure requirement — one less thing to get wrong by hand.
  - Against MSK: the current ~one-release-train version lag (section 17) matters less here, since FinPay doesn't have a stated need for the bleeding-edge Kafka release — a payments audit trail workload benefits far more from stability than from being on the newest Kafka features.
  - **Decision: Amazon MSK**, Standard brokers, `m7g.2xlarge`, 6 brokers across 3 AZs (2 per AZ) — the operational leverage (automated broker replacement, patch management, enforced AZ balance) outweighs the version-currency and per-broker-hour cost premium for a mid-size team with a compliance-driven, stability-over-bleeding-edge workload.

---

# 25. Case Study — Final Architecture

```text
                    Amazon MSK — Standard brokers, m7g.2xlarge

                              AWS Region
                    ┌──────────────┼──────────────┐
                  AZ-A            AZ-B            AZ-C
             Private subnet  Private subnet  Private subnet
              kf1, kf4        kf2, kf5        kf3, kf6
           (2 brokers/AZ,   (2 brokers/AZ,   (2 brokers/AZ,
            3× gp3/broker,   3× gp3/broker,   3× gp3/broker,
            no public IP)    no public IP)    no public IP)

        TLS + SASL + ACLs enforced on every listener (compliance requirement)

        Reachable only via: AWS Client VPN / Direct Connect / apps in-VPC
        Private Route 53 hosted zone for broker DNS

        RF = 3, retention = 7 days, 6 brokers survive a full AZ failure
        with throughput headroom (150 MB/sec/broker vs. 200 MB/sec safe limit)
```

- **Here's how I'd reason through it out loud, start to finish:** peak ingress is 600 MB/sec, and a benchmarked `m7g.2xlarge` broker safely handles about 200 MB/sec, so throughput alone says 3 brokers. But the real requirement isn't "survive normal operation," it's "survive losing an entire AZ" — and with only 3 brokers, one per AZ, losing an AZ would push the survivors past their safe throughput limit. So I double up: 6 brokers, 2 per AZ, and now losing an AZ still leaves 150 MB/sec per broker against a 200 MB/sec ceiling — real headroom, not a knife's edge. Storage sizing off the same 6 brokers comes out to about 130 TB per broker once I account for 7-day retention, RF 3, and operating headroom — more than a single `gp3` volume's 64 TiB cap, so instead of inflating the broker count just to dodge that ceiling, I give each broker multiple `gp3` volumes. Compliance makes TLS/SASL/ACLs non-negotiable regardless of who operates the cluster, and since that work is identical either way, the self-managed-vs-MSK choice comes down to who I want babysitting broker replacement and OS patches at 2 a.m. — for a mid-size team on a stability-first workload, that's Amazon MSK.

---

# 26. Senior Platform Engineer Mental Model

```text
                    Kafka in Production on AWS
                              │
      ┌───────────┬───────────┼───────────┬───────────┬───────────┐
      │           │           │           │           │           │
 Networking     Compute     Storage    AZ/Failure   Security   Ops model
      │           │           │           │           │           │
 private       sustained    gp3 sized  broker.rack  TLS/SASL   self-managed
 subnets,      family        + provi-  + real        + ACLs     vs MSK,
 no public     (e.g. m7g),   sioned    subnet per                weighed
 IP, VPN/      benchmarked   IOPS/     AZ, enforced               against
 bastion/SSM   per-broker    through-  distribution                team size
               throughput    put                                  + compliance
```

- None of these six axes is optional in production. The lab correctly simplified all six to focus on Kafka mechanics — production requires re-deciding every one of them deliberately.

---

# 27. Interview Answer

If asked:

> **"What would actually be different if you took a Kafka lab setup and ran it in production on AWS?"**

A strong answer:

> "Almost everything except the Kafka concepts themselves. Networking moves from a default VPC with public IPs to private subnets with no public IPs at all, reached through a bastion, SSM, or a VPN rather than the open internet — which also means DNS moves from hand-edited `/etc/hosts` to a private Route 53 hosted zone, since IPs change on broker replacement. Compute moves off burstable instances like `t3.small` onto a sustained-performance family — currently something like the `m7g` family, benchmarked for actual safe throughput per broker rather than guessed. Storage moves from an unsized `gp3` volume to one that's explicitly sized off retention, ingress, and replication factor, with IOPS and throughput provisioned separately from capacity. AZ placement becomes deliberate — real subnets per AZ, `broker.rack` set correctly — instead of wherever the console defaulted to. Security adds TLS, SASL, and ACLs, since a lab running PLAINTEXT with no auth is not something you'd ever expose to real traffic. Monitoring moves from eyeballing `--describe` to JMX metrics feeding Prometheus/Grafana or CloudWatch. And then there's a build-vs-buy decision: self-manage on EC2 for full control, or use Amazon MSK, which automates broker provisioning, patching, and replacement, but still leaves you to choose broker type and count, design your topics and partitions, and configure security exactly as you would self-managed."

Then, if pushed for a concrete example:

> "For a payments workload doing 300 MB/sec average ingress with 7-day retention that has to survive a full AZ failure, I wouldn't stop at the broker count throughput alone suggests — I'd size for what's left *after* losing an entire AZ, which usually means doubling up brokers across AZs rather than running exactly one per AZ. And I'd check the storage math against real limits like a single EBS volume's size cap, because if the per-broker number doesn't fit, the fix is more volumes per broker, not an inflated broker count that was never justified by throughput or failure tolerance in the first place."

That's a materially better answer than:

> **"You'd just use bigger instances and turn on encryption."**

---

[1]: https://aws.amazon.com/blogs/big-data/best-practices-for-right-sizing-your-apache-kafka-clusters-to-optimize-performance-and-cost/ "Best practices for right-sizing your Apache Kafka clusters to optimize performance and cost — AWS Big Data Blog"
[2]: https://docs.aws.amazon.com/msk/latest/developerguide/broker-instance-types.html "Amazon MSK broker instance types — Amazon Managed Streaming for Apache Kafka"
[3]: https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html "Amazon EBS General Purpose SSD volumes — Amazon EBS User Guide"
[4]: https://docs.aws.amazon.com/msk/latest/developerguide/msk-create-cluster.html "Create an MSK Provisioned cluster — Amazon Managed Streaming for Apache Kafka"
[5]: https://endoflife.date/amazon-msk "Amazon MSK — endoflife.date"
[6]: https://kafka.apache.org/blog/releases/ "Release Announcements — Apache Kafka"
[7]: https://www.automq.com/blog/aws-kafka-pricing-explained-msk-storage-traffic-and-hidden-cost-drivers "AWS Kafka Pricing Explained: MSK, Storage, Traffic, and Hidden Cost Drivers"
