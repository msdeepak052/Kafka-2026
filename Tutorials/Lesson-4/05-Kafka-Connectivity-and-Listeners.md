# Kafka Connectivity and Listeners — `advertised.listeners`

> **Scope:** Course items 32 ("Can I connect to my Kafka cluster?") and 33
> ("`advertised.listeners` setting — most important setting").
>
> **Why this file matters more than most:** the course itself flags item 33
> as the single most important Kafka networking setting. Get this wrong and
> your cluster looks broken even when every broker is healthy. Get it right
> and you understand *the* recurring "Kafka won't connect" support ticket
> for the rest of your career.

Your previous document (`04`) got `kf1`, `kf2`, `kf3` running as a real
3-broker cluster on AWS — Kafka 3.9.2, `/opt/kafka`, `kafka.service`,
`broker.id` 1/2/3 — talking to the `zk1`/`zk2`/`zk3` ZooKeeper ensemble from
Lesson-3. Each broker currently runs the simplest possible networking
config:

```properties
listeners=PLAINTEXT://kf1:9092
advertised.listeners=PLAINTEXT://kf1:9092
```

(with `kf2`/`kf3` substituted on the other two nodes). This works — but
only from *inside* the VPC. This file explains exactly why, and fixes it.

---

# 1. The Question This File Answers

Two people run the exact same command:

```bash
kafka-console-producer.sh --bootstrap-server kf1:9092 --topic test
```

```text
Person A: SSH'd into kf2 (another node inside the VPC)
   → Works fine.

Person B: their own laptop at home
   → Connects... then hangs. Or errors. Never produces a message.
```

Same command. Same cluster. Completely different outcome.

By the end of this file you'll know exactly why, and how to make Person B's
laptop work too.

---

# 2. The Wrong Mental Model

Most beginners assume a Kafka client works like this:

```text
Client
  │
  │  bootstrap.servers=kf1:9092
  ▼
kf1
  │
  │  (kf1 relays everything on your behalf)
  ▼
Done
```

i.e. "I connect to one broker, and that broker handles my request,
forwarding to other brokers internally if needed."

**This is not how Kafka works.** Kafka clients are *not* thin clients that
talk to one broker who proxies for the rest. They are "smart" — they learn
the cluster topology and then talk **directly** to whichever broker actually
owns the partition they need.

---

# 3. The Correct Mental Model — Two Distinct Steps

```text
Kafka client connection
        │
        ├── STEP 1: Bootstrap
        │     "Connect to ANY address in bootstrap.servers
        │      just to ask: what does this cluster look like?"
        │
        └── STEP 2: Direct connect
              "Using the answer from Step 1, connect DIRECTLY
               to whichever broker actually leads each partition
               I need — from now on, forever, for this client."
```

Every produce, every consume, every fetch after the initial bootstrap goes
**directly** to the partition leader's broker — never routed back through
the broker you originally bootstrapped through.

---

# 4. Step 1 in Detail — Bootstrap

```text
bootstrap.servers=kf1:9092
```

- This is **not** "the broker I will always talk to."
- It is only an **entry point** — a way to reach *some* broker in the
  cluster so you can ask it a question.
- You could list `kf1:9092`, or `kf2:9092`, or all three — it barely
  matters which one you pick, since its only job here is to answer one
  question: *"what does this cluster look like?"*
- This is why `bootstrap.servers` is usually written with more than one
  address — if `kf1` happens to be down when the client starts, it can
  still bootstrap through `kf2` or `kf3`.

---

# 5. Step 2 in Detail — The Metadata Response

The broker you bootstrapped through replies with **cluster metadata**:

```text
MetadataResponse
   │
   ├── List of all brokers in the cluster
   │      broker.id = 1 → advertised address = ???
   │      broker.id = 2 → advertised address = ???
   │      broker.id = 3 → advertised address = ???
   │
   └── For every partition of the requested topic
          Partition 0 → Leader = broker.id 2
          Partition 1 → Leader = broker.id 3
          Partition 2 → Leader = broker.id 1
```

- The critical field here is **each broker's advertised address**.
- That address comes from exactly one place: that broker's own
  `advertised.listeners` setting.
- The client does **not** use `bootstrap.servers` again for these
  connections. It uses whatever address the metadata response told it.

---

# 6. Step 3 in Detail — Direct Connection

```text
Client now holds:
   Partition 0 → connect directly to broker 2's advertised address
   Partition 1 → connect directly to broker 3's advertised address
   Partition 2 → connect directly to broker 1's advertised address
```

- The client opens **new, direct TCP connections** to each of those
  addresses.
- Every subsequent produce/consume/fetch call for that partition goes over
  that direct connection.
- `kf1` (the broker used for bootstrapping) is now completely out of the
  picture for partitions it doesn't lead — it did its one job (handing out
  metadata) and is not involved further.

---

# 7. Full ASCII Sequence Diagram

```text
   Client                    kf1 (bootstrap)              kf2                kf3
     │                            │                         │                  │
     │  STEP 1 — Bootstrap        │                         │                  │
     │───── TCP connect :9092 ───▶│                         │                  │
     │◀──── connection accepted ──│                         │                  │
     │                            │                         │                  │
     │───── MetadataRequest ─────▶│                         │                  │
     │                            │                         │                  │
     │  STEP 2 — Metadata reply   │                         │                  │
     │◀──── MetadataResponse ─────│                         │                  │
     │        "Partition 0 → Leader = kf2                   │                  │
     │         Partition 1 → Leader = kf3                   │                  │
     │         advertised.listeners:                        │                  │
     │           broker 2 = kf2:9092                        │                  │
     │           broker 3 = kf3:9092"                       │                  │
     │                            │                         │                  │
     │  STEP 3 — Direct connects (kf1 is no longer involved) │                  │
     │───────────────────────────────── TCP connect :9092 ─▶│                  │
     │◀──────────────────────────────── accepted ────────── │                  │
     │───────────────────────────────── ProduceRequest P0 ─▶│                  │
     │                            │                         │                  │
     │───────────────────────────────────────────────── TCP connect :9092 ────▶│
     │◀──────────────────────────────────────────────────── accepted ──────────│
     │───────────────────────────────────────────────── ProduceRequest P1 ────▶│
```

- Notice `kf1` only appears in Steps 1–2.
- Everything after that is the client talking **directly** to `kf2` and
  `kf3` — using the addresses `kf1` told it about, not `kf1` itself.

---

# 8. The Analogy — The Receptionist

- You walk into a large hospital and go to the **front desk**
  (bootstrap.servers) — the only address you knew before arriving.
- You ask the receptionist: *"I need cardiology."*
- The receptionist doesn't personally treat you and doesn't relay messages
  back and forth on your behalf. They say: *"Cardiology is in Building C,
  3rd floor, Room 310. Go there directly."* (the `MetadataResponse`, with
  the advertised address.)
- You then walk **directly** to Building C yourself, for this visit and
  every future visit — you don't keep going back through the front desk.
- **Now imagine the receptionist gives you directions that only make sense
  *inside* the hospital campus** — "take the internal tram to Building C" —
  but you're a visitor from another city with no access to the internal
  tram system. You successfully reached the front desk (you got *into* the
  building). But the directions you were given are useless to you. You're
  stuck in the lobby.
- That mismatch — reachable front desk, unreachable directions — is
  *exactly* what a bad `advertised.listeners` value does to a Kafka client.

---

# 9. Why `advertised.listeners` Is "The Most Important Setting"

- Step 1 (bootstrap) can **succeed completely** — TCP connects fine,
  metadata comes back fine — while Step 3 (direct connect) **fails
  entirely**.
- This produces a uniquely confusing failure mode for beginners:
  ```text
  "It connected... I saw it connect... then it just hangs."
  ```
- The client isn't stuck talking to a dead broker. It's stuck trying to
  reach an address that was **never reachable from where the client is
  running** — because that address is exactly what the *broker itself*
  claimed, via `advertised.listeners`, during Step 2.
- Every other broker setting (retention, replication factor, partition
  count...) affects *what* Kafka does. `advertised.listeners` affects
  *whether a client outside the broker's own network can talk to it at
  all* — silently, without any error at startup, without any complaint
  from the broker itself. The broker is perfectly healthy. The client is
  the one left hanging.

---

# 10. Case Study — Reproducing the Exact Failure

This is the scenario from Section 1, in full, using the **current**
(pre-fix) config from `04`:

```properties
# on kf1 right now
listeners=PLAINTEXT://kf1:9092
advertised.listeners=PLAINTEXT://kf1:9092
```

## From your laptop, using kf1's public IP

```bash
kafka-console-producer.sh \
  --bootstrap-server <kf1-PUBLIC-IP>:9092 \
  --topic test-topic
```

## What actually happens

```text
1. TCP connect to <kf1-PUBLIC-IP>:9092
      ↓
   SUCCEEDS
      │  (security group allows your IP on 9094... wait, 9092 —
      │   inbound rule for 9092 from "My IP" was already open
      │   for hands-on testing in lesson 04)
      ▼
2. MetadataRequest sent, MetadataResponse received
      ↓
   SUCCEEDS
      │  broker 1's advertised.listeners = "kf1:9092"
      ▼
3. Client now tries to open its "real" connection to broker 1
   at the address the broker just advertised: kf1:9092
      ↓
   kf1 is a bare hostname with no domain — your laptop's DNS
   resolver has never heard of it and has no way to look it up
      ↓
   FAILS
```

## The actual terminal output you'll see

```text
>hello kafka
[2026-08-22 10:16:03,880] WARN [Producer clientId=console-producer] Error connecting to node kf1:9092 (id: 1 rack: null) (org.apache.kafka.clients.NetworkClient)
java.net.UnknownHostException: kf1
	at java.base/java.net.InetAddress$CachedAddresses.get(InetAddress.java:...)
	at java.base/java.net.InetAddress.getAllByName0(InetAddress.java:...)
	at java.base/java.net.InetAddress.getAllByName(InetAddress.java:...)
	at org.apache.kafka.clients.DefaultHostResolver.resolve(DefaultHostResolver.java:27)
	at org.apache.kafka.clients.ClientUtils.resolve(ClientUtils.java:114)
	...
[2026-08-22 10:16:03,882] WARN [Producer clientId=console-producer] Bootstrap broker <kf1-PUBLIC-IP>:9092 (id: -1 rack: null) disconnected (org.apache.kafka.clients.NetworkClient)
[2026-08-22 10:17:03,900] ERROR Error when sending message to topic test-topic with key: null, value: 11 bytes with error: (org.apache.kafka.clients.producer.internals.ErrorLoggingCallback)
org.apache.kafka.common.errors.TimeoutException: Topic test-topic not present in metadata after 60000 ms.
```

- The **bootstrap** worked (that's why you got as far as typing a message
  and pressing Enter before anything failed).
- The **failure** is `UnknownHostException: kf1` — your laptop simply
  cannot resolve a bare private hostname that only exists inside AWS's VPC
  DNS / your `/etc/hosts` files on the `kf`/`zk` nodes.
- Even if `kf1` happened to resolve to a **private IP** instead of failing
  DNS outright, the symptom would just change from an immediate
  `UnknownHostException` to a silent **connection timeout** — your laptop
  still has no network route into the VPC's private address space either
  way.
- After 60 seconds of retrying, the producer gives up with
  `TimeoutException: Topic test-topic not present in metadata`.

This is precisely the "it connected, then it hung" failure predicted in
Section 9.

---

# 11. Kafka's Fix: The Multi-Listener Model

Kafka doesn't force one broker to have one address. A broker can define
**multiple named listeners**, each serving a different audience.

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
advertised.listeners=INTERNAL://kf1:9092,EXTERNAL://<kf1-public-ip>:9094
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
```

## What each line means

```text
listeners
   │
   └── Which host:port(s) THIS BROKER actually binds/listens on.
        0.0.0.0 = "accept connections arriving on any of this
        machine's network interfaces" — valid here, unlike in
        advertised.listeners.

advertised.listeners
   │
   └── What THIS BROKER tells clients to use, per listener name,
        in every MetadataResponse. This is what actually goes out
        over the wire to clients (Section 5).

listener.security.protocol.map
   │
   └── Maps each listener NAME to a security protocol
        (PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL / ...).
        INTERNAL and EXTERNAL are just labels you invent —
        Kafka doesn't attach special meaning to those words.

inter.broker.listener.name
   │
   └── Which listener BROKERS use to talk to EACH OTHER
        (replication, controller traffic). Must point at a
        listener whose advertised address stays reachable
        privately — never route broker-to-broker replication
        traffic over the public internet.
```

- Apache Kafka's own configuration reference confirms this shape:
  `listeners` is described as *"Listener List — Comma-separated list of
  URIs we will listen on and the listener names"* (example format:
  `PLAINTEXT://myhost:9092,SSL://:9091`); `advertised.listeners` is *"the
  addresses clients should use"* and explicitly notes it is **not** valid
  to advertise the `0.0.0.0` meta-address there (only in `listeners`);
  `inter.broker.listener.name` is *"Name of listener used for
  communication between brokers."* (Apache Kafka configuration
  documentation, `kafka.apache.org/39/generated/kafka_config.html`)

## Why `inter.broker.listener.name` must stay internal

```text
If inter.broker.listener.name = EXTERNAL (public IP):

kf1  ══════ replication traffic ══════▶  kf2
      (every byte round-trips out to
       the public internet and back in)
            │
            ├── extra AWS data-transfer cost
            ├── extra latency
            └── extra attack surface

If inter.broker.listener.name = INTERNAL (private hostname):

kf1  ══════ replication traffic ══════▶  kf2
      (stays inside the VPC — free,
       fast, and not internet-exposed)
```

This is exactly why the fix keeps `INTERNAL` — the private, VPC-only
listener — as the `inter.broker.listener.name`, and only adds `EXTERNAL`
as an *additional* option for clients that can't reach the VPC directly.

---

# 12. The Fix for This Lab — Design

```text
kf1
  ├── INTERNAL  :9092 → advertised as kf1:9092              (VPC-only clients + inter-broker)
  └── EXTERNAL  :9094 → advertised as <kf1-public-ip>:9094   (your laptop)

kf2
  ├── INTERNAL  :9092 → advertised as kf2:9092
  └── EXTERNAL  :9094 → advertised as <kf2-public-ip>:9094

kf3
  ├── INTERNAL  :9092 → advertised as kf3:9092
  └── EXTERNAL  :9094 → advertised as <kf3-public-ip>:9094
```

```text
                  Your Laptop
                       │
                       │  :9094 (EXTERNAL, public IP)
                       ▼
     ┌─────────────────┼─────────────────┐
     ▼                 ▼                 ▼
    kf1               kf2               kf3
     │  ╲               │  ╲              │  ╲
     │   ╲── :9092 (INTERNAL, private) ───┼───┤
     │                  │                 │
     └────── inter-broker replication ────┘
              (always over INTERNAL)
```

- Every broker now answers on **two** ports: `9092` for anyone already
  inside the VPC, `9094` for anyone outside it.
- The client picks which one it gets **based on which port it originally
  bootstrapped through** — Kafka returns the matching listener's
  advertised address for the listener the client connected on.

---

# 13. AWS Console — Open Port 9094

Your `kafka-sg` (created in `04`) currently allows `9092` from "My IP" for
in-VPC hands-on testing, but **9094 doesn't exist yet** — add it:

1. Open the **EC2 console** → left navigation pane → **Network & Security**
   → **Security Groups**.
2. Select `kafka-sg`.
3. Choose **Edit inbound rules**.
4. Choose **Add rule**:
   - **Type:** Custom TCP
   - **Port range:** `9094`
   - **Source:** **My IP** (auto-fills your current public IP)
5. Choose **Save rules**.

```text
kafka-sg inbound rules (after this step)

  22    ← My IP        (SSH)
  9092  ← My IP         (existing, in-VPC/direct testing)
  9092  ← kafka-sg      (broker-to-broker, if configured that way in 04)
  9094  ← My IP         (NEW — external client access)
```

> Same caveat as Lesson-3's SSH rule: **My IP** does not auto-update. If
> your home/mobile IP changes later and 9094 suddenly stops connecting,
> re-edit this rule and reselect **My IP** to refresh it before assuming
> anything is wrong with Kafka itself.

---

# 14. Hands-On — Edit `server.properties` on All 3 Nodes

SSH into each node and edit `/opt/kafka/config/server.properties`. Use
**each node's own public IP** for its `EXTERNAL` advertised listener — do
not reuse `kf1`'s public IP on `kf2` or `kf3`.

## On kf1

```bash
ssh -i kafka-lab.pem ubuntu@<kf1-public-ip>
sudo nano /opt/kafka/config/server.properties
```

Replace the existing listener lines with:

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
advertised.listeners=INTERNAL://kf1:9092,EXTERNAL://<kf1-public-ip>:9094
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
```

## On kf2

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
advertised.listeners=INTERNAL://kf2:9092,EXTERNAL://<kf2-public-ip>:9094
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
```

## On kf3

```properties
listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
advertised.listeners=INTERNAL://kf3:9092,EXTERNAL://<kf3-public-ip>:9094
listener.security.protocol.map=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
inter.broker.listener.name=INTERNAL
```

- `0.0.0.0` in `listeners` is fine and normal — it means "bind to every
  network interface on this machine." Only `advertised.listeners` must
  never use `0.0.0.0`, because that address means nothing to a remote
  client.
- Every property name and syntax above matches the current Apache Kafka
  configuration reference for `listeners`, `advertised.listeners`,
  `listener.security.protocol.map`, and `inter.broker.listener.name`.

---

# 15. Rolling Restart — One Node at a Time

```bash
sudo systemctl restart kafka.service
```

- Restart **one broker at a time**, not all three simultaneously.
- Wait for each broker to fully rejoin the cluster (`systemctl status
  kafka.service` shows `active (running)`, and `journalctl -u kafka -f`
  stops showing startup/recovery log lines) before restarting the next
  one.
- Why one at a time: with `RF=3` and 3 brokers, restarting all three
  together would take every replica of every partition offline
  simultaneously — a full outage. One-at-a-time keeps at least 2/3 replicas
  available throughout.
- **This is exactly a rolling restart** — the general "how do I safely
  change a broker's config in production" procedure gets its own dedicated
  lesson later in this course; this file only needs the one-line version
  because it's the first time you're touching `server.properties` after
  the brokers are already serving traffic.

```text
Restart order

kf1 → wait for healthy → kf2 → wait for healthy → kf3 → wait for healthy
```

---

# 16. Verify — Basic Reachability First

Before pulling out a full Kafka client, confirm the **port itself** is
reachable from outside AWS — this isolates "network/security-group problem"
from "Kafka problem":

```bash
nc -zv <kf1-public-ip> 9094
nc -zv <kf2-public-ip> 9094
nc -zv <kf3-public-ip> 9094
```

Expected:

```text
Connection to <kf1-public-ip> 9094 port [tcp/*] succeeded!
```

If this fails, **stop** — don't move on to testing with a Kafka client
yet. Go back to Section 13 (security group) before troubleshooting
anything Kafka-specific.

---

# 17. Verify — A Real Client, From Outside AWS

If you don't already have Kafka's CLI tools on your own machine (or in
WSL), install them the same way `04` installed them on the brokers:

```bash
cd ~
wget https://archive.apache.org/dist/kafka/3.9.2/kafka_2.13-3.9.2.tgz
tar -xzf kafka_2.13-3.9.2.tgz
cd kafka_2.13-3.9.2
```

You do **not** need Java's Kafka broker itself running locally — only the
`bin/` client scripts, which need a local JVM (`java -version` to confirm
one is installed).

## Produce, bootstrapping through all 3 EXTERNAL listeners

```bash
bin/kafka-console-producer.sh \
  --bootstrap-server <kf1-public-ip>:9094,<kf2-public-ip>:9094,<kf3-public-ip>:9094 \
  --topic test-topic
```

```text
>hello from outside AWS
>this actually works now
```

## Consume, from the beginning, in a second terminal

```bash
bin/kafka-console-consumer.sh \
  --bootstrap-server <kf1-public-ip>:9094,<kf2-public-ip>:9094,<kf3-public-ip>:9094 \
  --topic test-topic \
  --from-beginning
```

```text
hello from outside AWS
this actually works now
```

- Listing all 3 EXTERNAL addresses in `--bootstrap-server` (not just
  `kf1`) means the client can still bootstrap even if whichever node it
  tries first happens to be down — same reasoning as Section 4.
- Compare this to Section 10: same command shape, same laptop, only
  difference is port `9094` instead of `9092` and the fixed
  `advertised.listeners`. That's the entire fix.

---

# 18. The Tradeoff You Just Accepted — Public IPs Aren't Stable

- `<kf1-public-ip>` in `advertised.listeners` is the **current** public IP
  of that EC2 instance — nothing more.
- Default VPC instances **without an Elastic IP** get handed a **brand
  new** public IP every time they're **stopped and started** (a *reboot*
  keeps the same IP; a *stop/start* does not).
- If that happens:
  ```text
  kf1 stopped → started
        ↓
  New public IP assigned
        ↓
  advertised.listeners still has the OLD public IP
        ↓
  External clients bootstrap fine (if they hit a different node first)
  but fail Step 3 the moment metadata routes them to kf1
        ↓
  Same UnknownHostException/timeout failure as Section 10 — except
  now it looks like it "used to work and mysteriously broke"
  ```
- **Fix when this happens:** re-edit `server.properties` with the new
  public IP and do another rolling restart (Sections 14–15).
- **Production-grade fix, forward pointer only:** an **Elastic IP** is a
  public IP address you allocate and attach to an instance — it stays the
  same across stop/start (only releasing it or detaching it changes that).
  Attaching one per broker turns this whole section into a non-issue.
  That's a deliberately separate topic — this file just needed you to know
  the tradeoff exists before you walk away thinking today's fix is
  permanent.

---

# 19. Troubleshooting Checklist — Listener & Connectivity Issues

| Symptom | Likely cause | Check |
| --- | --- | --- |
| Client hangs after connecting, no error for a while | `advertised.listeners` points at an address the client can't route to | What does `advertised.listeners` actually say? Can *this specific client* resolve/route to it? |
| `UnknownHostException: kf1` (or similar bare hostname) | Client is outside the VPC and was handed a private hostname with no public DNS record | Are you bootstrapping via the `EXTERNAL` listener/port, or accidentally the `INTERNAL` one? |
| `Connection refused` immediately (not a timeout) | Nothing is listening on that port, or it's actively rejected | Is `kafka.service` actually running? Did the `listeners` line on that broker actually bind that port? |
| Connection times out (no refusal, no response) | Security group / firewall is silently dropping the packets | Re-check the inbound rule for that exact port and source (Section 13) — a timeout, not a refusal, is the classic SG symptom |
| Works from another `kf`/`zk` node, fails from your laptop | You're testing the `INTERNAL` listener/port from outside the VPC | Use the `EXTERNAL` listener's port (`9094`) and the broker's public IP, not `9092`/private hostname |
| `nc -zv` succeeds but the Kafka client still fails | Port is reachable, but Kafka's `advertised.listeners` is still wrong or stale (e.g. after a stop/start IP change) | Re-verify the *current* public IP matches what's configured (Section 18) |
| Some partitions work, others hang | Only some brokers have the `EXTERNAL` listener configured/reachable | Confirm all 3 nodes got the Section 14 edit and completed their rolling restart |
| `getent hosts kf1` / `/etc/hosts` shows a private IP, but you're testing from outside the VPC | Confusing the `INTERNAL` (private-hostname) path with the `EXTERNAL` (public-IP) path | `/etc/hosts` entries for `kf1`/`kf2`/`kf3` only exist *inside* the VPC (Lesson-3 Section 20 pattern) — your laptop has no such entries and isn't supposed to |
| Broker fails to start after the Section 14 edit | Typo in `listener.security.protocol.map` (listener name mismatch) or a port already in use | `journalctl -u kafka -n 100 --no-pager`; confirm every listener name used in `listeners` also appears in `listener.security.protocol.map` |
| Replication/ISR looks unhealthy after the edit | `inter.broker.listener.name` accidentally pointed at `EXTERNAL` instead of `INTERNAL` | Re-check Section 14 — it must be `INTERNAL` on all 3 nodes |

---

# 20. Interview Answer

If asked:

> **"Why would a Kafka client fail to connect even though the broker is
> healthy and the bootstrap connection succeeds?"**

A strong answer:

> "Kafka client connections are two-step. The client first connects to any
> address in `bootstrap.servers` just to fetch cluster metadata — which
> broker leads which partition, and each broker's advertised address. Every
> connection after that is made directly to those advertised addresses, not
> back through the bootstrap broker. If `advertised.listeners` on a broker
> points at an address — a private hostname or private IP — that the client
> can't route to, the bootstrap step succeeds but every subsequent direct
> connection hangs or times out. That's why I always configure multiple
> named listeners: an internal one, advertised as a private address, used
> for inter-broker traffic and same-network clients; and an external one,
> advertised as a reachable public address, for clients outside that
> network — with `inter.broker.listener.name` always pinned to the internal
> one so replication traffic never leaves the private network."

---

# 21. Final Hands-On Checklist

```text
Concept
□ Explain the bootstrap → metadata → direct-connect flow from memory
□ Explain why bootstrap can succeed while the real connection fails
□ Explain the receptionist analogy in your own words

AWS
□ Add inbound rule: kafka-sg, TCP 9094, source My IP

Configuration (all 3 nodes)
□ listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9094
□ advertised.listeners uses each node's OWN public IP for EXTERNAL
□ listener.security.protocol.map maps both names to PLAINTEXT
□ inter.broker.listener.name=INTERNAL (not EXTERNAL)

Operations
□ Rolling restart, one node at a time, verified healthy before the next
□ nc -zv each public IP on 9094 before testing with a real client
□ Produce + consume successfully from OUTSIDE the VPC (laptop/WSL)

Understanding
□ Know what breaks (and why) if a broker's public IP changes after a stop/start
□ Know that Elastic IP is the production-grade fix (without needing to implement it yet)
□ Can diagnose "hangs after connecting" vs "refused immediately" vs "times out" as three different root causes
```
