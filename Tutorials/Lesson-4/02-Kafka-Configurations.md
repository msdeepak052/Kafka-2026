# Kafka Configuration

For a **Senior Platform Engineer**, don't think of "Kafka configuration" as:

> "The settings inside `server.properties`."

Think of it as:

> **A set of independent, layered configuration surfaces — broker, topic, producer, consumer, and (for now) ZooKeeper — that combine to determine how any single message actually behaves.**

This file is purely conceptual. No hands-on editing happens here — that starts in file `03` onward, once the vocabulary below is in place.

---

# 1. What "Kafka Configuration" Actually Means

- There is no single "Kafka config file."
  - Newcomers assume `server.properties` is "the Kafka config." It isn't — it's only one layer.
  - Kafka configuration is really **several independent layers**, each owned by a different part of the system.
- Each layer:
  - Is set in a different place.
  - Is set by a different person/role (platform team vs. application team).
  - Takes effect at a different time (broker startup vs. topic creation vs. per-request from a client).
- Getting this mental model right **before** touching `server.properties` prevents a very common mistake:
  - Assuming a broker setting controls something a topic or client setting actually controls (or vice versa).

```text
"Kafka Configuration"
        │
        ├── Broker / server config     (server.properties)
        ├── Topic-level config         (per-topic overrides)
        ├── Producer client config     (set by the producing application)
        ├── Consumer client config     (set by the consuming application)
        └── ZooKeeper-related config   (relevant while this cluster is ZK-based)
```

---

# 2. The Five Layers, One Line Each

- **Broker/server config** — how the broker itself behaves; lives in `server.properties`; mostly static (restart required), some settings can be changed dynamically.
- **Topic-level config** — per-topic overrides of certain broker defaults (e.g. this one topic keeps data longer than every other topic).
- **Producer config** — controls how a producing application sends data (durability, batching, compression); the broker has no say in this.
- **Consumer config** — controls how a consuming application reads data (offsets, group membership, commit behavior); also entirely client-side.
- **ZooKeeper-related config** — how the broker talks to the ZooKeeper ensemble; relevant here because this lab's Kafka is still running in ZooKeeper mode.

```text
Producer ──▶ Broker ──▶ Topic (storage) ──▶ Broker ──▶ Consumer
   │            │              │               │           │
producer     broker        topic-level      broker      consumer
 config      config          config          config       config
```

- Notice a single message's journey touches **at least three** of these layers on the way through.

---

# 3. Analogy — Configuration as Company Policy Levels

- Think of a large organization instead of a Kafka cluster.
- **Company-wide HR policy** = broker-level config.
  - Applies to everyone by default unless a department says otherwise.
  - Example: "Standard data retention for internal records: 7 years."
  - Set centrally, changed rarely, requires a formal process (restart) for most changes.
- **Department policy** = topic-level config.
  - One department can override the company default for its own records.
  - Example: Legal department: "Our contracts are retained for 10 years, not 7."
  - This does not change company policy — it overrides it *for that one department only*.
- **Individual employee's working style** = producer/consumer client config.
  - How an individual actually gets their work done day-to-day.
  - Example: "I always double-check my work before submitting" (≈ `acks=all`) vs. "I submit fast and fix mistakes later" (≈ `acks=1`).
  - The company and the department don't dictate this — it's chosen by the person (application) doing the work.
- **The company directory service** = ZooKeeper-related config.
  - Everyone needs to know where to look up "who is currently in charge of what" — in this lab, that's ZooKeeper.
  - It's infrastructure that supports the other layers; it isn't itself "business policy."

```text
Company HR Policy         → broker/server config     (default.replication.factor, retention...)
   └── Department Policy  → topic-level config        (override for one topic)
        └── Employee      → producer/consumer config  (acks, batch.size, group.id, ...)

Company Directory Service → ZooKeeper-related config  (zookeeper.connect, session timeout)
```

- The key idea to carry forward: **a lower layer can override a higher layer for its own scope, but the higher layer still supplies the default when nothing overrides it.**

---

# 4. Layer 1 — Broker / Server Config (`server.properties`)

- This is the **broker's own configuration file** — it describes how one broker process behaves.
- Characteristics:
  - Lives on disk on each broker host.
  - Most settings are **static**: you edit the file, then restart the broker for the change to take effect.
  - A subset of settings are **dynamic**: they can be changed at runtime using `kafka-configs.sh --alter`, without a restart.
    - This lesson only needs you to know dynamic configuration *exists* as a concept.
    - The actual command syntax and workflow is covered later in its own dedicated hands-on lesson ("Hands-On: How to change a Kafka Broker Configuration").
- What it governs:
  - Broker identity (`broker.id`).
  - Where the broker listens for connections and how clients reach it (`listeners`, `advertised.listeners`).
  - Where log segments are stored on disk (`log.dirs`).
  - How the broker reaches the ZooKeeper ensemble (`zookeeper.connect`).
  - Cluster-wide *defaults* for things topics can later override (`num.partitions`, `log.retention.hours`, `default.replication.factor`, `min.insync.replicas`).

```text
server.properties
        │
        ├── Identity        broker.id
        ├── Networking       listeners, advertised.listeners
        ├── Storage          log.dirs
        ├── Coordination     zookeeper.connect (this lab)
        └── Defaults         num.partitions, retention, replication...
```

---

# 5. Layer 2 — Topic-Level Config

- Every topic can carry its **own** configuration that overrides selected broker defaults, scoped to that one topic.
- Set either:
  - At topic-creation time (`kafka-topics.sh --create --config ...`), or
  - Afterward, by altering an existing topic's config.
- Common examples:
  - `retention.ms` — how long this topic's data is kept, overriding the broker's `log.retention.hours` default.
  - `cleanup.policy` — `delete` (age/size-based expiry) vs. `compact` (keep latest value per key) for this topic specifically.
  - Per-topic overrides for things like `min.insync.replicas` or segment size.
- Why this layer exists:
  - Not every topic should follow the same rules.
  - A `payments-audit-log` topic and a `page-click-events` topic have very different retention and durability needs — but they can live on the same brokers, under the same broker defaults, with different topic-level overrides.

```text
Broker default:  retention.ms = 604800000   (7 days, from log.retention.hours=168)
        │
        ├── Topic "page-click-events"   → no override → keeps 7-day default
        └── Topic "payments-audit-log"  → retention.ms=31536000000 (365 days) → overridden
```

---

# 6. Layer 3 — Producer Client Config

- Set by the **application producing messages**, not by the broker and not by the topic.
- The broker cannot force a producer to behave a certain way — it can only enforce guarantees it controls (like `min.insync.replicas`) once a message arrives.
- Common producer settings to know the *names* of, before the hands-on lessons:
  - `acks` — how much replica acknowledgment the producer waits for before considering a write successful.
  - `batch.size` — how much data the producer tries to batch per request.
  - `linger.ms` — how long the producer waits to fill a batch before sending anyway.
  - `compression.type` — whether/how the producer compresses batches before sending.
- Platform-engineer framing:
  - These settings trade off **latency vs. throughput vs. durability**, entirely on the producing application's side.
  - Two different producer applications can write to the exact same topic with completely different `acks`/`batch.size` choices.

```text
Producer App A  → acks=all,  linger.ms=20   (durability-first)
Producer App B  → acks=1,    linger.ms=0    (latency-first)
        │
        └──▶ both write to the SAME topic, on the SAME broker
```

---

# 7. Layer 4 — Consumer Client Config

- Set by the **application consuming messages** — again, independent of broker and topic config.
- Common consumer settings to know the *names* of:
  - `group.id` — which consumer group this consumer belongs to (drives partition assignment and offset tracking).
  - `auto.offset.reset` — what to do when there's no committed offset yet (`earliest` vs. `latest`).
  - `enable.auto.commit` — whether offsets are committed automatically on an interval, or manually by the application.
- Why this matters conceptually:
  - Broker/topic config decides how long data is *retained*.
  - Consumer config decides how much of that retained data a *given consumer* actually chooses to read.
  - These are two different questions that are easy to conflate when first learning Kafka.

```text
Topic retention (broker/topic layer):  "data lives for 7 days"
Consumer auto.offset.reset (client layer): "where do I start reading if I have no offset yet?"
        │
        └── These answer different questions — retention ≠ read starting point
```

---

# 8. Layer 5 — ZooKeeper-Related Config

- Still relevant **for this lab specifically**, because this cluster runs Kafka in ZooKeeper mode.
- Lives inside `server.properties`, but conceptually it's its own category — it's about broker-to-ZooKeeper coordination, not about topics or clients.
- Key settings to know the names of:
  - `zookeeper.connect` — the ZooKeeper ensemble connection string (in this lab: the `zk1`, `zk2`, `zk3` hosts built in Lesson-3).
  - `zookeeper.session.timeout.ms` — how long ZooKeeper waits before considering a broker's session dead.
- Historical note, worth stating precisely since this lab is intentionally ZK-based right now:
  - Apache Kafka **3.9.x is the final release line that supports ZooKeeper mode** — Kafka 4.0 removed ZooKeeper mode entirely and runs KRaft-only.
  - This lab uses **Kafka 3.9.2**, a patch release within that final ZK-capable 3.9.x line, deliberately — it's the last version where a ZooKeeper-based setup like the one built in Lesson-3 is still fully supported.
  - Source: the official Kafka 4.0 upgrade guide states "Apache Kafka 4.0 only supports KRaft mode - ZooKeeper mode has been removed," and the Kafka 3.9.0 release announcement / KIP-833 material describe 3.9 as the final 3.x release still shipping (deprecated) ZooKeeper mode and the recommended bridge release toward 4.0.

```text
Kafka 3.9.x  → last release line with ZooKeeper mode (deprecated but functional)
Kafka 4.0+   → ZooKeeper mode removed, KRaft only

This lab → Kafka 3.9.2 → chosen specifically because it still supports
           the ZK1/ZK2/ZK3 ensemble built in Lesson-3
```

---

# 9. Precedence — How the Layers Actually Stack

- The relationship between the layers is **default → override → shape**, not "whichever one wins."
- Step by step, for a single concrete setting (`retention.ms` / `log.retention.hours`):
  - Broker sets a **cluster-wide default**.
    - `log.retention.hours=168` in `server.properties` → every new topic defaults to 7-day retention.
  - Topic-level config can **override that default for one topic only**.
    - `payments-audit-log` topic is created with `retention.ms=31536000000` → this one topic keeps data for 365 days, every other topic still gets 7.
  - Client-side config (producer/consumer) doesn't override retention at all — it **shapes behavior on top of** whatever the topic already decided.
    - A consumer with `auto.offset.reset=earliest` reading `payments-audit-log` can read back up to 365 days of history, because that's what the topic layer made available.
    - The same consumer setting on `page-click-events` can only reach back 7 days, because that topic never overrode the broker default.
- The general pattern to remember:

```text
Broker config     → sets the DEFAULT for the whole cluster
Topic config       → OVERRIDES the default for one topic
Producer config     → SHAPES how data gets written on top of that
Consumer config     → SHAPES how data gets read on top of that
```

- None of these layers "beats" another in a conflict sense — they answer different questions at different scopes.

---

# 10. Broker Settings Worth Knowing Before the Hands-On Begins

- These are introduced here only as **vocabulary/concepts** — actually editing them happens in later files.

- `broker.id`
  - A unique integer identifying this broker within the cluster.
  - Must be different on every broker (e.g. `0`, `1`, `2` for a 3-broker cluster).

- `listeners` / `advertised.listeners`
  - Controls what address/port the broker binds to, and what address it tells clients to use to reach it.
  - Important enough — and easy enough to get wrong on AWS specifically — that it gets its **own dedicated lesson** later in this series. Not covered in depth here.

- `log.dirs`
  - Filesystem path(s) where the broker stores its actual log segments (the real message data).
  - No default — must be explicitly set.

- `zookeeper.connect`
  - The ZooKeeper ensemble connection string this broker uses for coordination (covered in section 8 above).
  - No default — must be explicitly set.

- `num.partitions`
  - Default partition count applied to a topic that gets **auto-created** (no explicit `--partitions` given).
  - Apache Kafka default: `1`.

- `log.retention.hours` / `log.retention.bytes`
  - How long / how much data is kept before old log segments are eligible for deletion, cluster-wide default.
  - Apache Kafka default for `log.retention.hours`: `168` (7 days).
  - `log.retention.bytes` is a size-based cap; by default it's disabled (`-1`, meaning "no size limit," time-based retention governs instead).

- `default.replication.factor`
  - Replication factor applied to a topic that gets auto-created without an explicit `--replication-factor`.
  - Apache Kafka default: `1` — meaning **no redundancy** unless explicitly raised. This is a value production clusters virtually always override upward (e.g. to `3`).

- `min.insync.replicas`
  - The minimum number of in-sync replicas that must acknowledge a write when a producer uses `acks=all`, or the write is rejected.
  - Apache Kafka default: `1`.
  - This setting is what actually gives `acks=all` teeth — without raising it above `1`, `acks=all` can still be satisfied by a single replica.

- `auto.create.topics.enable`
  - Whether the broker is allowed to auto-create a topic the first time a producer/consumer references a topic name that doesn't exist yet.
  - Apache Kafka default: `true`.
  - Many production environments deliberately set this to `false`, to force topics to be created intentionally (with the right partition count / replication factor) rather than accidentally, via typo, with whatever the broker defaults happen to be.

```text
broker.id                      → who am I
listeners / advertised.listeners → how do clients reach me   (own lesson, later)
log.dirs                       → where do I store data
zookeeper.connect              → how do I coordinate (this lab, ZK mode)
num.partitions                 → default partitions for auto-created topics
log.retention.hours/bytes      → default retention
default.replication.factor     → default redundancy for auto-created topics
min.insync.replicas            → durability floor for acks=all
auto.create.topics.enable      → allow accidental topic creation?
```

- Sources for the default values above: Apache Kafka's official Broker Configs documentation.

---

# 11. Where This File Lives — The Convention for This Lab

- Lesson-3 installed ZooKeeper to `/opt/zookeeper` on each of `zk1`/`zk2`/`zk3` (`/opt/zookeeper/conf/zoo.cfg`, systemd-managed).
- This series follows the same convention for Kafka: once installed, the broker configuration file lives at:

```text
/opt/kafka/config/server.properties
```

- Every hands-on file from `03` onward in this Lesson-4 series assumes this exact path.
- This is a **lab convention**, not a Kafka requirement — Kafka doesn't care where you unpack it, but keeping it consistent with Lesson-3's `/opt/<component>` layout keeps the two ensembles easy to reason about side by side on the same hosts/AMI pattern.

---

# 12. How Configuration Changes Actually Take Effect

- Two fundamentally different mechanisms, and knowing which applies to which setting matters operationally.

- **Static configuration**
  - Edit `/opt/kafka/config/server.properties` directly.
  - Restart the broker process for the change to be picked up.
  - Applies to most broker-level settings — things tied to how the process starts up (`log.dirs`, `listeners`, `broker.id`, and others).
  - Cost: a rolling restart, one broker at a time, to avoid an availability gap.

- **Dynamic configuration**
  - Changed at runtime using `kafka-configs.sh --alter`, without restarting the broker.
  - Applies to a defined subset of settings that Kafka supports changing live.
  - No restart, so no availability gap for that change.
  - The actual command syntax, scope (`--entity-type brokers` vs. `--entity-type topics`, etc.), and walkthrough belong to a dedicated later lesson ("Hands-On: How to change a Kafka Broker Configuration") — not repeated here.

```text
Config change needed
        │
        ├── Is it a dynamic-eligible setting?
        │        │
        │        ├── Yes → kafka-configs.sh --alter   (no restart)
        │        └── No  → edit server.properties + restart broker
        │
        └── Topic-level config → always changeable via kafka-topics.sh /
                                  kafka-configs.sh, no broker restart needed,
                                  since it's per-topic metadata, not process state
```

---

# 13. Case Study — "FinPay" Payments Platform

- A concrete scenario tying every layer together.

- **Setup**
  - FinPay runs Kafka 3.9.2 on 3 brokers, coordinated by the ZK1/ZK2/ZK3 ensemble from Lesson-3.
  - Broker-level defaults in `server.properties` on every broker:
    ```text
    log.retention.hours=168          (7 days)
    default.replication.factor=1
    min.insync.replicas=1
    auto.create.topics.enable=false
    ```
  - FinPay deliberately set `auto.create.topics.enable=false` — every topic must be created intentionally.

- **Topic layer — two topics, two very different needs**
  - `card-swipe-events` (high volume, low individual value):
    ```text
    partitions = 12
    replication.factor = 3
    retention.ms = 604800000     (7 days — inherits broker default)
    min.insync.replicas = 2      (topic-level override)
    ```
  - `settlement-transactions` (low volume, must never be lost, audited):
    ```text
    partitions = 6
    replication.factor = 3
    retention.ms = 94608000000   (3 years — overrides the 7-day broker default)
    min.insync.replicas = 3      (topic-level override)
    ```

- **Producer layer — two different producing services**
  - The card-swipe ingestion service (high throughput, some loss tolerable):
    ```text
    acks=1
    linger.ms=10
    compression.type=lz4
    ```
  - The settlement service (financial correctness matters more than speed):
    ```text
    acks=all
    linger.ms=0
    compression.type=none
    ```
  - Because `settlement-transactions` has `min.insync.replicas=3` **and** the producer uses `acks=all`, a write is only acknowledged once all 3 replicas have it — the topic-level and producer-level settings reinforce each other.
  - The card-swipe producer's `acks=1` on a topic with `min.insync.replicas=2` means the producer isn't even asking for the durability the topic *could* provide — a deliberate throughput trade-off FinPay accepted for this data.

- **Consumer layer**
  - The fraud-detection consumer group reading `card-swipe-events` uses `auto.offset.reset=latest` — it only cares about new events, not historical backfill.
  - The regulatory-reporting consumer group reading `settlement-transactions` uses `auto.offset.reset=earliest` with manual offset commits — it must be able to replay the full 3-year retained history without ever silently skipping a record.

- **What this demonstrates**
  - One cluster, one set of broker defaults.
  - Two topics overriding those defaults completely differently, based on business need.
  - Four different client configurations (2 producers, 2 consumer groups), each shaping behavior on top of the topic they're using.
  - None of these layers were edited in the same place, by the same person, or at the same time — and that's the point.

---

# 14. Common Misconception to Avoid

- The mistake:
  - "I set `min.insync.replicas=3` at the broker level, so every topic is now safe."
- Why it's wrong:
  - `min.insync.replicas` set in `server.properties` only becomes the **default** for topics that don't override it.
  - It has **no effect at all** unless producers actually use `acks=all` — a producer using `acks=1` bypasses the guarantee regardless of what the broker or topic demands.
  - A topic can still override the broker default downward (e.g. `min.insync.replicas=1`) unless that's separately locked down.
- The correct mental model:

```text
Durability actually achieved
        =
min(  broker/topic-level min.insync.replicas ,
      what the producer's acks setting actually requests  )
```

- All three layers have to line up — broker/topic sets the floor, the producer has to actually ask for it.

---

# 15. Senior Platform Engineer Mental Model

```text
                      Kafka Configuration
                              │
        ┌───────────┬─────────┼─────────┬───────────┐
        │           │         │         │           │
    Broker       Topic     Producer  Consumer    ZooKeeper
   (defaults)  (overrides) (shapes    (shapes    (coordination,
        │           │      writes)    reads)      this lab only)
        │           │
        ├─ static (restart)      ├─ retention.ms
        └─ some dynamic          ├─ cleanup.policy
           (kafka-configs.sh)    └─ min.insync.replicas
```

- One-line summary:

> **Kafka configuration isn't one file — it's broker defaults, topic overrides, and independent client-side settings, each answering a different question, that must be reasoned about together, not in isolation.**

---

# 16. Interview Answer

If asked:

> **"How does Kafka configuration work — isn't it all just `server.properties`?"**

A weak answer:

> "You edit `server.properties` and restart the broker. That's Kafka configuration."

A strong Senior Platform Engineer answer:

> "Kafka configuration is layered, not singular. The broker's `server.properties` sets cluster-wide defaults — things like default retention, default replication factor, and default partition count for auto-created topics. Topic-level configuration can override specific broker defaults for an individual topic, like giving one audit topic much longer retention than everything else. On top of that, producer and consumer configuration is entirely client-side — things like `acks`, `batch.size`, `group.id`, or `auto.offset.reset` are set by the application, not the broker, and the broker can't force a client to choose stronger durability than it asks for. And in a ZooKeeper-based cluster like the one I'm running on Kafka 3.9.2, there's a separate coordination-layer configuration — `zookeeper.connect` and related settings — that's distinct from all of the above. I also distinguish static configuration, which needs a broker restart, from dynamic configuration, which can be changed at runtime with `kafka-configs.sh --alter` with no downtime. Reasoning about durability, for example, means looking at the topic's `min.insync.replicas` and the producer's `acks` together — neither one alone tells you the real guarantee."

That answer demonstrates the layered model — default, override, shape — rather than treating Kafka configuration as one flat file.
