# Hands-On: Changing Kafka Broker Configuration (Static & Dynamic) + Advanced Configuration

<img width="1760" height="970" alt="image" src="https://github.com/user-attachments/assets/e44f2b8f-e6ca-4d87-8763-91a4d1c0916d" />


> **Course items covered:** "43. Hands-On: How to change a Kafka Broker Configuration" and "44. Advanced Kafka Configuration."

For a **Senior Platform Engineer**, don't think of "changing a broker config" as:

> "SSH in, edit a file, restart."

Think of it as:

> **Choosing the cheapest mechanism that actually applies to this setting, then executing that change across a live cluster without ever dropping below the failure tolerance you designed for.**

---

# 1. What This File Delivers On

- File `02` (`Kafka-Configurations.md`) introduced static vs. dynamic configuration as a **concept**, and explicitly deferred the real workflow:
  - *"The actual command syntax and workflow is covered later in its own dedicated hands-on lesson ('Hands-On: How to change a Kafka Broker Configuration')."*
- This is that file. Three things happen here, in order:

```text
Part 1 → Static config change   (edit file, restart, rolling-restart the cluster)
Part 2 → Dynamic config change  (kafka-configs.sh --alter, no restart)
Part 3 → Advanced configuration (conceptual tour — item 44)
```

- Everything here runs against the real 3-broker Kafka 3.9.2 cluster built in earlier Lesson-4 files: `kf1` / `kf2` / `kf3`, `/opt/kafka/config/server.properties`, systemd unit `kafka.service`, security group `kafka-sg`, coordinating with the `zk1`/`zk2`/`zk3` ZooKeeper ensemble from Lesson-3, dual listeners on `9092` (internal) / `9094` (external).

---

# 2. Recap — Static vs. Dynamic, With the Vocabulary That Actually Matters

- Apache Kafka's own **Broker Configs** documentation gives every single broker setting a **`Dynamic Update Mode`** value. There are exactly three:
  - **`read-only`** — requires a broker restart to take effect. This is what file `02` called "static."
  - **`per-broker`** — may be updated dynamically, scoped to one specific broker only.
  - **`cluster-wide`** — may be updated dynamically as a default applied to every broker in the cluster (and can also be overridden per-broker, for testing).
- This is not a vague heuristic — it's a literal, documented property of every config name. You don't have to guess whether a setting supports dynamic update; you look it up.

```text
Every broker setting has exactly one Dynamic Update Mode:

read-only     → edit server.properties + restart broker           (Part 1)
per-broker    → kafka-configs.sh --alter --entity-type brokers
                --entity-name <id>                                 (Part 2)
cluster-wide  → kafka-configs.sh --alter --entity-type brokers
                --entity-default   (or --entity-name for testing)  (Part 2)
```

- Source: Apache Kafka 3.9 Broker Configs documentation (`kafka.apache.org/39/configuration/broker-configs/`), "Updating Broker Configs" section.

---

# 3. Environment Recap — This Cluster, Right Now

```text
                     ZooKeeper ensemble (Lesson-3)
                     zk1 / zk2 / zk3  :2181

                              │
                              ▼
                     Kafka cluster (Lesson-4)
        ┌─────────────────────┼─────────────────────┐
        │                     │                     │
       kf1                   kf2                   kf3
   broker.id=1           broker.id=2           broker.id=3
   :9092 internal        :9092 internal        :9092 internal
   :9094 external        :9094 external        :9094 external

   /opt/kafka/config/server.properties   (same path, all 3 nodes)
   systemd unit: kafka.service
   SG: kafka-sg
```

- Everything below assumes you can already `ssh` into `kf1`, `kf2`, `kf3`, and that `kafka-topics.sh` / `kafka-configs.sh` live under `/opt/kafka/bin/` on each host (and, for admin convenience, on whichever host/laptop you're driving the CLI from).
- All CLI examples below use the **internal** listener (`:9092`) as the bootstrap server, since we're running these commands from inside the cluster's own network.

---

# 4. Part 1 — Static Configuration Change (Item 43a)

- Goal: change **`log.retention.hours`** from the Kafka default (`168`, i.e. 7 days) to **`72`** (3 days), across the whole cluster, the correct operational way.

## Picking the Setting: Why `log.retention.hours`

- It's a broker-level **default** that shapes every topic that doesn't explicitly override retention — a realistic, common production change (e.g. cost control on disk).
- Critically for this lesson: per Kafka's own Broker Configs table, `log.retention.hours` is documented with **`Update Mode: read-only`** — it genuinely cannot be changed without a restart. That makes it an honest static example, not a contrived one.
  - (There's a closely related setting, `log.retention.ms`, that *is* dynamic — cluster-wide. That distinction becomes the whole point of Part 2, so hold that thought.)

```text
log.retention.hours    → Update Mode: read-only     (this section)
log.retention.minutes  → Update Mode: read-only
log.retention.ms       → Update Mode: cluster-wide   (Part 2)
log.retention.bytes    → Update Mode: cluster-wide   (Part 2)
```

- Yes, this looks inconsistent at first glance — three configs that all mean "how long to keep data" don't all share the same update mode. That's real, documented Kafka behavior, not a typo in this lesson. Knowing this distinction is exactly the kind of detail a Platform Engineer is expected to catch before promising a team "no downtime" for a config change.

## Step 1 — SSH Into `kf1` and Inspect the Current Config

```bash
ssh -i kafka-lab.pem ubuntu@<kf1-public-ip>
```

Check what's actually in the file today:

```bash
grep -i "log.retention" /opt/kafka/config/server.properties
```

Expected (this setting was never explicitly set in earlier Lesson-4 files, so the broker is running on Kafka's built-in default):

```text
(no output — log.retention.hours is not present, so the compiled-in default of 168 applies)
```

Confirm that default is really in effect right now, from any admin host:

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --describe --all --entity-type brokers --entity-name 1 \
  | grep -i "log.retention.hours"
```

Expected:

```text
  log.retention.hours=168 sensitive=false synonyms={DEFAULT_CONFIG:log.retention.hours=168}
```

- `--all` (not just `--describe`) matters here — by default `kafka-configs.sh --describe` only shows settings that have a **dynamic override**. Since nothing has been dynamically changed yet, a plain `--describe` would print nothing at all. `--all` also prints settings sitting at their static/default value, with a `synonyms={...}` trail showing exactly where the effective value came from (`DEFAULT_CONFIG` here — meaning "nobody set this, it's the compiled-in Kafka default").

## Step 2 — Edit `server.properties` on `kf1`

```bash
sudo nano /opt/kafka/config/server.properties
```

Add:

```properties
log.retention.hours=72
```

Save and exit.

## Step 3 — Restart `kf1` and Confirm It Rejoined

```bash
sudo systemctl restart kafka
```

```bash
sudo systemctl status kafka
```

Expected:

```text
Active: active (running)
```

```bash
journalctl -u kafka --no-pager -n 30 | grep -i "started"
```

Expected (one of the last lines):

```text
kf1 kafka[1234]: [KafkaServer id=1] started (kafka.server.KafkaServer)
```

Confirm the value actually took: rerun the same check as Step 1.

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --describe --all --entity-type brokers --entity-name 1 \
  | grep -i "log.retention.hours"
```

Expected now:

```text
  log.retention.hours=72 sensitive=false synonyms={STATIC_BROKER_CONFIG:log.retention.hours=72}
```

- The synonym source changed from `DEFAULT_CONFIG` to `STATIC_BROKER_CONFIG` — that's Kafka telling you exactly where the value is coming from: your `server.properties`, not the built-in default.

## Step 4 — The Important Trap: One Broker ≠ The Cluster

- **`kf1` alone now has `log.retention.hours=72`. `kf2` and `kf3` still have `168`.**
- This isn't a partial rollout in any meaningful sense — it's an **inconsistent cluster**. Which value actually governs a given topic's default retention depends on which broker happens to be the one you asked, or (worse) can vary depending on internal broker-default resolution paths.
- Verify the inconsistency directly:

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf2:9092 \
  --describe --all --entity-type brokers --entity-name 2 \
  | grep -i "log.retention.hours"
```

```text
  log.retention.hours=168 sensitive=false synonyms={DEFAULT_CONFIG:log.retention.hours=168}
```

```text
kf1 (broker.id=1) → log.retention.hours=72    ← changed
kf2 (broker.id=2) → log.retention.hours=168   ← NOT changed yet
kf3 (broker.id=3) → log.retention.hours=168   ← NOT changed yet
```

- A static config change is **not "done" until every broker in the cluster has been restarted with the new value.** This is the operational point the course item is actually testing.

## Step 5 — Rolling Restart: `kf2`, Then `kf3`

- The procedure is identical to Step 2/3, repeated per-broker, **one at a time, with a health check in between**:

```text
kf1  →  edit + restart  →  verify healthy  ┐
                                            │
kf2  →  edit + restart  →  verify healthy  ┤  never overlap
                                            │
kf3  →  edit + restart  →  verify healthy  ┘
```

On `kf2`:

```bash
ssh -i kafka-lab.pem ubuntu@<kf2-public-ip>
sudo nano /opt/kafka/config/server.properties
```

```properties
log.retention.hours=72
```

```bash
sudo systemctl restart kafka
sudo systemctl status kafka
```

**Before moving to `kf3`, verify the cluster is healthy** (from any admin host):

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kf1:9092,kf2:9092,kf3:9092 \
  --describe --under-replicated-partitions
```

Expected — **empty output**:

```text
(no output)
```

- Empty output means no partition currently has fewer in-sync replicas than it's supposed to — i.e., `kf2` fully rejoined the ISR (in-sync replica set) for every partition it hosts a replica of, before you touch `kf3`.

Only then repeat on `kf3`:

```bash
ssh -i kafka-lab.pem ubuntu@<kf3-public-ip>
sudo nano /opt/kafka/config/server.properties
```

```properties
log.retention.hours=72
```

```bash
sudo systemctl restart kafka
sudo systemctl status kafka
```

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kf1:9092,kf2:9092,kf3:9092 \
  --describe --under-replicated-partitions
```

```text
(no output)
```

## Step 6 — Why Never Two At Once

- This directly extends the RF/failure-tolerance math from `Lesson-4/01-Kafka-Cluster-Size.md`:

```text
RF = 3, 3 brokers
        ↓
Cluster tolerates exactly 1 broker being unavailable at a time
```

- **Restarting one broker at a time is, operationally, identical to the "tolerate 1 broker failure" scenario that RF=3 was built for.** While `kf2` is restarting, every partition that has a replica on `kf2` still has its other 2 replicas (on whichever of `kf1`/`kf3` hold them) available — reads and writes continue.
- **Restarting two brokers at the same time is not covered by that math at all:**

```text
RF = 3 → tolerates 1 broker down
                ↓
Restart kf2 AND kf3 simultaneously
                ↓
Effectively 2 of 3 brokers unavailable at once
                ↓
For ANY partition whose replicas happen to live on exactly
{kf2, kf3} + one more → only 1 of 3 replicas remains online
                ↓
- If that 1 survivor isn't in-sync, no eligible leader exists
  → partition unavailable
- Any producer requiring more in-sync replicas than are left
  (acks=all with min.insync.replicas > 1) gets rejected
```

- This is exactly why the rolling-restart sequence above has an explicit health-check gate (`--under-replicated-partitions` returning empty) between every single broker restart — that gate is what proves it's now safe to take the *next* broker down. Skipping the gate and restarting `kf2` and `kf3` back-to-back without waiting is functionally the same mistake as manually triggering a 2-of-3 failure.

## Step 7 — Verify the Change Took Effect Cluster-Wide

Confirm all three brokers now agree:

```bash
for b in kf1:9092:1 kf2:9092:2 kf3:9092:3; do
  host=$(echo $b | cut -d: -f1); id=$(echo $b | cut -d: -f3)
  echo "== broker $id ($host) =="
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
    --describe --all --entity-type brokers --entity-name $id \
    | grep -i "log.retention.hours"
done
```

Expected:

```text
== broker 1 (kf1) ==
  log.retention.hours=72 sensitive=false synonyms={STATIC_BROKER_CONFIG:log.retention.hours=72}
== broker 2 (kf2) ==
  log.retention.hours=72 sensitive=false synonyms={STATIC_BROKER_CONFIG:log.retention.hours=72}
== broker 3 (kf3) ==
  log.retention.hours=72 sensitive=false synonyms={STATIC_BROKER_CONFIG:log.retention.hours=72}
```

Now prove it behaviorally, not just by reading config — create a topic **without** specifying retention explicitly, and check what it inherited:

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kf1:9092,kf2:9092,kf3:9092 \
  --create --topic verify-retention --partitions 3 --replication-factor 3
```

```text
Created topic verify-retention.
```

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --describe --all --entity-type topics --entity-name verify-retention \
  | grep -i "retention.ms"
```

Expected:

```text
  retention.ms=259200000 sensitive=false synonyms={STATIC_BROKER_CONFIG:log.retention.hours=72, DEFAULT_CONFIG:log.retention.ms=604800000}
```

- `259200000` ms = 72 hours × 3,600,000 ms/hour — the new default, correctly inherited by a brand-new topic, with the synonym trail proving it came from `kf1`'s `server.properties`, not the old 7-day default.

---

# 5. Part 2 — Dynamic Configuration Change (Item 43b)

- The whole reason Part 1 needed a 3-step rolling restart is that `log.retention.hours` happens to be `read-only`. Most settings a Platform Engineer touches day-to-day in a healthy cluster are not — they're `cluster-wide` or `per-broker`, meaning **zero restart, zero rolling-restart choreography, zero availability risk.**

## Which Settings Can Actually Go Dynamic

- Per Apache Kafka 3.9's Broker Configs documentation, some notable examples of each `Dynamic Update Mode`:

```text
read-only (static only — Part 1's world)
  broker.id
  log.dirs
  zookeeper.connect
  num.partitions
  auto.create.topics.enable
  auto.leader.rebalance.enable
  log.retention.hours / log.retention.minutes

per-broker (dynamic, scoped to one broker)
  advertised.listeners
  SSL keystore/truststore configs for a specific listener

cluster-wide (dynamic, applies to every broker by default)
  log.retention.ms
  log.retention.bytes
  min.insync.replicas
  unclean.leader.election.enable
  log.cleaner.threads / other log-cleaner thread configs
  num.network.threads, num.io.threads, num.replica.fetchers (thread pools)
  max.connections.per.ip
```

- Why `broker.id`, `log.dirs`, and `zookeeper.connect` are read-only makes intuitive sense once you think about it: they're all things the broker process needs **before** it can even start up and begin listening for admin requests — there's no running broker to send a dynamic-alter request to yet.

## Where Dynamic Configs Live, and Why That Matters

- Apache's documentation is explicit about precedence when a setting could come from more than one place:

```text
1. Dynamic per-broker config      (stored in ZooKeeper)
2. Dynamic cluster-wide default   (stored in ZooKeeper)
3. Static broker config           (server.properties)
4. Kafka's compiled-in default
```

- Two operationally important facts fall directly out of this:
  - **A dynamic override always wins over the static file value**, without you touching or even opening `server.properties`.
  - **Dynamic configs are stored in ZooKeeper, in this ZK-mode cluster** — not in broker memory, not in a local file. That means:
    - They **persist across a broker restart.** Restart `kf1` after setting a dynamic override, and the override is still there when it comes back — because it was never on `kf1`'s disk to begin with, it was in ZooKeeper.
    - They apply **cluster-wide, immediately**, for `cluster-wide`-scoped settings — every broker picks up the same value from ZooKeeper without any of them restarting.

```text
        ZooKeeper (zk1/zk2/zk3)
                │
     ┌──────────┼──────────┐
     │  dynamic broker configs
     │  (per-broker + cluster-wide defaults)
     └──────────┬──────────┘
                │
     ┌──────────┼──────────┐
     ▼          ▼          ▼
   kf1        kf2        kf3       ← all read the same ZK path,
                                       no restart needed, survives
                                       any individual broker restart
```

## Worked Example A — Broker-Level Dynamic Change (`log.retention.ms`, cluster-wide)

- Scenario: after Part 1, retention is `72` hours everywhere via the static file. Now say the requirement changes again — drop to `48` hours — and this time you want it live immediately, no rolling restart.
- Use `log.retention.ms` (the dynamic sibling of `log.retention.hours`), targeting `--entity-default` so it applies cluster-wide in one command:

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --entity-type brokers --entity-default \
  --alter --add-config log.retention.ms=172800000
```

Expected:

```text
Completed updating config for brokers in the cluster: [default].
```

`172800000` ms = 48 hours.

Describe it, on any broker — cluster-wide dynamic configs show up identically everywhere:

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --describe --all --entity-type brokers --entity-name 1 \
  | grep -i "log.retention"
```

Expected:

```text
  log.retention.ms=172800000 sensitive=false synonyms={DYNAMIC_DEFAULT_BROKER_CONFIG:log.retention.ms=172800000, STATIC_BROKER_CONFIG:log.retention.hours=72, DEFAULT_CONFIG:log.retention.ms=604800000}
  log.retention.hours=72 sensitive=false synonyms={STATIC_BROKER_CONFIG:log.retention.hours=72}
```

- Read the synonym trail on the first line carefully — it's precedence made visible: the dynamic override (`172800000`) wins, but it's stacked **on top of** the static `72`-hour value underneath it, which is stacked on top of the original `168`-hour default. **No file was edited. No broker restarted.** All three brokers are already serving `48`-hour retention as the new effective default the moment that one command finished.
- Undo it (revert to whatever's underneath — the static `72`-hour value, not the original Kafka default):

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --entity-type brokers --entity-default \
  --alter --delete-config log.retention.ms
```

```text
Completed updating config for brokers in the cluster: [default].
```

## Worked Example B — Per-Topic Dynamic Change (`retention.ms` override)

- Topic-level config changes are dynamic by nature — they're metadata about a topic, not broker process state, so there's never a restart involved for these, regardless of the broker-level `Update Mode` rules above.
- Scenario: a new topic, `clickstream-raw`, needs much shorter retention than the cluster default — 6 hours, high volume, no long-term value.

```bash
/opt/kafka/bin/kafka-topics.sh --bootstrap-server kf1:9092,kf2:9092,kf3:9092 \
  --create --topic clickstream-raw --partitions 6 --replication-factor 3
```

```text
Created topic clickstream-raw.
```

**Before** — describe it, it's just inheriting the broker/cluster default:

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --describe --all --entity-type topics --entity-name clickstream-raw \
  | grep -i "retention.ms"
```

```text
  retention.ms=259200000 sensitive=false synonyms={STATIC_BROKER_CONFIG:log.retention.hours=72, DEFAULT_CONFIG:log.retention.ms=604800000}
```

Now override it for this topic only:

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --entity-type topics --entity-name clickstream-raw \
  --alter --add-config retention.ms=21600000
```

```text
Completed updating config for topic clickstream-raw.
```

**After**:

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --describe --all --entity-type topics --entity-name clickstream-raw \
  | grep -i "retention.ms"
```

```text
  retention.ms=21600000 sensitive=false synonyms={DYNAMIC_TOPIC_CONFIG:retention.ms=21600000, STATIC_BROKER_CONFIG:log.retention.hours=72, DEFAULT_CONFIG:log.retention.ms=604800000}
```

- `21600000` ms = 6 hours. Every other topic on the cluster (including `verify-retention` from Part 1) is completely unaffected — this override is scoped to `clickstream-raw` alone.

## Worked Example C — Dynamic Logging Level (`broker-loggers`)

- A different dynamic entity type entirely — not a broker setting, not a topic setting, a **log4j logger level**, changeable live for troubleshooting without a restart (and without leaving verbose logging on permanently, since it's just as easy to revert).

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --entity-type broker-loggers --entity-name 1 \
  --describe | grep -i "kafka.controller.KafkaController"
```

```text
  kafka.controller.KafkaController=INFO sensitive=false synonyms={}
```

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --entity-type broker-loggers --entity-name 1 \
  --alter --add-config kafka.controller.KafkaController=DEBUG
```

```text
Completed updating config for broker: 1.
```

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --entity-type broker-loggers --entity-name 1 \
  --describe | grep -i "kafka.controller.KafkaController"
```

```text
  kafka.controller.KafkaController=DEBUG sensitive=false synonyms={}
```

- Available levels: `TRACE`, `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`. Remember to set it back to `INFO` once you're done troubleshooting — `DEBUG`/`TRACE` on a busy broker generates a lot of log volume.

## Static vs Dynamic — Comparison Table

| | **Static** | **Dynamic** |
|---|---|---|
| **How** | Edit `server.properties`, restart broker | `kafka-configs.sh --alter` |
| **Restart required?** | Yes, per broker | No |
| **Scope** | Whatever you set on that one file (per-broker by nature, since each broker has its own file) | `--entity-name <id>` = one broker; `--entity-default` = cluster-wide; `--entity-type topics --entity-name <topic>` = one topic |
| **Cluster-wide rollout effort** | Manual rolling restart, one broker at a time, with health checks between | One command (`--entity-default`), applied to every broker instantly |
| **Persists across restart?** | Yes — it's on disk in the file | Yes — stored in ZooKeeper in this ZK-mode cluster, survives any broker restart |
| **Applies to** | Every `read-only`-mode setting (`broker.id`, `log.dirs`, `zookeeper.connect`, `num.partitions`, `log.retention.hours`, ...) | Every `per-broker`/`cluster-wide`-mode setting (`log.retention.ms`, `min.insync.replicas`, `unclean.leader.election.enable`, topic-level configs, broker-loggers, ...) |
| **When to use** | The setting has no other choice (it's `read-only`), or you're doing an initial/major provisioning change anyway | Anything eligible — it's strictly lower-risk: no availability gap, no rolling-restart choreography to get wrong |

---

# 6. Part 3 — Advanced Kafka Configuration (Item 44)

- This section is intentionally conceptual — not a new hands-on walkthrough, but the set of configuration ideas a beginner course mentions once and moves past, that a Platform Engineer needs to actually reason about.

## `unclean.leader.election.enable` — Availability vs. Durability

- Default: **`false`**. Dynamic update mode: **`cluster-wide`** — and per Kafka's own docs, from Kafka 2.0.0 onward, flipping this dynamically takes effect automatically once the controller picks it up (no restart, no forced re-election needed).
- The scenario it governs:
  - A partition's leader replica dies. The remaining in-sync replicas (ISR) are eligible to become the new leader — no data loss, this is the normal case RF exists for.
  - But suppose **every** in-sync replica is also down, and only an **out-of-sync** replica (one that had fallen behind, missing some recent writes) is still alive.
  - With `unclean.leader.election.enable=false` (the default): Kafka refuses to elect that out-of-sync replica. The partition stays **unavailable** (no leader, no reads/writes) until an in-sync replica comes back.
  - With it set to `true`: Kafka **will** elect the out-of-sync replica as leader. The partition becomes available again immediately — but whatever writes existed only on the now-dead in-sync replicas are **permanently lost**, and the new leader may even overwrite/diverge from what those writes would have been.
- This is a direct, small-scale instance of the CAP theorem trade-off, worth stating plainly even without assuming prior exposure to it:
  - In a distributed system, when a network/node partition happens, you cannot have both perfect **Consistency** (every reader sees the same, fully correct data) and perfect **Availability** (every request gets *some* answer) at the same time — you have to pick which one degrades.
  - `unclean.leader.election.enable=false` picks **consistency/durability over availability**: refuse to serve rather than serve possibly-wrong or incomplete data.
  - `unclean.leader.election.enable=true` picks **availability over consistency/durability**: keep serving, accept that some already-acknowledged data may be gone.
- There's no universally correct answer — it's a business decision expressed as a config value:

```text
Payments / financial ledger topic   → false (default) — never silently lose committed data
Live metrics / clickstream topic    → true is sometimes acceptable — staying up matters more
                                        than a few seconds of lost events
```

## `min.insync.replicas` + `acks=all` — What the Guarantee Actually Is

- Default: **`1`**. Dynamic update mode: **`cluster-wide`** (also settable per-topic).
- The precise mechanics, not just the failure-demo angle:
  - `min.insync.replicas` is a **broker/topic-side floor**: "when a producer uses `acks=all`, refuse the write unless at least this many replicas (including the leader) are in-sync and have it."
  - `acks=all` is a **producer-side request**: "don't tell me the write succeeded until every current in-sync replica has it."
  - Neither setting alone determines the actual guarantee — it's the **combination**:

```text
Durability actually achieved
        =
min(  min.insync.replicas (broker/topic floor) ,
      what the producer's acks setting actually asks for  )
```

  - `min.insync.replicas=1` (the default) means `acks=all` can be satisfied by a **single** replica having the data — the same weak guarantee as `acks=1`, just phrased differently. This is the single most common Kafka durability misconfiguration: teams set `acks=all` and assume they're "safe," without ever raising `min.insync.replicas` above its default of `1`.
  - The classic durable setup: `replication.factor=3`, `min.insync.replicas=2`, producer `acks=all`. A write only succeeds once 2 of the 3 replicas confirm it — the cluster can lose any 1 broker and every acknowledged write is still present on at least 1 surviving replica.
- What it does **not** guarantee:
  - It says nothing about **consistency of reads** for consumers reading at different offsets/times — that's a separate concern.
  - It does not prevent data loss from **unclean leader election** — the two settings interact: if `unclean.leader.election.enable=true`, an out-of-sync replica (which by definition didn't satisfy `min.insync.replicas` at write time) can still become leader and the "protected" write is gone anyway. Setting `min.insync.replicas=2` and leaving `unclean.leader.election.enable=true` is a common way to accidentally undermine the durability you thought you configured.

## Log Compaction — `cleanup.policy=compact` vs `delete`

- `cleanup.policy` is a topic-level setting, default `delete`. The two modes solve fundamentally different problems:
  - **`delete`** (the default): segments are dropped once they age out (`retention.ms`) or the topic exceeds a size cap (`retention.bytes`). This is an **event log** — every message matters as a distinct historical fact; old ones eventually expire wholesale.
  - **`compact`**: Kafka guarantees to retain **at least the latest value for every key** forever (older values for the same key get removed by the background log-cleaner), but does not guarantee to retain every intermediate value or keep any time-based history.
- Concrete example, side by side:

```text
Topic: clickstream-raw          Topic: customer-current-address
cleanup.policy=delete           cleanup.policy=compact

key = user_id (irrelevant        key = customer_id
       to retention)              value = latest known address

Every click is a fact that       Only the LATEST address per
matters once, then ages out.     customer_id matters — old
                                  addresses are noise, not history.

Kept: last 6 hours of clicks     Kept: 1 row per customer_id,
(from Worked Example B above)    forever, however old it is
```

  - A compacted `customer-current-address` topic behaves like a durable, replayable key-value store: any consumer that reads it from the beginning ends up with exactly one current row per `customer_id`, without you ever writing "delete the old value" — the newer message for the same key naturally supersedes it after compaction runs.
  - Using `compact` on `clickstream-raw` would be wrong: clicks don't have a meaningful "key" whose latest value matters — you'd lose almost all the history you actually want.
  - Using `delete` on `customer-current-address` would be wrong the other way: a customer whose address hasn't changed in 8 months would silently lose their record once it aged past the retention window, even though it's still the correct, current value.

## Quotas — the Noisy-Neighbor Problem

- Real syntax (Apache Kafka 3.9 basic operations docs):

```bash
/opt/kafka/bin/kafka-configs.sh --bootstrap-server kf1:9092 \
  --alter --add-config 'producer_byte_rate=10485760,consumer_byte_rate=10485760' \
  --entity-type clients --entity-name ingest-service-a
```

```text
Completed updating config for entity: client-id 'ingest-service-a'.
```

- `10485760` bytes/sec = 10 MB/s — a per-client-id cap on how fast that client may produce/consume, enforced by the broker via response throttling once the client exceeds it.
- Why multi-tenant clusters need this — a case study:
  - Three teams share one Kafka cluster: `payments`, `notifications`, `ingest-team-x`.
  - `ingest-team-x` ships a buggy release: a retry loop with no backoff starts producing the same batch repeatedly at far above its normal rate.
  - Without quotas, that one misbehaving producer can consume enough of the shared **network bandwidth and disk I/O** on every broker that `payments`' completely unrelated, well-behaved traffic starts seeing elevated latency or timeouts — a classic **noisy neighbor**.
  - With a `producer_byte_rate` quota set on `ingest-team-x`'s client-id, the broker throttles *that client specifically* once it exceeds its allotted rate — its own producer slows down (via broker-injected delay in the produce response), but `payments` and `notifications` never notice anything happened.
- Quotas can be scoped by `--entity-type users`, `--entity-type clients`, or the combination of both (per-user-per-client), and set as a specific override (`--entity-name`) or a cluster-wide default (`--entity-default`) — same CLI shape as the broker/topic examples above.

## `auto.create.topics.enable=false` — Production Default, Not Lab Default

- Default: **`true`**. Dynamic update mode: **`read-only`** (this one genuinely needs a restart to flip — it governs behavior the broker decides very early in handling a metadata request).
- This lab has been running with the default (`true`) the whole time — every topic created across this series showed up because it was explicitly created with `kafka-topics.sh --create`, but `true` means Kafka *would* have silently created any topic name the moment a producer or consumer first referenced it, with no explicit creation step at all.
- Why production environments almost universally flip this to `false`:
  - A typo'd topic name in a producer's config (`order-events` vs `orders-events`) doesn't fail loudly — with auto-create on, Kafka just creates a brand-new, garbage topic on the spot and starts accepting data into it.
  - That auto-created topic gets whatever the broker's **current defaults** happen to be — `num.partitions` (default `1`!) and `default.replication.factor` (default `1` — **zero redundancy**) — almost certainly not the partition count or replication factor that topic would have needed if someone had created it on purpose.
  - The mistake is often invisible for a while: data is flowing, no errors, just quietly into the wrong, under-replicated, single-partition topic — discovered later, sometimes after data that should have gone to the real topic is unrecoverable.
- With `auto.create.topics.enable=false`, that same typo instead fails fast and loud — the producer/consumer gets an `UNKNOWN_TOPIC_OR_PARTITION` error immediately, someone notices, and the real topic gets created deliberately with the partition count and RF it actually needs.

---

# 7. Senior Platform Engineer Mental Model

```text
                    "I need to change a Kafka config"
                                  │
                                  ▼
                 Look up its Dynamic Update Mode
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼
         read-only            per-broker           cluster-wide
              │                   │                   │
     edit server.properties   kafka-configs.sh    kafka-configs.sh
     + rolling restart,       --entity-name <id>  --entity-default
     ONE broker at a time,        (dynamic)            (dynamic)
     health-check between
     every restart
              │
     tie to RF/failure-tolerance math:
     never take down more brokers
     at once than the cluster is
     designed to survive
```

- One-line summary:

> **A config change isn't "safe" or "risky" in the abstract — its Dynamic Update Mode tells you exactly which mechanism applies, and for the settings that still require a restart, the rolling-restart discipline is just the cluster's own RF/failure-tolerance math, applied on purpose instead of by accident.**

---

# 8. Interview Answer

If asked:

> **"How do you safely change a Kafka broker configuration in production?"**

A weak answer:

> "Edit `server.properties` and restart the broker."

A strong Senior Platform Engineer answer:

> "First I check the setting's Dynamic Update Mode in Kafka's Broker Configs documentation — it's either `read-only`, `per-broker`, or `cluster-wide`. If it's dynamic, I use `kafka-configs.sh --alter` — `--entity-name <id>` for one broker, `--entity-default` for a cluster-wide default — which takes effect immediately with zero restart and zero availability risk, and in a ZooKeeper-mode cluster those dynamic overrides are stored in ZooKeeper, so they persist across any later broker restart. If the setting is `read-only` — things like `broker.id`, `log.dirs`, or `zookeeper.connect`, which the broker needs before it can even start — I edit `server.properties` and do a rolling restart: one broker at a time, verifying `--under-replicated-partitions` comes back empty before touching the next one. I never restart more than the number of brokers my replication factor is designed to tolerate losing simultaneously — for an RF=3 cluster that's exactly one at a time, because restarting two at once is operationally the same as an unplanned 2-of-3 broker failure."

That answer demonstrates the actual decision tree — look up the update mode, then apply the mechanism that matches it — rather than treating every config change as a blanket "edit and restart" operation.

---

# 9. Hands-On Checklist

```text
Static configuration (Part 1)
□ Inspect current effective value with kafka-configs.sh --describe --all
□ Edit /opt/kafka/config/server.properties on kf1
□ Restart kf1, confirm systemctl status = active (running)
□ Confirm --under-replicated-partitions is empty before touching kf2
□ Repeat on kf2, then kf3 — never two brokers at once
□ Verify all 3 brokers agree via kafka-configs.sh --describe --all
□ Verify behaviorally: create a topic, confirm it inherits the new default

Dynamic configuration (Part 2)
□ Know the 3 Dynamic Update Mode values: read-only / per-broker / cluster-wide
□ Alter a cluster-wide broker default (log.retention.ms) with --entity-default
□ Alter a per-topic config (retention.ms) with --entity-type topics
□ Alter a broker-logger level with --entity-type broker-loggers
□ Read a synonyms={...} trail and explain the precedence it shows
□ Explain why dynamic overrides survive a broker restart in this ZK-mode cluster

Advanced configuration (Part 3 — conceptual)
□ Explain unclean.leader.election.enable as an availability/durability trade-off
□ Explain why acks=all alone doesn't guarantee durability without min.insync.replicas > 1
□ Explain when cleanup.policy=compact is correct vs. cleanup.policy=delete
□ Explain what a producer_byte_rate/consumer_byte_rate quota protects against
□ Explain why production clusters set auto.create.topics.enable=false
```
