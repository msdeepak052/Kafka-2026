# ZooKeeper Quorum Sizing

## 1. What is Quorum?

A **quorum = majority of ZooKeeper nodes** required to keep the ensemble operational.

Formula:

```text
Quorum = floor(N/2) + 1
```

---

## 2. Common Sizing

| ZooKeeper Nodes | Quorum | Failures Tolerated |
| --------------: | -----: | -----------------: |
|               1 |      1 |                0 ❌ |
|               2 |      2 |                0 ❌ |
|           **3** |  **2** |            **1** ✅ |
|           **5** |  **3** |            **2** ✅ |
|           **7** |  **4** |            **3** ✅ |

### Easy rule

```text
3 ZK → tolerate 1 failure
5 ZK → tolerate 2 failures
7 ZK → tolerate 3 failures
```

---

## 3. Why Odd Numbers?

Because adding an even node often doesn't improve failure tolerance.

Example:

```text
3 nodes → quorum 2 → tolerate 1 failure
4 nodes → quorum 3 → tolerate 1 failure
```

So **3 is better than 4** for most deployments.

Similarly:

```text
5 → quorum 3 → tolerate 2
6 → quorum 4 → tolerate 2
```

---

<img width="1608" height="913" alt="image" src="https://github.com/user-attachments/assets/93d5633e-f350-404a-8533-1fa1fcc810ce" />

<img width="1608" height="913" alt="image" src="https://github.com/user-attachments/assets/77d9aba9-d980-4a82-b64b-1387b52bd416" />

## 4. Real Production Example

### 3-node ensemble

```text
       ZooKeeper
    ┌────┼────┐
   ZK1  ZK2  ZK3
```

If ZK1 fails:

```text
ZK1 ❌
ZK2 ✅
ZK3 ✅

2/3 → QUORUM ✅
```

ZooKeeper continues operating.

If two fail:

```text
ZK1 ❌
ZK2 ❌
ZK3 ✅

1/3 → NO QUORUM ❌
```

ZooKeeper cannot safely process quorum-dependent updates.

---

## 5. Why Not Just Use 1?

```text
ZK1 ❌
   ↓
No ZooKeeper
   ↓
Kafka coordination affected
```

It's a **single point of failure**.

---

## 6. Why Not 10?

More nodes don't automatically mean better.

More nodes mean:

* More infrastructure
* More network communication
* More coordination overhead
* Higher operational complexity

For most Kafka deployments:

```text
3 nodes → common
5 nodes → larger/critical environments
```

---

## 7. Senior Platform Engineer Rule

Think:

```text
           ZooKeeper Ensemble
                  │
        ┌─────────┴─────────┐
        ▼                   ▼
   Majority exists      Majority lost
        │                   │
        ▼                   ▼
   Keep operating       Stop safely
```

### Remember:

> **ZooKeeper needs a majority, not all nodes. Use odd-sized ensembles—typically 3 or 5. The quorum determines how many ZooKeeper failures the cluster can tolerate while maintaining consistency.**
