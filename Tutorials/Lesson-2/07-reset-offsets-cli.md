# Kafka Consumer Groups — Resetting Offsets

The CLI used for resetting consumer-group offsets is:

```bash
kafka-consumer-groups.sh
```

The important option is:

```bash
--reset-offsets
```

The basic idea:

> **Resetting offsets does NOT delete Kafka messages. It changes the consumer group's position so that the group will read from a different point in the partition.**

---

# 1. Why Do We Reset Offsets?

Suppose your topic has:

```text
orders-P0

Offset
  0 → Order-1001
  1 → Order-1002
  2 → Order-1003
  3 → Order-1004
  4 → Order-1005
  5 → Order-1006
```

Your consumer group currently has:

```text
order-service
Current offset = 5
```

You realize:

> "We need to process the old orders again."

Instead of producing the messages again, you can move the consumer group's offset backwards.

For example:

```text
Before:

0  1  2  3  4  5
               ↑
          Group position
```

Reset to offset `2`:

```text
0  1  2  3  4  5
      ↑
  New position
```

The group can then consume from that position.

---

# 2. Very Important — Resetting ≠ Deleting

This is one of the most important things to remember.

### Reset offset:

```text
Consumer position changes
        ↓
Messages remain in Kafka
```

### Delete messages:

```text
Kafka retention/deletion
        ↓
Records eventually disappear
```

So:

```text
Reset offset
   ❌ does NOT delete records
   ❌ does NOT modify records
   ✅ changes consumer group's position
```

---

# 3. Architecture

```text
                    Kafka Topic
                        │
                        ▼
                     orders
                        │
             ┌──────────┼──────────┐
             ▼          ▼          ▼
            P0         P1         P2
             │          │          │
             ▼          ▼          ▼
        Offset log  Offset log  Offset log
             │
             ▼
       Consumer Group
       "order-service"
             │
             ▼
       Committed Offset
             │
             ▼
    kafka-consumer-groups.sh
             │
             │ --reset-offsets
             ▼
       New Group Position
```

---

# 4. Important Safety Rule 🔥

Before resetting offsets, the consumer group should be **inactive**.

In simple terms:

```text
Consumers running
       ↓
Stop consumers
       ↓
Reset offsets
       ↓
Start consumers again
```

Why?

You don't want active consumers processing while you're changing their positions.

---

# 5. First — Check Current Offset

Before doing anything:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

Example:

```text
GROUP          TOPIC    PARTITION   CURRENT-OFFSET   LOG-END-OFFSET   LAG
order-service  orders   0           500              600              100
order-service  orders   1           450              500               50
order-service  orders   2           600              600                0
```

Now you know where the group currently is.

---

# 6. Reset to Beginning

Suppose you want the group to consume available records from the beginning.

Use:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders \
  --reset-offsets \
  --to-earliest
```

This means:

```text
Current position
       ↓
Move to earliest available offset
```

For example:

```text
Before:

0  1  2  3  4  5  6
                  ↑
              Group position


After --to-earliest:

0  1  2  3  4  5  6
↑
New position
```

---

# 7. Very Important: `--reset-offsets` Does NOT Immediately Apply the Change

This is an important safety feature.

The command without `--execute` gives you the proposed reset.

For example:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders \
  --reset-offsets \
  --to-earliest
```

Think:

```text
--reset-offsets
      ↓
"Show me what would happen."
```

Then, when you're sure, use:

```text
--execute
```

---

# 8. Actually Execute the Reset

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --execute
```

Now:

```text
Current offset
      ↓
Reset
      ↓
Earliest available offset
```

---

# 9. Reset to Latest

You can also move the group's position to the latest available offset:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders \
  --reset-offsets \
  --to-latest \
  --execute
```

Conceptually:

```text
0  1  2  3  4  5  6
                  ↑
               latest
```

This effectively tells the group:

> "Start consuming from the current end rather than replaying the existing backlog."

---

# 10. Reset to a Specific Offset

Suppose:

```text
orders-P0

0
1
2
3
4
5
6
7
```

You want the group to start from offset `3`.

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders:3 \
  --reset-offsets \
  --to-offset 3 \
  --execute
```

Conceptually:

```text
0  1  2  3  4  5  6  7
         ↑
      new position
```

---

# 11. Reset Only a Specific Partition

Suppose:

```text
orders

P0
P1
P2
```

You only want to reset P1.

You can specify:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders:1 \
  --reset-offsets \
  --to-earliest \
  --execute
```

Conceptually:

```text
P0 → unchanged

P1 → reset to earliest

P2 → unchanged
```

This is useful when only one partition has a problem.

---

# 12. Reset by Shifting the Offset

You can move the current offset by a specific amount.

For example:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders \
  --reset-offsets \
  --shift-by -100 \
  --execute
```

Meaning:

```text
Current offset = 500

500 - 100
   ↓
New offset = 400
```

You moved the group **100 records backwards**.

---

# 13. Real Production Case Study

Imagine:

```text
orders
│
├── P0
├── P1
└── P2
```

Your application accidentally deployed bad processing logic.

It processed:

```text
P0 → offsets 100–200
```

but you need to process those events again after fixing the application.

Current state:

```text
order-service
P0 → offset 200
```

You stop the consumers.

Then reset P0:

```text
200
 ↓
100
```

Start the fixed application again.

Now:

```text
P0

100
101
102
...
200
```

can be processed again.

### The key idea:

```text
Bad processing
      ↓
Fix application
      ↓
Reset consumer-group offset
      ↓
Replay existing Kafka records
```

This is one of the most useful operational capabilities of Kafka.

---

# 14. Another Real Case — Start Fresh

Suppose a new application is joining:

```text
analytics-service
```

You don't want it to process the historical backlog.

You want it to consume only future records.

You could reset its group to latest:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group analytics-service \
  --topic orders \
  --reset-offsets \
  --to-latest \
  --execute
```

Conceptually:

```text
Existing records
─────────────────────●
                     ↑
                  latest

New records
                     │
                     ▼
                  consume
```

---

# 15. Reset to Earliest vs Latest

| Option          | Meaning                              |
| --------------- | ------------------------------------ |
| `--to-earliest` | Start from earliest available offset |
| `--to-latest`   | Move to latest position              |
| `--to-offset N` | Move to specific offset              |
| `--shift-by N`  | Move relative to current position    |

Example:

```text
Current = 500
```

### Earliest

```text
500 → earliest
```

### Latest

```text
500 → latest
```

### Specific

```text
500 → 300
```

using:

```text
--to-offset 300
```

### Relative

```text
500 → 400
```

using:

```text
--shift-by -100
```

---

# 16. Resetting Offsets Does Not Mean "Replay Immediately"

This distinction is important.

```text
Reset offset
     ↓
Group's position changes
     ↓
Consumer starts/continues
     ↓
Consumer reads records from new position
```

So:

```text
Reset ≠ Replay by itself
```

It's better to think:

> **Reset the position, then the consumer can replay from that position.**

---

# 17. Verify After Reset

After executing the reset:

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

Check:

```text
CURRENT-OFFSET
```

Example:

```text
Before:

CURRENT-OFFSET = 500

After reset:

CURRENT-OFFSET = 100
```

Now the group has moved backwards.

---

# 18. Recommended Operational Workflow

For production, remember this workflow:

```text
        Consumer Group Problem
                 │
                 ▼
        Stop consumers
                 │
                 ▼
        Check current offsets
                 │
                 ▼
        Decide reset strategy
                 │
        ┌────────┼───────────┐
        ▼        ▼           ▼
    earliest   latest     specific
        │        │           │
        └────────┼───────────┘
                 ▼
        Run reset WITHOUT
             --execute
                 │
                 ▼
          Review proposed
             offsets
                 │
                 ▼
             --execute
                 │
                 ▼
        Start consumers
                 │
                 ▼
        Verify consumption
```

---

# 19. Commands to Keep in Your Notes

### Check current position

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group order-service
```

### Preview earliest reset

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders \
  --reset-offsets \
  --to-earliest
```

### Execute earliest reset

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders \
  --reset-offsets \
  --to-earliest \
  --execute
```

### Reset to latest

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders \
  --reset-offsets \
  --to-latest \
  --execute
```

### Reset to a specific offset

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders:0 \
  --reset-offsets \
  --to-offset 100 \
  --execute
```

### Shift backwards

```bash
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group order-service \
  --topic orders \
  --reset-offsets \
  --shift-by -100 \
  --execute
```

---

# 🔥 Final Mental Model

```text
                    Kafka Topic
                        │
                        ▼
                     Partition
                        │
              0  1  2  3  4  5  6
                        │
                        │
                 Consumer Group
                        │
                        ▼
                Current Offset
                       4
                        │
                 --reset-offsets
                        │
             ┌──────────┼──────────┐
             ▼          ▼          ▼
        earliest      latest    offset N
             │          │          │
             └──────────┼──────────┘
                        ▼
                  New Position
                        │
                        ▼
                   Consumer
                        │
                        ▼
                  Reads again
```

### 🔥 Three things to remember

1. **Resetting offsets changes the consumer group's position — it does not delete Kafka records.**
2. **Use the command without `--execute` first to preview the reset, then use `--execute` to apply it.**
3. **Stop/inactivate the consumer group before resetting offsets, then restart consumers and verify the new position.**

**Platform Engineer interview answer:**

> "Kafka consumer-group offset reset allows us to move a consumer group's committed position to an earlier, later, or specific offset without modifying the underlying Kafka records. It's commonly used for controlled message replay or skipping existing backlog. Operationally, I would stop the consumers, preview the reset, execute it, restart the consumers, and verify the resulting offsets and lag."
