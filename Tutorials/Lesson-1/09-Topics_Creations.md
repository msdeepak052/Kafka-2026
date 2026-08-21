# 9. Creating a New Kafka Topic — Who Adds It?

This is a very practical question, especially from a **Senior Platform Engineer perspective**.

We already know:

```text
Kafka Cluster
     ↓
Broker
     ↓
Topic
     ↓
Partitions
     ↓
Records
     ↓
Consumers / Consumer Groups
```

Now the question is:

> **Who actually creates a Topic in Kafka, and how?**

---

# 1. Who Creates a Kafka Topic?

A Kafka topic can generally be created in two ways:

### Option 1 — Explicitly created

A person/team creates it using Kafka administration tools.

For example:

```text
Platform Engineer
      ↓
Kafka CLI
      ↓
Kafka Cluster
      ↓
New Topic
```

### Option 2 — Automatically created

Depending on the Kafka configuration, Kafka can automatically create a topic when an application refers to a topic that doesn't exist.

```text
Application
     ↓
"orders" topic
     ↓
Topic doesn't exist
     ↓
Kafka may create it automatically
```

For **production environments**, explicit topic creation is generally preferred because you want control over the topic's configuration and partitioning.

---

# 2. Easy Analogy 🏢

Imagine your company has a large office.

Someone says:

> "We need a new department called Payments."

There are two possibilities.

### Controlled approach

You contact the facilities/admin team:

```text
Application Team
      ↓
Platform Team
      ↓
Create Payments Department
      ↓
Department ready
```

The Platform Team decides things such as:

* How much space?
* How many rooms?
* Which floor?
* What configuration?

This is similar to **explicit Kafka topic creation**.

---

### Automatic approach

Someone simply walks into the building and says:

> "Payments department."

And the building automatically creates it.

Convenient, but potentially dangerous if nobody controls:

* Size
* Configuration
* Naming
* Capacity

That's why automatic creation can be undesirable in a controlled production environment.

---

# 3. Explicitly Creating a Topic

Kafka provides the `kafka-topics.sh` CLI.

Example:

```bash
kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 3
```

You're telling Kafka:

```text
Create topic:
    Name              = orders
    Partitions        = 3
    Replication Factor = 3
```

---

# 4. What Happens?

Conceptually:

```text
              Platform Engineer
                     │
                     │ kafka-topics.sh
                     ▼
              Kafka Cluster
                     │
                     ▼
              Create "orders"
                     │
              ┌──────┼──────┐
              ▼      ▼      ▼
             P0     P1     P2
```

The topic now exists.

---

# 5. Who Normally Creates Topics in a Company?

This depends on the organization's operating model.

### Small environment

The application developer might create it:

```text
Developer
   ↓
Kafka CLI
   ↓
Topic
```

### Larger production environment

A common model is:

```text
Application Team
      │
      │ Request
      ▼
Platform / Kafka Team
      │
      │ Create / approve
      ▼
Kafka Cluster
```

For example:

```text
Application Team:

"We need topic payment-events"

        ↓

Platform Team:

Name?
Partitions?
Replication?
Retention requirements?
Ordering requirements?

        ↓

Create topic
```

This gives the platform team control over Kafka infrastructure.

---

# 6. Senior Platform Engineer Perspective

If a developer tells you:

> "Create a topic called `payments`."

You shouldn't immediately run:

```bash
kafka-topics.sh --create ...
```

You should first understand what they need.

At your **current learning stage**, focus on these things:

### Topic name

```text
payments
```

### Number of partitions

```text
3
```

### Replication factor

```text
3
```

These are the concepts you've already learned.

So you might receive:

```text
Topic:
payment-events

Partitions:
6

Replication Factor:
3
```

Then create it.

---

# 7. Why Shouldn't Everyone Randomly Create Topics?

Imagine 100 developers have access to Kafka.

They start creating:

```text
test1
test2
test3
mytopic
mytopic-new
mytopic-final
mytopic-final-v2
abc
xyz
payment
payments
payment-events
```

Soon the Kafka cluster becomes difficult to manage.

A platform team usually wants:

```text
                 Kafka Cluster
                      │
             Topic Creation Process
                      │
          ┌───────────┼───────────┐
          ▼           ▼           ▼
       Naming     Partitions   Replication
       Standard    Decision       Decision
```

This is why production Kafka environments usually have some form of **topic governance**.

---

# 8. Automatic Topic Creation

Kafka has a broker configuration related to automatic topic creation:

```properties
auto.create.topics.enable=true
```

If enabled, Kafka can automatically create a topic when a client references a topic that doesn't exist.

Conceptually:

```text
Application
     │
     │ "orders"
     ▼
Kafka
     │
     │ orders doesn't exist
     ▼
Create topic
```

---

# 9. Why Automatic Creation Can Be Dangerous

Suppose the developer makes a typo:

```text
Correct:
payment-events
```

But application uses:

```text
payment-event
```

If automatic creation is enabled:

```text
payment-event
```

could potentially be created as a separate topic.

Now you have:

```text
payment-events
payment-event
```

The application might be writing to the wrong topic.

That's a very confusing production issue.

---

# 10. Another Problem — Default Configuration

Suppose an application accidentally creates:

```text
payments
```

automatically.

You may not have intentionally chosen:

```text
Partitions = ?
Replication = ?
```

You don't want production infrastructure decisions to happen accidentally.

So controlled environments generally prefer:

```text
Application Team
       ↓
Topic request
       ↓
Platform/Kafka Team
       ↓
Review
       ↓
Explicit creation
```

---

# 11. Infrastructure-as-Code Approach

As a Senior Platform Engineer, you may also create Kafka topics through **automation/IaC** rather than manually running CLI commands.

Conceptually:

```text
Git
 │
 ▼
Terraform
 │
 ▼
Kafka Provider
 │
 ▼
Kafka Cluster
 │
 ▼
Topic
```

For example:

```text
Git Repository
      ↓
Terraform
      ↓
Plan
      ↓
Apply
      ↓
Kafka Topic
```

This gives you:

* Version control
* Review
* Repeatability
* Auditability
* Consistency

We don't need to go deeper into Terraform implementation yet.

---

# 12. Production Workflow

A good production workflow can look like:

```text
Developer
    │
    │ Request
    ▼
Platform Team
    │
    ├── Topic name
    ├── Partition requirement
    └── Replication requirement
    │
    ▼
Review
    │
    ▼
Terraform / Kafka CLI
    │
    ▼
Kafka Cluster
    │
    ▼
Topic Created
```

---

# 13. Example

Application team says:

> "We need a topic for order events."

They provide:

```text
Topic:
order-events

Partitions:
6

Replication Factor:
3
```

Platform engineer creates:

```bash
kafka-topics.sh \
  --bootstrap-server kafka01:9092 \
  --create \
  --topic order-events \
  --partitions 6 \
  --replication-factor 3
```

Then verify:

```bash
kafka-topics.sh \
  --bootstrap-server kafka01:9092 \
  --describe \
  --topic order-events
```

You should see the topic and its partitions.

---

# 14. Simple Architecture

```text
                 Application Team
                       │
                       │ Topic Request
                       ▼
              Platform Engineer
                       │
             ┌─────────┴─────────┐
             │                   │
          CLI /              Terraform
          Admin Tool              │
             │                   │
             └─────────┬─────────┘
                       ▼
                 Kafka Cluster
                       │
                 Create Topic
                       │
                 ┌─────┼─────┐
                 ▼     ▼     ▼
                P0    P1    P2
```

---

# 15. Important Things to Remember

### Who can create a topic?

* A user/application with the appropriate Kafka permissions can create one.
* Organizationally, the **Platform/Kafka team** often controls production topic creation.

### How?

* Kafka CLI
* Automation/IaC
* Application-driven automatic creation, if enabled

### Production preference?

> **Prefer controlled/explicit topic creation rather than accidental automatic creation.**

### What does the creator decide?

At this stage, remember:

* **Topic name**
* **Partition count**
* **Replication factor**

---

# 16. Interview Answer 🎯

If asked:

> **"Who creates Kafka topics?"**

You can say:

> "Kafka topics can be created explicitly using Kafka administration tools such as `kafka-topics.sh`, through infrastructure automation such as Terraform, or they can be auto-created if the broker is configured to allow it. In a production environment, I prefer controlled topic creation through the platform team or IaC so that naming, partition count, and replication are intentionally defined rather than relying on accidental topic creation."

### Mental model

```text
                 Who creates?
                      │
        ┌─────────────┼─────────────┐
        ▼             ▼             ▼
      Kafka CLI    Terraform    Auto-create
        │             │             │
        └─────────────┼─────────────┘
                      ▼
                 Kafka Cluster
                      │
                      ▼
                    Topic
```

This fits naturally after **Consumer Groups** in your current sequence without jumping into the later Kafka concepts.
