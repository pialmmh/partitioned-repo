# 🧩 Generic Sharding-Aware Repository Framework (For AI Code Agent)

We are building a **generic, framework-independent library** for sharded and partitioned data access using **Apache ShardingSphere**. This library supports time-based scaling for large datasets like `sms`, `orders`, `calls`, `alerts`, etc.

It handles both **partitioned table** and **multi-table** storage strategies, and automatically creates/cleans tables or partitions based on a rolling date window.

---

## ✅ Supported Storage Modes (via Apache ShardingSphere)

### 1. **Single Table with Date-Based Partitioning**
- One logical table (e.g., `event`)
- Sharded into physical tables like `event_YYYYMMDD`
- Sharding handled via a `created_at` field using ShardingSphere sharding algorithm

### 2. **Multi-Table Sharding by Date**
- Fully separate tables like `order20250719`, `call20250720`, etc.
- Each table created per day
- Library manages creation/deletion based on retention policy

---

## 🔧 Configuration Parameters

- `mode`: `PARTITIONED_TABLE` or `MULTI_TABLE`
- `entityName`: e.g., `order`, `sms`, `call`
- `shardKey`: e.g., `created_at`
- `retentionSpanDays`: e.g., `7`
- `partitionAdjustmentTime`: e.g., `04:00`
- `shardingAlgorithmClass`: for class-based routing
- `dataSource`: externally injected

---

## 📊 Query Support

Supports transparent group-by queries across partitions or tables.

### Partitioned Table Mode:
```sql
SELECT user_id, DATE(created_at) AS eventDate, COUNT(*) AS eventCount
FROM event
WHERE created_at BETWEEN '2025-07-19' AND '2025-07-25'
GROUP BY user_id, eventDate;
```

### Multi-Table Mode (Auto-Generated Internally):
```sql
SELECT user_id, eventDate, SUM(eventCount) AS eventCount
FROM (
  SELECT user_id, DATE(created_at) AS eventDate, COUNT(*) AS eventCount
  FROM sms20250719
  WHERE created_at BETWEEN '2025-07-19 00:00:00' AND '2025-07-19 23:59:59'
  GROUP BY user_id, eventDate

  UNION ALL

  SELECT user_id, DATE(created_at) AS eventDate, COUNT(*) AS eventCount
  FROM sms20250720
  WHERE created_at BETWEEN '2025-07-20 00:00:00' AND '2025-07-20 23:59:59'
  GROUP BY user_id, eventDate
) t
GROUP BY user_id, eventDate;
```

---

## 🔁 Runtime Behavior

- On **startup**: Create required tables or partitions for ±`retentionSpanDays`
- On **daily schedule** (`partitionAdjustmentTime`):
    - if the property automanagepartition=true
      - Add future tables/partitions
      - Drop expired ones

---

## ☁️ Dependency Setup (Using Our Custom Maven Repo)

### 1. Add the custom Maven repository:

```xml
<repositories>
  <repository>
    <id>pialmmh-github-repo</id>
    <url>https://pialmmh.github.io/maven-repo</url>
  </repository>
</repositories>
```

### 2. Add the ShardingSphere dependency:

```xml
<dependencies>
  <dependency>
    <groupId>org.apache.shardingsphere</groupId>
    <artifactId>shardingsphere-all-in-one</artifactId>
    <version>5.5.3</version>
  </dependency>
</dependencies>
```

This fat JAR includes core JDBC sharding, orchestration, and utility modules for local use.

---

## 🧩 Use Cases

- `sms`, `call`, `order`, `invoice`, `event`, `audit`, `alert`, `message`, etc.
- Any entity with high insert rate and time-bound lifecycle
- Needs range queries, aggregation, and retention support

---

## 💡 Goals

- ⚙️ Hide sharding and partitioning logic from application code
- 🧩 Provide a unified insert/query interface
- 🚫 No Spring or Quarkus-specific annotations — config via constructor
- 🔄 Works in plain Java, Spring Boot, Quarkus

---

Let me know if you need a sample `sharding-jdbc.yaml` or Java config bootstrapping this setup.
