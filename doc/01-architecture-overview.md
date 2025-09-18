# Split-Verse Architecture Overview

## 1. Introduction

Split-Verse is a distributed sharding framework for MySQL that provides horizontal scaling through database-level sharding and table-level partitioning. It enforces best practices for distributed systems while maintaining simplicity and performance.

## 2. Core Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Application Layer                      │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│              SplitVerseRepository (Public API)            │
│  • Builder-only access pattern                           │
│  • Type-safe configuration                               │
│  • Compile-time validation                               │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│                    Routing Layer                         │
│  • Level 1: Hash(ID) → Select Shard (Database)          │
│  • Level 2: Partition Value → Select Table               │
└─────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────┐
│                  Physical Storage Layer                  │
│                                                          │
│  Shard 1 (server1.dc1.com)     Shard 2 (server2.dc2.com)│
│    ├── table_2024_01_01           ├── table_2024_01_01  │
│    ├── table_2024_01_02           ├── table_2024_01_02  │
│    └── table_2024_01_03           └── table_2024_01_03  │
└─────────────────────────────────────────────────────────┘
```

## 3. Key Design Principles

### 3.1 Single Primary Key Only
- **No composite keys** in database schema
- Every table has a single `VARCHAR(22)` primary key
- Simplifies ORM integration and data migrations

### 3.2 External ID Generation
- IDs must be generated externally (UUID, ULID, NanoID)
- No AUTO_INCREMENT columns
- Ensures consistent hashing across shards

### 3.3 Two-Level Distribution
- **Level 1 (Sharding)**: Hash-based distribution across database servers
- **Level 2 (Partitioning)**: Value-based or hash-based table selection within each shard

### 3.4 Multi-Table Over Native Partitioning
- Prefer separate tables over MySQL native partitions
- Avoids composite key requirements
- Simplifies maintenance and archival

## 4. Sharding Strategies

### 4.1 SINGLE_KEY_HASH
- ID serves as both shard key and partition key
- Suitable for user profiles, accounts
- Simple but limited query flexibility

### 4.2 DUAL_KEY_HASH_RANGE
- ID for shard selection
- Separate column for range-based partitioning
- Ideal for time-series data (orders, logs, events)

### 4.3 DUAL_KEY_HASH_HASH
- ID for shard selection
- Separate column for hash-based partitioning
- Good for categorical distribution

## 5. Query Resolution Flow

```
Query: findByIdAndPartitionRange("order-123", startDate, endDate)
    ↓
Step 1: Hash("order-123") → Shard 2
    ↓
Step 2: Date range → Tables [orders_2024_01_01, orders_2024_01_02]
    ↓
Step 3: Execute on Shard 2:
    SELECT * FROM orders_2024_01_01 WHERE id = ? AND created_at BETWEEN ? AND ?
    UNION ALL
    SELECT * FROM orders_2024_01_02 WHERE id = ? AND created_at BETWEEN ? AND ?
    ↓
Step 4: Merge results and return
```

## 6. Performance Characteristics

| Query Type | Performance | Reason |
|------------|------------|--------|
| ID + Partition Value | ⭐⭐⭐⭐⭐ | Direct table access |
| Partition Range Only | ⭐⭐⭐⭐ | Partition pruning |
| ID Only | ⭐⭐ | Must scan all partitions |
| Full Scan | ⭐ | Scans all shards and partitions |

## 7. Deployment Architecture

### 7.1 Distributed Deployment
```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   DC1 East   │     │   DC2 West   │     │   DC3 Europe │
│              │     │              │     │              │
│  Shard 1-3   │     │  Shard 4-6   │     │  Shard 7-9   │
└──────────────┘     └──────────────┘     └──────────────┘
```

### 7.2 No Database Names in Configuration
- Shards identified by host:port only
- Database names derived from sharding strategy
- Enables true geographic distribution

## 8. Data Lifecycle Management

### 8.1 Automatic Maintenance
- Scheduled creation of future partitions
- Automatic deletion of expired partitions
- Configurable retention policies

### 8.2 Table Naming Conventions
- Date-based: `{base}_{yyyy}_{MM}_{dd}`
- Range-based: `{base}_range_{index}`
- Hash-based: `{base}_hash_{bucket}`

## 9. Scalability Limits

| Dimension | Limit | Notes |
|-----------|-------|-------|
| Shards | Unlimited | Add shards dynamically |
| Tables per shard | 10,000+ | MySQL limit |
| Records per table | Millions | Depends on hardware |
| ID size | 8-22 bytes | Configurable |
| Query parallelism | Shard count | One thread per shard |

## 10. Future Enhancements

1. **Read replicas** per shard
2. **Cross-shard transactions** (2PC)
3. **Automatic rebalancing** of data
4. **Query optimization** with statistics
5. **Monitoring and metrics** integration