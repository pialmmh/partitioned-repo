# Split-Verse: Infinite Horizontal Sharding Layer

**Split-Verse** is a horizontal sharding layer built on top of partitioned-repo that enables infinite scalability across multiple MySQL instances. It provides transparent sharding for telecom-scale applications handling millions of users.

## 🎯 Core Requirements

### 1. **ID Generation Strategy**
- **NO AUTO_INCREMENT**: All IDs must be generated externally before insert
- **Supports Any ID Type**: String, Long, UUID, composite keys
- **Recommended**: NanoID or ULID for new systems
- **Telecom IDs Supported**: MSISDN, IMSI, ICCID, custom account IDs

### 2. **Sharding Architecture**
- **Hash-Based Sharding**: Using consistent hashing with virtual nodes
- **Elastic Scaling**: Add/remove MySQL instances with minimal data movement
- **No Cross-Shard Transactions**: Each shard is independent
- **Transparent Routing**: Applications don't need to know about shards

### 3. **MySQL Instance Management**
- **Multiple MySQL Masters**: Each shard points to a different MySQL instance
- **Active-Active Support**: Compatible with MySQL bi-directional replication
- **Connection Pooling**: Per-shard connection pool management
- **Health Monitoring**: Automatic detection of unhealthy shards

### 4. **API Compatibility**
- **Same Interface**: Implements the same ShardingRepository interface
- **Drop-in Replacement**: Can replace partitioned-repo with minimal changes
- **Backward Compatible**: Start with 1 shard, scale to N shards

## 📋 Implementation Phases

### Phase 1: Minimal Implementation (Current)
- Single shard wrapper around existing partitioned-repo
- Basic hash-based routing (ready for multi-shard)
- External ID generation requirement
- Configuration structure for future shards

### Phase 2: Multi-Shard Support
- Multiple MySQL instance management
- Consistent hashing with virtual nodes
- Parallel query execution for non-sharded keys
- Shard health monitoring

### Phase 3: Production Features
- Shard rebalancing tools
- Global secondary indexes
- Query optimization and caching
- Monitoring and metrics

## 🏗️ Architecture

```
┌─────────────────────────────────────────┐
│          Application Layer              │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│         SplitVerseRepository            │
│  (Routing + Shard Management Layer)     │
└────────┬──────────────────┬─────────────┘
         │                  │
┌────────▼────────┐ ┌───────▼────────┐
│PartitionedRepo │ │PartitionedRepo │ ...
│   (Shard 1)    │ │   (Shard 2)    │
└────────┬────────┘ └───────┬────────┘
         │                  │
┌────────▼────────┐ ┌───────▼────────┐
│   MySQL DB 1    │ │   MySQL DB 2    │
│  Instance/Host  │ │  Instance/Host  │
└─────────────────┘ └─────────────────┘
```

## 🚀 Quick Start

### 1. Single Shard (Transition Mode)

```java
// Start with your existing setup wrapped in split-verse
SplitVerseRepository<UserEntity, String> repo = SplitVerseRepository.builder()
    .withSingleShard(
        ShardConfig.builder()
            .shardId("shard-001")
            .host("127.0.0.1")
            .port(3306)
            .database("telecom_db")
            .username("root")
            .password("password")
            .build()
    )
    .withEntityClass(UserEntity.class)
    .withKeyClass(String.class)
    .build();

// Use exactly like partitioned-repo
String userId = NanoIdUtils.randomNanoId(); // External ID generation
UserEntity user = new UserEntity();
user.setId(userId);
user.setMsisdn("+8801712345678");
user.setCreatedAt(LocalDateTime.now());

repo.insert(user);
```

### 2. Multi-Shard (Future)

```java
// Scale to multiple shards when ready
SplitVerseRepository<UserEntity, String> repo = SplitVerseRepository.builder()
    .withShardConfigs(Arrays.asList(
        ShardConfig.builder()
            .shardId("dhaka-01")
            .host("10.0.1.10")
            .port(3306)
            .database("telecom_dhaka")
            .build(),
        ShardConfig.builder()
            .shardId("chittagong-01")
            .host("10.0.2.10")
            .port(3306)
            .database("telecom_ctg")
            .build()
    ))
    .withRoutingStrategy(RoutingStrategy.CONSISTENT_HASH)
    .withEntityClass(UserEntity.class)
    .withKeyClass(String.class)
    .build();
```

## 📊 Use Cases

### Telecom Subscriber Management
```java
@Table(name = "subscribers")
public class SubscriberEntity implements ShardingEntity<String> {
    @Id
    @Column(name = "subscriber_id")
    private String id; // MSISDN or account ID
    
    @Column(name = "msisdn")
    private String msisdn;
    
    @Column(name = "imsi")
    private String imsi;
    
    @Column(name = "balance")
    private BigDecimal balance;
    
    @ShardingKey
    @Column(name = "created_at")
    private LocalDateTime createdAt;
}
```

### CDR (Call Detail Records)
```java
@Table(name = "cdr")
public class CdrEntity implements ShardingEntity<String> {
    @Id
    @Column(name = "cdr_id")
    private String id; // Externally generated
    
    @Column(name = "caller_msisdn")
    private String callerMsisdn;
    
    @Column(name = "called_msisdn")
    private String calledMsisdn;
    
    @Column(name = "duration_seconds")
    private Integer duration;
    
    @ShardingKey
    @Column(name = "call_time")
    private LocalDateTime callTime;
}
```

## 🔧 Configuration

### application.yml
```yaml
split-verse:
  # Start with single shard
  shards:
    - id: primary-shard
      host: ${MYSQL_HOST:127.0.0.1}
      port: ${MYSQL_PORT:3306}
      database: ${MYSQL_DATABASE:telecom_db}
      username: ${MYSQL_USER:root}
      password: ${MYSQL_PASSWORD:password}
      connection-pool-size: 10
      
  routing:
    strategy: HASH  # or CONSISTENT_HASH when multi-shard
    hash-function: MURMUR3
    
  # Prepared for future expansion
  future-shards:
    - id: shard-002
      host: mysql-shard-002.internal
      enabled: false  # Enable when ready
```

## ⚠️ Important Considerations

### What Split-Verse IS:
- ✅ Horizontal sharding layer for infinite scalability
- ✅ Transparent routing to multiple MySQL instances
- ✅ Drop-in replacement for partitioned-repo
- ✅ Support for any ID type (string, numeric, composite)

### What Split-Verse IS NOT:
- ❌ NOT a distributed transaction coordinator
- ❌ NOT a replacement for MySQL replication
- ❌ NOT a query optimizer for complex JOINs
- ❌ NOT suitable for cross-shard transactions

### Limitations:
1. **No Cross-Shard Transactions**: Each operation is confined to one shard
2. **No Cross-Shard JOINs**: JOINs only work within same shard
3. **Fan-out Queries**: Non-sharding-key queries hit all shards
4. **Eventual Consistency**: With active-active replication

## 🗺️ Migration Path

### Step 1: Prepare (Current System)
- Ensure all IDs are externally generated
- Remove AUTO_INCREMENT dependencies
- Test with single split-verse instance

### Step 2: Transition
- Deploy split-verse with single shard
- Monitor performance and stability
- Prepare additional MySQL instances

### Step 3: Scale
- Add new shards one at a time
- Use consistent hashing for minimal disruption
- Monitor data distribution

## 📈 Performance Targets

- **Single Shard**: 10,000+ ops/second
- **10 Shards**: 100,000+ ops/second  
- **Query Routing**: < 1ms overhead
- **Shard Addition**: < 5% data movement

## 🔍 Monitoring

Key metrics to track:
- Operations per second per shard
- Data distribution across shards
- Query fanout ratio
- Connection pool utilization
- Shard response times

## 📝 License

Same as partitioned-repo project

---

**Note**: This is a gradual evolution from partitioned-repo. Start with single-shard mode, validate in production, then scale horizontally as needed.