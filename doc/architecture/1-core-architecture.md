# Core Architecture

## Overview

Split-Verse is a high-performance, sharding-aware repository framework designed for managing large-scale time-series data across multiple database shards and partitions. It provides automatic data routing, partition management, and transparent query operations across distributed data.

## Key Design Principles

### 1. **Entity-Centric Design**
- All entities must implement `ShardingEntity<P>` interface
- Entities require:
  - `@Id` field (String, externally generated like UUID/ULID)
  - `@ShardingKey` field (partition column, must be Comparable)
  - Standard getter/setter methods

### 2. **Repository Pattern**
- `SplitVerseRepository` - Main entry point, manages multiple shards
- `GenericMultiTableRepository` - Handles multi-table partitioning within a shard
- `GenericPartitionedTableRepository` - Handles native database partitions

### 3. **Metadata-Driven Operations**
- `EntityMetadata` - Caches entity structure via reflection (one-time cost)
- `FieldMetadata` - Field-level information including annotations
- SQL generation based on metadata, not runtime reflection

## Component Architecture

```
┌─────────────────────────────────────┐
│     Application Layer               │
│     (User Entities & DAOs)          │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│     SplitVerseRepository            │
│     (Shard Router & Coordinator)    │
└──────────────┬──────────────────────┘
               │
    ┌──────────┴──────────┐
    ▼                     ▼
┌───────────┐      ┌───────────┐
│  Shard 1  │      │  Shard N  │
│Repository │  ... │Repository │
└───┬───────┘      └───┬───────┘
    │                  │
┌───▼───────┐      ┌───▼───────┐
│Connection │      │Connection │
│  Provider │      │  Provider │
└───┬───────┘      └───┬───────┘
    │                  │
┌───▼───────┐      ┌───▼───────┐
│  MySQL    │      │  MySQL    │
│  Database │      │  Database │
└───────────┘      └───────────┘
```

## Core Components

### 1. **SplitVerseRepository**
- **Purpose**: Main repository coordinating operations across shards
- **Responsibilities**:
  - Shard selection based on entity ID hash
  - Fan-out queries for cross-shard operations
  - Result aggregation from multiple shards
  - Transaction coordination

### 2. **ShardConfig**
- **Purpose**: Configuration for individual database shards
- **Properties**:
  - Connection details (host, port, database, credentials)
  - Shard identifier and weight
  - Enable/disable flag for maintenance

### 3. **ConnectionProvider**
- **Purpose**: Manages database connections per shard
- **Features**:
  - HikariCP connection pooling
  - Connection lifecycle management
  - Maintenance connection for DDL operations
  - UTC timezone enforcement

### 4. **EntityMetadata**
- **Purpose**: Cached reflection data for entity classes
- **Benefits**:
  - One-time reflection cost at startup
  - Prepared SQL statement generation
  - Type-safe field access
  - Annotation validation

## Design Patterns

### 1. **Builder Pattern**
Used extensively for configuration:
```java
SplitVerseRepository.builder()
    .withSingleShard(shardConfig)
    .withEntityClass(MyEntity.class)
    .withRepositoryMode(RepositoryMode.MULTI_TABLE)
    .build();
```

### 2. **Strategy Pattern**
- `PartitionStrategy` - Different partitioning approaches
- `PersistenceProvider` - Database-specific implementations
- `SqlGenerator` - Database-specific SQL generation

### 3. **Template Method Pattern**
- Base repository classes define operation templates
- Concrete implementations provide specific behaviors

### 4. **Factory Pattern**
- `PersistenceProviderFactory` - Creates database-specific providers
- `SqlGeneratorFactory` - Creates database-specific SQL generators
- `PartitionStrategyFactory` - Creates partitioning strategies

## Thread Safety

### Thread-Safe Components:
- `SplitVerseRepository` - All public methods synchronized where needed
- `ConnectionProvider` - Thread-safe connection pooling
- `EntityMetadata` - Immutable after creation

### Non-Thread-Safe:
- Entity instances (user responsibility)
- ResultSet processing (handled internally)

## Performance Optimizations

### 1. **Metadata Caching**
- Entity structure analyzed once at startup
- SQL statements pre-generated and cached
- No runtime reflection during operations

### 2. **Batch Operations**
- Bulk inserts group entities by target table
- Prepared statements reused within batches
- Configurable batch sizes

### 3. **Connection Pooling**
- HikariCP for efficient connection management
- Per-shard connection pools
- Configurable pool sizes

### 4. **Query Optimization**
- Partition pruning for date-range queries
- Index hints for MySQL queries
- Parallel shard queries for aggregations

## Error Handling

### 1. **Validation Errors**
- Entity validation at startup (fail-fast)
- Required annotations checked
- Type compatibility verified

### 2. **Runtime Errors**
- SQLException wrapped with context
- Automatic retry for transient failures
- Graceful degradation for shard failures

### 3. **Boundary Conditions**
- MySQL-style partition boundary handling
- Values below minimum → lowest partition
- Values above maximum → exception thrown

## Extension Points

### 1. **Custom Entity Types**
Implement `ShardingEntity<P>` with any Comparable partition type:
- `LocalDateTime` for time-series
- `Long` for sequential IDs
- `String` for categorical partitioning

### 2. **Custom Persistence Providers**
Extend `PersistenceProvider` for new databases:
- Implement CRUD operations
- Define DDL generation
- Handle database-specific features

### 3. **Custom SQL Generators**
Implement `SqlGenerator` for database-specific SQL:
- Optimize for specific database features
- Handle proprietary syntax
- Implement special operations

## Limitations

### 1. **ID Requirements**
- Must be String type
- Externally generated (UUID, ULID, etc.)
- No auto-increment support

### 2. **Transaction Scope**
- No distributed transactions across shards
- Single-shard transactions only
- Application-level consistency required

### 3. **Query Limitations**
- No cross-shard joins
- Aggregations require application-level processing
- Complex queries may require multiple round-trips