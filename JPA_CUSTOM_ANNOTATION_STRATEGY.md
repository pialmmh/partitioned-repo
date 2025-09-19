# Split-Verse Custom Annotations with JPA

## The Problem
Split-Verse needs custom annotations (@ShardingKey, @PartitionKey) alongside JPA annotations (@Entity, @Id, @Table).

## Solution: Split-Verse Handles Its Own Annotations

### 1. Entity Design - Dual Annotations

```java
package com.telcobright.entities;

import javax.persistence.*;
import com.telcobright.core.annotation.ShardingKey;
import com.telcobright.core.annotation.PartitionKey;

@Entity  // JPA annotation
@Table(name = "users")  // JPA annotation
public class User {

    @Id  // JPA annotation for persistence
    @Column(name = "user_id")
    @ShardingKey  // Split-Verse annotation for routing
    private String userId;

    @Column(name = "created_at")
    @PartitionKey(type = PartitionType.MONTHLY)  // Split-Verse annotation
    private LocalDateTime createdAt;

    @Column(name = "email")
    private String email;

    // Standard getters/setters
}
```

### 2. Repository Layer - Clean Separation

```java
// db-util provides the JPA repository (doesn't know about Split-Verse annotations)
public interface UserJpaRepository extends MySqlOptimizedRepository<User, String> {
    // Works with JPA annotations only
    Optional<User> findById(String id);
    List<User> findByEmail(String email);

    @Query("SELECT u FROM User u WHERE u.createdAt BETWEEN :start AND :end")
    List<User> findByDateRange(@Param("start") LocalDateTime start,
                               @Param("end") LocalDateTime end);
}
```

### 3. Split-Verse Wrapper - Handles Routing

```java
// Split-Verse wrapper that understands custom annotations
@Component
public class SplitVerseUserRepository {

    // Multiple JPA repositories (one per shard)
    @Autowired @Qualifier("shard1")
    private UserJpaRepository shard1Repository;

    @Autowired @Qualifier("shard2")
    private UserJpaRepository shard2Repository;

    @Autowired @Qualifier("shard3")
    private UserJpaRepository shard3Repository;

    private final ShardRouter router;
    private final AnnotationProcessor annotationProcessor;

    public SplitVerseUserRepository() {
        this.router = new HashRouter(3);
        this.annotationProcessor = new AnnotationProcessor(User.class);
    }

    public User findById(String id) {
        // Extract sharding key from entity metadata
        String shardingKey = annotationProcessor.extractShardingKey(id);

        // Route to correct shard
        UserJpaRepository shardRepo = getShardRepository(shardingKey);

        // Use standard JPA
        return shardRepo.findById(id).orElse(null);
    }

    public void batchInsert(List<User> users) {
        // Group by sharding key (using Split-Verse annotations)
        Map<String, List<User>> shardGroups = annotationProcessor.groupByShardingKey(users);

        for (Map.Entry<String, List<User>> entry : shardGroups.entrySet()) {
            String shardId = entry.getKey();
            List<User> shardUsers = entry.getValue();

            // Get appropriate JPA repository
            UserJpaRepository shardRepo = getShardRepositoryById(shardId);

            // Use db-util's MySQL optimization
            shardRepo.insertExtendedToMysql(shardUsers);
        }
    }

    private UserJpaRepository getShardRepository(String shardingKey) {
        int shardNum = router.route(shardingKey);
        return switch(shardNum) {
            case 0 -> shard1Repository;
            case 1 -> shard2Repository;
            case 2 -> shard3Repository;
            default -> shard1Repository;
        };
    }
}
```

### 4. Annotation Processor - Split-Verse Component

```java
package com.telcobright.core.processor;

// This is Split-Verse's responsibility
public class AnnotationProcessor {

    private final Field shardingKeyField;
    private final Field partitionKeyField;

    public AnnotationProcessor(Class<?> entityClass) {
        // Find fields with @ShardingKey
        this.shardingKeyField = findFieldWithAnnotation(entityClass, ShardingKey.class);

        // Find fields with @PartitionKey
        this.partitionKeyField = findFieldWithAnnotation(entityClass, PartitionKey.class);
    }

    public String extractShardingKey(Object entity) {
        try {
            shardingKeyField.setAccessible(true);
            return (String) shardingKeyField.get(entity);
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract sharding key", e);
        }
    }

    public Map<String, List<User>> groupByShardingKey(List<User> users) {
        return users.stream()
            .collect(Collectors.groupingBy(this::extractShardingKey));
    }

    public String getPartitionSuffix(Object entity) {
        // Extract partition key and calculate partition
        // e.g., "202401" for January 2024
    }
}
```

## Architecture Layers

```
┌─────────────────────────────────────┐
│         Application Code            │
└─────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────┐
│    Split-Verse Repository Layer     │ ← Handles @ShardingKey, @PartitionKey
│  (Routing, Sharding, Partitioning)  │ ← Uses AnnotationProcessor
└─────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────┐
│      MySqlOptimizedRepository       │ ← db-util (only knows JPA)
│        (JPA + MySQL batch)          │ ← Uses @Entity, @Id, @Table
└─────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────┐
│          Spring Data JPA            │
└─────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────┐
│            Hibernate                │
└─────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────┐
│     MySQL Shards (1, 2, 3...)       │
└─────────────────────────────────────┘
```

## Key Points

### 1. **db-util doesn't need to know about Split-Verse annotations**
- It just provides JPA + MySQL batch insert
- Works with standard JPA annotations only

### 2. **Split-Verse handles its own annotations**
- AnnotationProcessor reads @ShardingKey, @PartitionKey
- Routes to appropriate shard
- Groups entities for batch operations

### 3. **Clean separation of concerns**
- **JPA layer**: Persistence (@Entity, @Id, @Table)
- **Split-Verse layer**: Sharding/Routing (@ShardingKey, @PartitionKey)

### 4. **Entities have both annotations**
- JPA annotations for persistence
- Split-Verse annotations for routing
- No conflict because they serve different purposes

## Example Usage

```java
@Service
public class UserService {

    @Autowired
    private SplitVerseUserRepository repository;  // Split-Verse wrapper

    public void createUsers(List<User> users) {
        // Split-Verse handles routing based on @ShardingKey
        // db-util handles MySQL batch insert
        repository.batchInsert(users);
    }

    public List<User> findRecentUsers(String userId, LocalDateTime since) {
        // Split-Verse routes to correct shard based on userId
        // JPA executes the query on that shard
        return repository.findByUserIdAndCreatedAtAfter(userId, since);
    }
}
```

## Summary

- **db-util**: Provides JPA + MySQL optimization (doesn't care about sharding)
- **Split-Verse**: Handles routing using custom annotations
- **Entity**: Has both JPA and Split-Verse annotations
- **Clean separation**: Each layer handles its own concerns