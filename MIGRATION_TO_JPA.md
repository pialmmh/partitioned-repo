# Split-Verse Migration to JPA

## Overview
Migrate Split-Verse to use JPA for standard CRUD operations while maintaining custom sharding capabilities and MySQL extended insert optimization.

## Architecture Design

### 1. Entity Hierarchy

```java
import javax.persistence.*;
import com.telcobright.core.annotation.ShardingKey;
import com.telcobright.core.annotation.PartitionKey;

// Base JPA entity with sharding capabilities
@MappedSuperclass
public abstract class ShardingJpaEntity {

    @Id
    @Column(name = "id", nullable = false)
    private String id;  // Split-Verse requires String IDs

    // Getters/setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    // Abstract method for sharding
    public abstract String getShardKey();
}

// Example entity combining JPA and sharding
@Entity
@Table(name = "users")
public class User extends ShardingJpaEntity {

    @ShardingKey  // Custom Split-Verse annotation
    @Column(name = "user_id")
    private String userId;

    @PartitionKey(type = PartitionType.MONTHLY)  // Custom annotation
    @Column(name = "created_at")
    private LocalDateTime createdAt;

    @Column(name = "name")
    private String name;

    @Column(name = "email")
    private String email;

    @Override
    public String getShardKey() {
        return userId;
    }
}
```

### 2. Repository Layer

```java
import org.springframework.data.jpa.repository.JpaRepository;

// Base sharding-aware JPA repository
public interface ShardingJpaRepository<T extends ShardingJpaEntity, ID>
    extends JpaRepository<T, ID> {

    // Standard JPA methods available:
    // - findById(ID id)
    // - save(T entity)
    // - delete(T entity)
    // - findAll()

    // Custom method for MySQL extended insert
    int batchInsertMySQL(List<T> entities);
}

// Concrete implementation
@Repository
public class ShardingJpaRepositoryImpl<T extends ShardingJpaEntity, ID>
    extends SimpleJpaRepository<T, ID>
    implements ShardingJpaRepository<T, ID> {

    @PersistenceContext
    private EntityManager entityManager;

    private final ShardRouter shardRouter;

    @Override
    public Optional<T> findById(ID id) {
        // Route to correct shard
        EntityManager shardEm = getShardEntityManager(id);
        return Optional.ofNullable(shardEm.find(entityClass, id));
    }

    @Override
    @Transactional
    public int batchInsertMySQL(List<T> entities) {
        // Group by shard
        Map<String, List<T>> shardGroups = groupByShardKey(entities);

        int totalInserted = 0;
        for (Map.Entry<String, List<T>> entry : shardGroups.entrySet()) {
            String shardId = entry.getKey();
            List<T> shardEntities = entry.getValue();

            // Get connection for specific shard
            EntityManager shardEm = getShardEntityManager(shardId);
            Connection conn = shardEm.unwrap(Connection.class);

            // Use MySQL extended insert
            String sql = buildExtendedInsertSQL(shardEntities);
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                setParameters(pstmt, shardEntities);
                totalInserted += pstmt.executeUpdate();
            }
        }
        return totalInserted;
    }
}
```

### 3. Split-Verse Integration

```java
// Modified SplitVerseRepository using JPA
public class SplitVerseRepository<T extends ShardingJpaEntity>
    implements ShardingRepository<T> {

    private final Map<String, EntityManager> shardEntityManagers;
    private final HashRouter router;
    private final Class<T> entityClass;

    @Override
    public T findById(String id) {
        // Determine shard
        String shardKey = extractShardKey(id);
        String shardId = router.route(shardKey);

        // Use JPA on specific shard
        EntityManager em = shardEntityManagers.get(shardId);
        return em.find(entityClass, id);
    }

    @Override
    public void save(T entity) {
        // Route to correct shard
        String shardId = router.route(entity.getShardKey());
        EntityManager em = shardEntityManagers.get(shardId);

        em.getTransaction().begin();
        em.persist(entity);
        em.getTransaction().commit();
    }

    @Override
    public int batchInsert(List<T> entities) {
        // For MySQL, use extended insert
        if (isMySQLDatabase()) {
            return batchInsertMySQLExtended(entities);
        }
        // For others, use JPA batch
        return batchInsertJPA(entities);
    }

    private int batchInsertMySQLExtended(List<T> entities) {
        Map<String, List<T>> shardGroups = groupByShardKey(entities);

        int total = 0;
        for (Map.Entry<String, List<T>> entry : shardGroups.entrySet()) {
            String shardId = entry.getKey();
            List<T> batch = entry.getValue();

            // Get raw connection from JPA
            EntityManager em = shardEntityManagers.get(shardId);
            Connection conn = em.unwrap(Connection.class);

            // Build and execute MySQL extended INSERT
            String sql = "INSERT INTO " + getTableName() + " (...) VALUES " +
                batch.stream()
                    .map(e -> "(?)")
                    .collect(Collectors.joining(", "));

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Set parameters
                total += pstmt.executeUpdate();
            }
        }
        return total;
    }
}
```

## Migration Steps

### Step 1: Add JPA Dependencies

```xml
<!-- Add to split-verse pom.xml -->
<dependencies>
    <!-- JPA API -->
    <dependency>
        <groupId>javax.persistence</groupId>
        <artifactId>javax.persistence-api</artifactId>
        <version>2.2</version>
    </dependency>

    <!-- Hibernate as JPA provider -->
    <dependency>
        <groupId>org.hibernate</groupId>
        <artifactId>hibernate-core</artifactId>
        <version>5.6.15.Final</version>
    </dependency>

    <!-- Spring Data JPA (optional, for repository support) -->
    <dependency>
        <groupId>org.springframework.data</groupId>
        <artifactId>spring-data-jpa</artifactId>
        <version>2.7.14</version>
    </dependency>
</dependencies>
```

### Step 2: Create Base Entity

```java
package com.telcobright.core.entity;

import javax.persistence.*;

@MappedSuperclass
public abstract class BaseShardingEntity {

    @Id
    @Column(name = "id", nullable = false, length = 100)
    @GeneratedValue(generator = "split-verse-id")
    @GenericGenerator(
        name = "split-verse-id",
        strategy = "com.telcobright.core.id.ShardAwareIdGenerator"
    )
    private String id;

    @Version
    private Long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }

    // Abstract methods for sharding
    public abstract String getShardKey();
    public abstract String getPartitionKey();
}
```

### Step 3: Update Existing Entities

```java
// Before (custom annotations only)
@Table(name = "users")  // Custom annotation
public class User implements ShardingEntity {
    @Id(autoGenerated = false)  // Custom annotation
    private String id;
}

// After (JPA + custom annotations)
@Entity  // JPA
@javax.persistence.Table(name = "users")  // JPA
@ShardingConfig(strategy = ShardingStrategy.HASH)  // Custom
public class User extends BaseShardingEntity {
    // ID handled by base class

    @ShardingKey  // Custom annotation
    @Column(name = "user_id")
    private String userId;
}
```

### Step 4: Configure Multiple EntityManagers for Shards

```java
@Configuration
public class ShardingJpaConfiguration {

    @Bean
    public Map<String, EntityManagerFactory> shardEntityManagerFactories() {
        Map<String, EntityManagerFactory> factories = new HashMap<>();

        for (ShardConfig shardConfig : shardConfigs) {
            LocalContainerEntityManagerFactoryBean em =
                new LocalContainerEntityManagerFactoryBean();
            em.setDataSource(createDataSource(shardConfig));
            em.setPackagesToScan("com.telcobright.entities");
            em.setJpaVendorAdapter(new HibernateJpaVendorAdapter());

            Properties props = new Properties();
            props.put("hibernate.hbm2ddl.auto", "validate");
            props.put("hibernate.show_sql", "false");
            props.put("hibernate.jdbc.batch_size", "1000");
            em.setJpaProperties(props);

            em.afterPropertiesSet();
            factories.put(shardConfig.getShardId(), em.getObject());
        }

        return factories;
    }
}
```

## Benefits of JPA Migration

### 1. Standard JPA Features
- **Lazy Loading**: Automatic for relationships
- **Caching**: L1 and L2 cache support
- **Optimistic Locking**: Via @Version
- **Auditing**: @PrePersist, @PreUpdate
- **Criteria API**: Type-safe queries
- **JPQL**: Database-independent queries

### 2. Keep Custom Features
- **Sharding**: Route queries to correct shard
- **Partitioning**: Table partitioning within shards
- **MySQL Extended Insert**: 5-10x faster batch inserts
- **Custom ID Generation**: Shard-aware IDs

### 3. Best of Both Worlds
```java
// Standard JPA for single operations
User user = repository.findById("user123");  // JPA
user.setName("New Name");
repository.save(user);  // JPA with optimistic locking

// Custom optimization for batch
List<User> users = generateMillionUsers();
repository.batchInsertMySQL(users);  // MySQL extended insert
```

## Implementation Example

```java
@Entity
@javax.persistence.Table(name = "orders")
@ShardingConfig(
    shardKey = "customerId",
    partitionKey = "orderDate",
    partitionType = PartitionType.MONTHLY
)
public class Order extends BaseShardingEntity {

    @ShardingKey
    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @PartitionKey
    @Column(name = "order_date", nullable = false)
    private LocalDateTime orderDate;

    @Column(name = "total_amount")
    private BigDecimal totalAmount;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL)
    private List<OrderItem> items;

    @Override
    public String getShardKey() {
        return customerId;
    }

    @Override
    public String getPartitionKey() {
        return orderDate.format(DateTimeFormatter.ISO_DATE);
    }
}

// Usage
@Service
public class OrderService {

    @Autowired
    private SplitVerseRepository<Order> repository;

    public void processOrders(List<Order> orders) {
        // Batch insert with MySQL optimization
        repository.batchInsertMySQL(orders);

        // Standard JPA query on specific shard
        Order order = repository.findById("order123");

        // Update using JPA
        order.setTotalAmount(newAmount);
        repository.save(order);
    }
}
```

## Summary

The migration to JPA is not only possible but recommended because:

1. **Maintains all sharding capabilities** - routing, partitioning, etc.
2. **Adds standard JPA benefits** - caching, lazy loading, transactions
3. **Keeps MySQL optimization** - extended insert for batch operations
4. **Cleaner code** - leverage JPA's maturity instead of reinventing
5. **Better ecosystem** - works with Spring Data, QueryDSL, etc.

The key is using JPA for what it does well (standard CRUD, relationships, caching) while keeping custom optimizations (sharding logic, MySQL extended insert) where they provide value.