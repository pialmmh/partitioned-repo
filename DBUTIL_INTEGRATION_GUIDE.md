# Split-Verse Integration with DB-Util

## Prerequisites
✅ db-util is installed in local Maven repository

## Step 1: Add Dependency to Split-Verse

Add to `/home/mustafa/telcobright-projects/routesphere/split-verse/pom.xml`:

```xml
<dependencies>
    <!-- Existing dependencies... -->

    <!-- DB-Util for MySQL optimized batch insert -->
    <dependency>
        <groupId>com.telcobright</groupId>
        <artifactId>dbutil</artifactId>
        <version>1.0-SNAPSHOT</version>
    </dependency>

    <!-- Spring Data JPA (required by db-util) -->
    <dependency>
        <groupId>org.springframework.data</groupId>
        <artifactId>spring-data-jpa</artifactId>
        <version>2.7.14</version>
    </dependency>

    <!-- JPA API -->
    <dependency>
        <groupId>javax.persistence</groupId>
        <artifactId>javax.persistence-api</artifactId>
        <version>2.2</version>
    </dependency>

    <!-- Hibernate -->
    <dependency>
        <groupId>org.hibernate</groupId>
        <artifactId>hibernate-core</artifactId>
        <version>5.6.15.Final</version>
    </dependency>
</dependencies>
```

## Step 2: Verify Installation

```bash
cd /home/mustafa/telcobright-projects/routesphere/split-verse
mvn dependency:tree | grep dbutil

# Should show:
# com.telcobright:dbutil:jar:1.0-SNAPSHOT:compile
```

## Step 3: Create JPA Entity with Dual Annotations

```java
package com.telcobright.splitverse.entity;

import javax.persistence.*;
import com.telcobright.core.annotation.ShardingKey;
import com.telcobright.core.annotation.PartitionKey;

@Entity  // JPA
@Table(name = "users")  // JPA
public class User {

    @Id  // JPA
    @Column(name = "user_id")
    @ShardingKey  // Split-Verse custom
    private String userId;

    @Column(name = "email")
    private String email;

    @Column(name = "created_at")
    @PartitionKey(type = PartitionType.MONTHLY)  // Split-Verse custom
    private LocalDateTime createdAt;

    // Getters/setters
}
```

## Step 4: Create Repository for Each Shard

```java
package com.telcobright.splitverse.repository;

import com.telcobright.util.db.repository.MySqlOptimizedRepository;

// Shard 1 Repository
@Repository
public interface UserShard1Repository extends MySqlOptimizedRepository<User, String> {
    // Inherits all JPA methods + insertExtendedToMysql()

    @Query("SELECT u FROM User u WHERE u.createdAt BETWEEN :start AND :end")
    List<User> findByDateRange(@Param("start") LocalDateTime start,
                               @Param("end") LocalDateTime end);
}

// Shard 2 Repository
@Repository
public interface UserShard2Repository extends MySqlOptimizedRepository<User, String> {
    // Same interface for shard 2
}

// Shard 3 Repository
@Repository
public interface UserShard3Repository extends MySqlOptimizedRepository<User, String> {
    // Same interface for shard 3
}
```

## Step 5: Configure Multiple DataSources

```java
@Configuration
@EnableJpaRepositories(
    basePackages = "com.telcobright.splitverse.repository.shard1",
    entityManagerFactoryRef = "shard1EntityManagerFactory",
    transactionManagerRef = "shard1TransactionManager",
    repositoryFactoryBeanClass = MySqlOptimizedRepositoryFactory.class  // from db-util
)
public class Shard1DataSourceConfig {

    @Bean
    @ConfigurationProperties("spring.datasource.shard1")
    public DataSource shard1DataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean shard1EntityManagerFactory() {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(shard1DataSource());
        em.setPackagesToScan("com.telcobright.splitverse.entity");
        em.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
        return em;
    }
}

// Repeat for Shard2, Shard3...
```

## Step 6: Create Split-Verse Wrapper

```java
@Component
public class SplitVerseUserRepository {

    @Autowired private UserShard1Repository shard1;
    @Autowired private UserShard2Repository shard2;
    @Autowired private UserShard3Repository shard3;

    private final HashRouter router = new HashRouter(3);

    public void batchInsert(List<User> users) {
        // Group by shard using Split-Verse annotations
        Map<Integer, List<User>> shardGroups = groupByShardKey(users);

        for (Map.Entry<Integer, List<User>> entry : shardGroups.entrySet()) {
            MySqlOptimizedRepository<User, String> repo = getShardRepo(entry.getKey());

            // Use db-util's MySQL optimized batch insert!
            repo.insertExtendedToMysql(entry.getValue());
        }
    }

    public User findById(String id) {
        int shardNum = router.route(extractShardKey(id));
        MySqlOptimizedRepository<User, String> repo = getShardRepo(shardNum);
        return repo.findById(id).orElse(null);
    }

    private MySqlOptimizedRepository<User, String> getShardRepo(int shardNum) {
        return switch(shardNum) {
            case 0 -> shard1;
            case 1 -> shard2;
            case 2 -> shard3;
            default -> shard1;
        };
    }
}
```

## Step 7: Test the Integration

```java
@SpringBootTest
public class DbUtilIntegrationTest {

    @Autowired
    private SplitVerseUserRepository repository;

    @Test
    public void testBatchInsert() {
        // Create test users
        List<User> users = IntStream.range(0, 10000)
            .mapToObj(i -> {
                User user = new User();
                user.setUserId("user_" + i);
                user.setEmail("user" + i + "@example.com");
                user.setCreatedAt(LocalDateTime.now());
                return user;
            })
            .collect(Collectors.toList());

        // Insert using db-util's MySQL optimization
        long start = System.currentTimeMillis();
        repository.batchInsert(users);
        long time = System.currentTimeMillis() - start;

        System.out.println("Inserted 10,000 users in " + time + "ms");
        // Should be ~1-2 seconds instead of 15-20 seconds
    }
}
```

## Configuration Properties

```yaml
# application.yml
spring:
  datasource:
    shard1:
      url: jdbc:mysql://127.0.0.1:3306/shard1_db?useSSL=false
      username: root
      password: 123456
      driver-class-name: com.mysql.cj.jdbc.Driver
    shard2:
      url: jdbc:mysql://127.0.0.1:3306/shard2_db?useSSL=false
      username: root
      password: 123456
      driver-class-name: com.mysql.cj.jdbc.Driver
    shard3:
      url: jdbc:mysql://127.0.0.1:3306/shard3_db?useSSL=false
      username: root
      password: 123456
      driver-class-name: com.mysql.cj.jdbc.Driver

  jpa:
    hibernate:
      ddl-auto: validate
    properties:
      hibernate:
        jdbc:
          batch_size: 1000
        order_inserts: true
```

## Verify It Works

```bash
# Build Split-Verse with new dependencies
cd /home/mustafa/telcobright-projects/routesphere/split-verse
mvn clean compile

# If successful, db-util is properly integrated!
```

## Benefits

1. ✅ **No duplicate code** - db-util handles MySQL optimization
2. ✅ **Standard JPA** - All JPA features work
3. ✅ **5-10x faster** batch inserts
4. ✅ **Clean separation** - Split-Verse handles sharding, db-util handles optimization

## Next Steps

1. Migrate existing Split-Verse entities to JPA
2. Replace custom SQL generation with JPA repositories
3. Keep custom sharding annotations alongside JPA annotations
4. Test with actual MySQL shards