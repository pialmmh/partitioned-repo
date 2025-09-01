# Standalone Library Architecture

## 100% Framework-Independent Design

This library is a **pure Java library** with zero framework dependencies and builder-only configuration.

### Core Principles

1. **No Configuration Files**: No properties, YAML, XML, or JSON files required
2. **Builder-Only Pattern**: All components configured programmatically via builders
3. **No Framework Dependencies**: No Spring, Quarkus, Jakarta EE, or any framework
4. **No Annotations**: No framework-specific annotations (@Component, @Service, etc.)
5. **No Static Configuration**: No singletons or static configuration holders
6. **No Resource Loading**: No classpath scanning or resource file loading

### Verification Checklist

✅ **No properties files** - `src/main/resources` is empty  
✅ **Private constructors** - Can only instantiate via builders  
✅ **No framework imports** - Pure Java + MySQL connector only  
✅ **No DI annotations** - No @Inject, @Autowired, @Component  
✅ **No static config** - All configuration passed via builders  
✅ **No resource loading** - No getResourceAsStream or ClassLoader usage  

### Builder-Only Configuration

```java
// The ONLY way to create repositories
GenericPartitionedTableRepository<Order, Long> repo = 
    GenericPartitionedTableRepository.<Order, Long>builder(Order.class, Long.class)
        .host("localhost")        // Explicit configuration
        .port(3306)              // No hidden defaults
        .database("mydb")        // Required parameters
        .username("user")        // Must be provided
        .password("pass")        // Cannot be null
        .build();                // Returns configured instance
```

### Framework Integration Pattern

When using with frameworks, create producer beans using the framework's features:

#### Spring Boot Integration
```java
@Configuration
public class RepositoryConfig {
    
    @Bean
    public GenericPartitionedTableRepository<Order, Long> orderRepository(
            @Value("${db.host}") String host,
            @Value("${db.port}") int port,
            @Value("${db.database}") String database,
            @Value("${db.username}") String username,
            @Value("${db.password}") String password,
            Logger customLogger) {
        
        return GenericPartitionedTableRepository.<Order, Long>builder(Order.class, Long.class)
            .host(host)
            .port(port)
            .database(database)
            .username(username)
            .password(password)
            .logger(customLogger)
            .partitionRetentionPeriod(30)
            .build();
    }
}
```

#### Quarkus Integration
```java
@ApplicationScoped
public class RepositoryProducer {
    
    @ConfigProperty(name = "db.host")
    String host;
    
    @ConfigProperty(name = "db.database")
    String database;
    
    @Inject
    Logger logger;
    
    @Produces
    @ApplicationScoped
    public GenericMultiTableRepository<Sms, Long> smsRepository() {
        return GenericMultiTableRepository.<Sms, Long>builder(Sms.class, Long.class)
            .host(host)
            .database(database)
            .username(System.getenv("DB_USER"))
            .password(System.getenv("DB_PASS"))
            .logger(logger)
            .build();
    }
}
```

#### Plain Java Integration
```java
public class Application {
    public static void main(String[] args) {
        // Read config however you want
        Properties props = loadProperties();
        
        // Create repository
        var repo = GenericPartitionedTableRepository.<Order, Long>builder(Order.class, Long.class)
            .host(props.getProperty("host"))
            .database(props.getProperty("database"))
            .username(props.getProperty("username"))
            .password(props.getProperty("password"))
            .build();
        
        // Use repository
        repo.insert(new Order(...));
    }
}
```

### Benefits of Standalone Design

1. **Zero Lock-in**: Use with any framework or no framework
2. **Clean Testing**: No framework bootstrapping for unit tests
3. **Minimal Dependencies**: Only MySQL connector required
4. **Predictable**: No magic, no hidden configuration
5. **Portable**: Works in any Java environment
6. **Explicit**: All configuration visible in code

### Dependencies

The library has minimal dependencies:

```xml
<!-- Runtime dependencies -->
- mysql-connector-j (MySQL driver)
- jackson (JSON serialization for monitoring)

<!-- Test dependencies only -->
- junit-jupiter
- testcontainers
```

### No Hidden Behavior

- No classpath scanning
- No annotation processing
- No bytecode manipulation
- No reflection for configuration
- No proxy generation
- No AOP/interceptors

Everything is explicit, programmatic, and transparent.