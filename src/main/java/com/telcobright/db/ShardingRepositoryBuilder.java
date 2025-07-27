package com.telcobright.db;

import com.telcobright.db.annotation.ShardingMode;
import com.telcobright.db.metadata.EntityMetadata;
import com.telcobright.db.repository.ShardingRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * Simplified builder for creating ShardingRepository with minimal configuration
 * 
 * User only needs to provide:
 * - Repository type (multi-table or partitioned)
 * - Entity class
 * - MySQL connection parameters
 * 
 * Builder handles:
 * - DataSource creation (HikariCP)
 * - EntityMetadata extraction
 * - ShardingSphere configuration
 * - Repository factory setup
 */
public class ShardingRepositoryBuilder {
    
    private ShardingMode repositoryType;
    private String host = "localhost";
    private int port = 3306;
    private String database;
    private String username;
    private String password;
    private int maxPoolSize = 10;
    private int minIdle = 5;
    private long connectionTimeout = 60000;    // 1 minute for partition operations
    private long idleTimeout = 600000;
    private long maxLifetime = 1800000;
    private String charset = "utf8mb4"; // Default charset
    private String collation = "utf8mb4_unicode_ci"; // Default collation
    
    /**
     * Set repository type (required)
     * @param repositoryType MULTI_TABLE or PARTITIONED_TABLE
     */
    public ShardingRepositoryBuilder type(ShardingMode repositoryType) {
        this.repositoryType = repositoryType;
        return this;
    }
    
    /**
     * Set MySQL host (default: localhost)
     */
    public ShardingRepositoryBuilder host(String host) {
        this.host = host;
        return this;
    }
    
    /**
     * Set MySQL port (default: 3306)
     */
    public ShardingRepositoryBuilder port(int port) {
        this.port = port;
        return this;
    }
    
    /**
     * Set MySQL database name (required)
     */
    public ShardingRepositoryBuilder database(String database) {
        this.database = database;
        return this;
    }
    
    /**
     * Set MySQL username (required)
     */
    public ShardingRepositoryBuilder username(String username) {
        this.username = username;
        return this;
    }
    
    /**
     * Set MySQL password (required)
     */
    public ShardingRepositoryBuilder password(String password) {
        this.password = password;
        return this;
    }
    
    /**
     * Set connection pool max size (default: 10)
     */
    public ShardingRepositoryBuilder maxPoolSize(int maxPoolSize) {
        this.maxPoolSize = maxPoolSize;
        return this;
    }
    
    /**
     * Set the character set for database connections
     * Default: utf8mb4 (full UTF-8 Unicode support)
     */
    public ShardingRepositoryBuilder charset(String charset) {
        this.charset = charset;
        return this;
    }
    
    /**
     * Set the collation for database connections
     * Default: utf8mb4_unicode_ci (case-insensitive Unicode collation)
     */
    public ShardingRepositoryBuilder collation(String collation) {
        this.collation = collation;
        return this;
    }
    
    /**
     * Set connection timeout for long-running operations (e.g., partition management)
     * @param timeoutMillis Timeout in milliseconds (default: 60000 = 1 minute)
     */
    public ShardingRepositoryBuilder connectionTimeout(long timeoutMillis) {
        this.connectionTimeout = timeoutMillis;
        return this;
    }
    
    /**
     * Set connection pool min idle (default: 5)
     */
    public ShardingRepositoryBuilder minIdle(int minIdle) {
        this.minIdle = minIdle;
        return this;
    }
    
    
    /**
     * Set idle timeout in ms (default: 600000)
     */
    public ShardingRepositoryBuilder idleTimeout(long idleTimeout) {
        this.idleTimeout = idleTimeout;
        return this;
    }
    
    /**
     * Set max lifetime in ms (default: 1800000)
     */
    public ShardingRepositoryBuilder maxLifetime(long maxLifetime) {
        this.maxLifetime = maxLifetime;
        return this;
    }
    
    /**
     * Build ShardingRepository for the given entity class
     * Handles all DataSource creation and ShardingSphere configuration internally
     * 
     * @param entityClass The entity class (builder will override @ShardingTable mode with specified type)
     * @return Configured ShardingRepository ready for use
     * @throws SQLException if DataSource creation fails
     * @throws IllegalArgumentException if required parameters are missing
     */
    public <T> ShardingRepository<T> buildRepository(Class<T> entityClass) throws SQLException {
        validateRequiredParameters();
        
        // 1. Create MySQL DataSource using HikariCP
        DataSource actualDataSource = createMySQLDataSource();
        
        // 2. Extract entity metadata and override repository type
        EntityMetadata<T> metadata = EntityMetadata.of(entityClass);
        
        // Override the repository type from builder setting
        if (repositoryType != null) {
            metadata = metadata.withShardingMode(repositoryType);
        }
        
        // 3. Create ShardingSphere DataSource with sharding rules
        DataSource shardingSphereDataSource = ShardingSphereFactory.createShardingSphereDataSource(
            actualDataSource,
            metadata.getTableName(),
            metadata.getShardKey(),
            metadata.getRetentionSpanDays()
        );
        
        // 4. Create factory and return repository
        ShardingRepositoryFactory factory = new ShardingRepositoryFactory(
            actualDataSource, 
            shardingSphereDataSource,
            database
        );
        
        return factory.getRepository(entityClass);
    }
    
    /**
     * Build ShardingRepositoryFactory for managing multiple entities
     * Useful when you need repositories for multiple entity types
     * 
     * @return Configured ShardingRepositoryFactory
     * @throws SQLException if DataSource creation fails
     */
    public ShardingRepositoryFactory buildFactory() throws SQLException {
        validateRequiredParameters();
        
        // Create MySQL DataSource
        DataSource actualDataSource = createMySQLDataSource();
        
        // For factory creation, we need at least one entity to configure ShardingSphere
        // We'll create a minimal ShardingSphere DataSource that can be reconfigured later
        DataSource shardingSphereDataSource = ShardingSphereFactory.createShardingSphereDataSource(
            actualDataSource,
            "default_table", // Will be overridden per entity
            "created_at",    // Common shard key
            7                // Default retention
        );
        
        return new ShardingRepositoryFactory(actualDataSource, shardingSphereDataSource, database);
    }
    
    /**
     * Create MySQL DataSource using HikariCP with configured parameters
     */
    private DataSource createMySQLDataSource() {
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.mysql.cj.jdbc.Driver");
        // MySQL JDBC driver uses "UTF-8" for utf8mb4 in the URL
        String urlCharset = charset.equalsIgnoreCase("utf8mb4") ? "UTF-8" : charset;
        
        config.setJdbcUrl(String.format(
            "jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&characterEncoding=%s&socketTimeout=%d&connectTimeout=%d",
            host, port, database, urlCharset, connectionTimeout, connectionTimeout
        ));
        config.setUsername(username);
        config.setPassword(password);
        
        // Connection pool settings
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(minIdle);
        config.setConnectionTimeout(connectionTimeout);  // 1 minute default for partition ops
        config.setIdleTimeout(idleTimeout);
        config.setMaxLifetime(maxLifetime);
        
        // Set connection init SQL to ensure charset and collation
        config.setConnectionInitSql(String.format(
            "SET NAMES '%s' COLLATE '%s'",
            charset, collation
        ));
        
        return new HikariDataSource(config);
    }
    
    /**
     * Validate that required parameters are set
     */
    private void validateRequiredParameters() {
        if (repositoryType == null) {
            throw new IllegalArgumentException("Repository type is required - use .type(ShardingMode.MULTI_TABLE) or .type(ShardingMode.PARTITIONED_TABLE)");
        }
        if (database == null || database.trim().isEmpty()) {
            throw new IllegalArgumentException("Database name is required");
        }
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("Username is required");
        }
        if (password == null) {
            throw new IllegalArgumentException("Password is required");
        }
    }
    
    /**
     * Create builder for multi-table sharding (separate tables per day)
     * Example: sms_20250727, sms_20250728, etc.
     */
    public static ShardingRepositoryBuilder multiTable() {
        return new ShardingRepositoryBuilder().type(ShardingMode.MULTI_TABLE);
    }
    
    /**
     * Create builder for partitioned table sharding (single table with partitions)
     * Example: event table with event_20250727, event_20250728 partitions
     */
    public static ShardingRepositoryBuilder partitionedTable() {
        return new ShardingRepositoryBuilder().type(ShardingMode.PARTITIONED_TABLE);
    }
    
    /**
     * Generic MySQL builder - requires explicit type() call
     * @deprecated Use multiTable() or partitionedTable() for clearer intent
     */
    @Deprecated
    public static ShardingRepositoryBuilder mysql() {
        return new ShardingRepositoryBuilder();
    }
}