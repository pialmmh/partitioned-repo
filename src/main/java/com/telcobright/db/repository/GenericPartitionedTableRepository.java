package com.telcobright.db.repository;

import com.telcobright.db.entity.ShardingEntity;
import com.telcobright.db.metadata.EntityMetadata;
import com.telcobright.db.monitoring.*;
import com.telcobright.db.pagination.Page;
import com.telcobright.db.pagination.PageRequest;
import com.telcobright.db.query.QueryDSL;
import com.telcobright.db.connection.ConnectionProvider;
import com.telcobright.db.connection.ConnectionProvider.MaintenanceConnection;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Generic Partitioned Table Repository implementation
 * Uses MySQL native partitioning on a single table
 * 
 * Entities must implement ShardingEntity<K> to ensure they have
 * required 'id' and 'created_at' fields.
 * 
 * @param <T> Entity type that implements ShardingEntity<K>
 * @param <K> Primary key type
 */
public class GenericPartitionedTableRepository<T extends ShardingEntity<K>, K> implements ShardingRepository<T, K> {
    private static final Logger LOGGER = Logger.getLogger(GenericPartitionedTableRepository.class.getName());
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    private final ConnectionProvider connectionProvider;
    private final String database;
    private final String tableName;
    private final int partitionRetentionPeriod;
    private final boolean autoManagePartitions;
    private final LocalTime partitionAdjustmentTime;
    private final boolean initializePartitionsOnStart;
    private final EntityMetadata<T, K> metadata;
    private final Class<T> entityClass;
    private final Class<K> keyClass;
    private final MonitoringService monitoringService;
    
    private ScheduledExecutorService scheduler;
    
    private GenericPartitionedTableRepository(Builder<T, K> builder) {
        this.database = builder.database;
        this.partitionRetentionPeriod = builder.partitionRetentionPeriod;
        this.autoManagePartitions = builder.autoManagePartitions;
        this.partitionAdjustmentTime = builder.partitionAdjustmentTime;
        this.initializePartitionsOnStart = builder.initializePartitionsOnStart;
        this.entityClass = builder.entityClass;
        this.keyClass = builder.keyClass;
        
        // Initialize entity metadata (performs reflection once)
        this.metadata = new EntityMetadata<>(entityClass, keyClass);
        
        // Use provided table name or derive from entity
        this.tableName = builder.tableName != null ? builder.tableName : metadata.getTableName();
        
        // Create ConnectionProvider
        this.connectionProvider = new ConnectionProvider.Builder()
            .host(builder.host)
            .port(builder.port)
            .database(builder.database)
            .username(builder.username)
            .password(builder.password)
            .build();
        
        // Initialize monitoring if enabled
        if (builder.monitoringConfig != null && builder.monitoringConfig.isEnabled()) {
            RepositoryMetrics metrics = new RepositoryMetrics("Partitioned", tableName, 
                    builder.monitoringConfig.getInstanceId());
            MetricsCollector metricsCollector = new MetricsCollector(connectionProvider, database);
            this.monitoringService = new DefaultMonitoringService(builder.monitoringConfig, metrics, metricsCollector);
            this.monitoringService.start();
        } else {
            this.monitoringService = null;
        }
        
        // Initialize table and partitions if needed
        // Initialize table and partitions for retention period on startup
        if (initializePartitionsOnStart) {
            try {
                LOGGER.info("Initializing partitioned table and partitions for retention period...");
                initializeTable();
                initializePartitionsForRetentionPeriod();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to initialize partitioned table", e);
            }
        }
        
        // Start scheduler if auto-management is enabled
        if (autoManagePartitions) {
            startScheduler();
        }
    }
    
    /**
     * Insert entity into partitioned table (MySQL handles routing)
     * Note: Target partition must exist (created during startup), otherwise SQLException will be thrown
     */
    @Override
    public void insert(T entity) throws SQLException {
        String fullTableName = database + "." + tableName;
        String sql = String.format(metadata.getInsertSQL(), fullTableName);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            
            metadata.setInsertParameters(stmt, entity);
            stmt.executeUpdate();
            
            // Set generated ID if applicable
            if (metadata.getIdField() != null && metadata.getIdField().isAutoGenerated()) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    if (keys.next()) {
                        @SuppressWarnings("unchecked")
                        K id = (K) Long.valueOf(keys.getLong(1));
                        metadata.setId(entity, id);
                    }
                }
            }
        }
    }
    
    /**
     * Insert multiple entities
     * Note: Target partitions must exist (created during startup), otherwise SQLException will be thrown
     */
    @Override
    public void insertMultiple(List<T> entities) throws SQLException {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        
        String fullTableName = database + "." + tableName;
        String sql = String.format(metadata.getInsertSQL(), fullTableName);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            
            for (T entity : entities) {
                metadata.setInsertParameters(stmt, entity);
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            
            // Set generated IDs if applicable
            if (metadata.getIdField() != null && metadata.getIdField().isAutoGenerated()) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    for (T entity : entities) {
                        if (keys.next()) {
                            @SuppressWarnings("unchecked")
                            K id = (K) Long.valueOf(keys.getLong(1));
                            metadata.setId(entity, id);
                        }
                    }
                }
            }
        }
    }
    
    /**
     * Find all entities by date range (with partition pruning)
     */
    @Override
    public List<T> findAllByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        String fullTableName = database + "." + tableName;
        
        String sql = String.format("SELECT * FROM %s WHERE %s BETWEEN ? AND ?", 
            fullTableName, shardingColumn);
        
        List<T> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(startDate));
            stmt.setTimestamp(2, Timestamp.valueOf(endDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(metadata.mapResultSet(rs));
                }
            }
        }
        
        return results;
    }
    
    /**
     * Find entity by ID (MySQL scans all partitions)
     */
    @Override
    public T findById(K id) throws SQLException {
        String fullTableName = database + "." + tableName;
        String sql = String.format(metadata.getSelectByIdSQL(), fullTableName);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            setIdParameter(stmt, 1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return metadata.mapResultSet(rs);
                }
            }
        }
        
        return null;
    }
    
    /**
     * Find entity by ID within a date range
     */
    @Override
    public T findByIdAndDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        // Returns the first entity found in the date range
        List<T> entities = findAllByDateRange(startDate, endDate);
        return entities.isEmpty() ? null : entities.get(0);
    }
    
    /**
     * Find all entities by IDs within a date range
     */
    @Override
    public List<T> findAllByIdsAndDateRange(List<K> ids, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        
        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        String fullTableName = database + "." + tableName;
        
        // Create IN clause with placeholders
        String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
        String sql = String.format("SELECT * FROM %s WHERE %s IN (%s) AND %s >= ? AND %s <= ?", 
                                 fullTableName, idColumn, placeholders, shardingColumn, shardingColumn);
        
        List<T> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            // Set ID parameters
            int paramIndex = 1;
            for (K id : ids) {
                setIdParameter(stmt, paramIndex++, id);
            }
            
            // Set date range parameters
            stmt.setTimestamp(paramIndex++, Timestamp.valueOf(startDate));
            stmt.setTimestamp(paramIndex, Timestamp.valueOf(endDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(metadata.mapResultSet(rs));
                }
            }
        }
        
        return results;
    }
    
    /**
     * Find all entities before a specific date
     */
    @Override
    public List<T> findAllBeforeDate(LocalDateTime beforeDate) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        String fullTableName = database + "." + tableName;
        
        String sql = String.format("SELECT * FROM %s WHERE %s < ?", fullTableName, shardingColumn);
        
        List<T> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(beforeDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(metadata.mapResultSet(rs));
                }
            }
        }
        
        return results;
    }
    
    /**
     * Find all entities after a specific date
     */
    @Override
    public List<T> findAllAfterDate(LocalDateTime afterDate) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        String fullTableName = database + "." + tableName;
        
        String sql = String.format("SELECT * FROM %s WHERE %s > ?", fullTableName, shardingColumn);
        
        List<T> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(afterDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(metadata.mapResultSet(rs));
                }
            }
        }
        
        return results;
    }
    
    /**
     * Update entity by primary key in partitioned table
     */
    @Override
    public void updateById(K id, T entity) throws SQLException {
        String fullTableName = database + "." + tableName;
        String sql = String.format(metadata.getUpdateByIdSQL(), fullTableName);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            metadata.setUpdateParameters(stmt, entity, id);
            
            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated == 0) {
                throw new SQLException("Entity with ID " + id + " not found");
            }
        }
    }
    
    /**
     * Update entity by primary key within a specific date range
     */
    @Override
    public void updateByIdAndDateRange(K id, T entity, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String fullTableName = database + "." + tableName;
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        
        // Add date range to the WHERE clause for partition pruning
        String sql = String.format(metadata.getUpdateByIdSQL() + " AND %s >= ? AND %s <= ?",
                                 fullTableName, shardingColumn, shardingColumn);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            metadata.setUpdateParameters(stmt, entity, id);
            
            // Add date range parameters
            int paramCount = stmt.getParameterMetaData().getParameterCount();
            stmt.setTimestamp(paramCount - 1, Timestamp.valueOf(startDate));
            stmt.setTimestamp(paramCount, Timestamp.valueOf(endDate));
            
            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated == 0) {
                throw new SQLException("Entity with ID " + id + " not found in date range " + 
                                     startDate + " to " + endDate);
            }
        }
    }
    
    private void initializeTable() throws SQLException {
        String fullTableName = database + "." + tableName;
        String createSQL = String.format(metadata.getCreateTableSQL(), fullTableName);
        
        // For partitioned tables, MySQL requires the partitioning column to be part of PRIMARY KEY
        // Modify PRIMARY KEY to include sharding column
        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        
        createSQL = createSQL.replace(idColumn + " BIGINT PRIMARY KEY AUTO_INCREMENT", 
                                     idColumn + " BIGINT AUTO_INCREMENT");
        
        createSQL = createSQL.replace(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", 
                ", PRIMARY KEY (" + idColumn + ", " + shardingColumn + ")) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\nPARTITION BY RANGE (TO_DAYS(" + shardingColumn + "))");
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            
            // Check if table exists
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet rs = metaData.getTables(database, null, tableName, null)) {
                if (!rs.next()) {
                    // Table doesn't exist, create it with initial partition
                    LocalDateTime now = LocalDateTime.now();
                    String partitionName = "p" + now.format(DATE_FORMAT);
                    
                    // Create partition for current date + 1 day (VALUES LESS THAN is exclusive)
                    LocalDateTime nextDay = now.plusDays(1);
                    String partitionValue = "TO_DAYS('" + nextDay.toLocalDate().toString() + "')";
                    
                    createSQL += " (\n  PARTITION " + partitionName + 
                                " VALUES LESS THAN (" + partitionValue + ")\n)";
                    
                    // Debug: Log the SQL being executed
                    LOGGER.info("Executing CREATE TABLE SQL: " + createSQL);
                    
                    stmt.execute(createSQL);
                    LOGGER.info("Created partitioned table: " + fullTableName);
                }
            }
        }
    }
    
    
    private boolean partitionExists(String partitionName) throws SQLException {
        String sql = "SELECT partition_name FROM information_schema.partitions " +
                    "WHERE table_schema = ? AND table_name = ? AND partition_name = ?";
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, database);
            stmt.setString(2, tableName);
            stmt.setString(3, partitionName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
    }
    
    private void createPartition(String partitionName, LocalDateTime date) throws SQLException {
        // Create partition for the next day (VALUES LESS THAN is exclusive)
        LocalDateTime nextDay = date.plusDays(1);
        String partitionValue = "TO_DAYS('" + nextDay.toLocalDate().toString() + "')";
        
        String sql = String.format("ALTER TABLE %s.%s ADD PARTITION (PARTITION %s VALUES LESS THAN (%s))",
            database, tableName, partitionName, partitionValue);
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            LOGGER.info("Created partition: " + partitionName);
        }
    }
    
    private void dropPartition(String partitionName) throws SQLException {
        String sql = String.format("ALTER TABLE %s.%s DROP PARTITION %s",
            database, tableName, partitionName);
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            LOGGER.info("Dropped partition: " + partitionName);
        }
    }
    
    private List<String> getPartitions() throws SQLException {
        String sql = "SELECT partition_name FROM information_schema.partitions " +
                    "WHERE table_schema = ? AND table_name = ? AND partition_name IS NOT NULL " +
                    "ORDER BY partition_ordinal_position";
        
        List<String> partitions = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, database);
            stmt.setString(2, tableName);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    partitions.add(rs.getString("partition_name"));
                }
            }
        }
        
        return partitions;
    }
    
    private void setIdParameter(PreparedStatement stmt, int index, K id) throws SQLException {
        if (keyClass == Long.class) {
            stmt.setLong(index, (Long) id);
        } else if (keyClass == String.class) {
            stmt.setString(index, (String) id);
        } else if (keyClass == Integer.class) {
            stmt.setInt(index, (Integer) id);
        } else {
            stmt.setObject(index, id);
        }
    }
    
    private <R> List<R> executeQuery(String sql, Map<String, Object> parameters, 
                                    ResultSetMapper<R> mapper) throws SQLException {
        List<R> results = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            if (parameters != null) {
                int index = 1;
                for (Object value : parameters.values()) {
                    stmt.setObject(index++, value);
                }
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(mapper.map(rs));
                }
            }
        }
        
        return results;
    }
    
    private void performAutomaticMaintenance(LocalDateTime referenceDate) throws SQLException {
        // Use MaintenanceConnection to get exclusive access during maintenance
        try (MaintenanceConnection maintenanceConn = connectionProvider.getMaintenanceConnection(
                "Automatic partition maintenance for " + tableName)) {
            
            LOGGER.info("Starting automatic partition maintenance for " + tableName);
            
            LocalDateTime startDate = referenceDate.minusDays(partitionRetentionPeriod);
            LocalDateTime endDate = referenceDate.plusDays(partitionRetentionPeriod);
            
            createPartitionsForDateRange(startDate, endDate);
            
            LocalDateTime cutoffDate = referenceDate.minusDays(partitionRetentionPeriod);
            dropOldPartitions(cutoffDate);
            
            LOGGER.info("Partition maintenance completed for " + tableName);
        }
    }
    
    public void createPartitionsForDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        LocalDateTime current = startDate;
        while (!current.isAfter(endDate)) {
            String partitionName = "p" + current.format(DATE_FORMAT);
            if (!partitionExists(partitionName)) {
                createPartition(partitionName, current);
            }
            current = current.plusDays(1);
        }
    }
    
    public void dropOldPartitions(LocalDateTime cutoffDate) throws SQLException {
        if (!autoManagePartitions) {
            return;
        }
        
        String cutoffDateStr = cutoffDate.format(DATE_FORMAT);
        List<String> partitions = getPartitions();
        
        for (String partitionName : partitions) {
            if (partitionName.startsWith("p") && partitionName.length() > 1) {
                String dateStr = partitionName.substring(1);
                if (dateStr.compareTo(cutoffDateStr) < 0) {
                    dropPartition(partitionName);
                }
            }
        }
    }
    
    /**
     * Initialize all partitions needed for the retention period at startup.
     * This ensures all partitions exist before any insert operations.
     */
    private void initializePartitionsForRetentionPeriod() throws SQLException {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusDays(partitionRetentionPeriod);
        LocalDateTime endDate = now.plusDays(partitionRetentionPeriod);
        
        LOGGER.info(String.format("Initializing partitions for retention period: %s to %s (%d days)", 
                    startDate.toLocalDate(), endDate.toLocalDate(), 
                    partitionRetentionPeriod * 2 + 1));
        
        createPartitionsForDateRange(startDate, endDate);
        
        LOGGER.info("Completed partition initialization for retention period");
    }
    
    private void startScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "PartitionedTableRepository-Scheduler-" + tableName);
            thread.setDaemon(true);
            return thread;
        });
        
        scheduleNextRun();
    }
    
    private void scheduleNextRun() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime nextRun = now.toLocalDate().plusDays(1).atTime(partitionAdjustmentTime);
        
        if (nextRun.isBefore(now)) {
            nextRun = nextRun.plusDays(1);
        }
        
        long delay = java.time.Duration.between(now, nextRun).toMillis();
        
        scheduler.schedule(() -> {
            try {
                performScheduledMaintenance();
                scheduleNextRun();
            } catch (Exception e) {
                LOGGER.severe("Failed to perform scheduled maintenance: " + e.getMessage());
                scheduleNextRun();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }
    
    private void performScheduledMaintenance() throws SQLException {
        LocalDateTime now = LocalDateTime.now();
        performAutomaticMaintenance(now);
    }
    
    @Override
    public void shutdown() {
        // Shutdown scheduler first
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(60, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // Shutdown ConnectionProvider
        if (connectionProvider != null) {
            connectionProvider.shutdown();
            LOGGER.info("ConnectionProvider shutdown");
        }
    }
    
    
    /**
     * Functional interface for mapping ResultSet to entity
     */
    @FunctionalInterface
    public interface ResultSetMapper<R> {
        R map(ResultSet rs) throws SQLException;
    }
    
    /**
     * Builder for GenericPartitionedTableRepository
     */
    public static class Builder<T extends ShardingEntity<K>, K> {
        private final Class<T> entityClass;
        private final Class<K> keyClass;
        private String host = "localhost";
        private int port = 3306;
        private String database;
        private String username;
        private String password;
        private String tableName;
        private int partitionRetentionPeriod = 7;
        private boolean autoManagePartitions = true;
        private LocalTime partitionAdjustmentTime = LocalTime.of(4, 0);
        private boolean initializePartitionsOnStart = true;
        private MonitoringConfig monitoringConfig;
        
        
        public Builder(Class<T> entityClass, Class<K> keyClass) {
            this.entityClass = entityClass;
            this.keyClass = keyClass;
        }
        
        public Builder<T, K> host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder<T, K> port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder<T, K> database(String database) {
            this.database = database;
            return this;
        }
        
        public Builder<T, K> username(String username) {
            this.username = username;
            return this;
        }
        
        public Builder<T, K> password(String password) {
            this.password = password;
            return this;
        }
        
        public Builder<T, K> tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }
        
        public Builder<T, K> partitionRetentionPeriod(int days) {
            this.partitionRetentionPeriod = days;
            return this;
        }
        
        public Builder<T, K> autoManagePartitions(boolean enable) {
            this.autoManagePartitions = enable;
            return this;
        }
        
        public Builder<T, K> partitionAdjustmentTime(int hour, int minute) {
            this.partitionAdjustmentTime = LocalTime.of(hour, minute);
            return this;
        }
        
        public Builder<T, K> partitionAdjustmentTime(LocalTime time) {
            this.partitionAdjustmentTime = time;
            return this;
        }
        
        public Builder<T, K> initializePartitionsOnStart(boolean initialize) {
            this.initializePartitionsOnStart = initialize;
            return this;
        }
        
        public Builder<T, K> monitoring(MonitoringConfig monitoringConfig) {
            this.monitoringConfig = monitoringConfig;
            return this;
        }
        
        
        public GenericPartitionedTableRepository<T, K> build() {
            if (database == null || username == null || password == null) {
                throw new IllegalStateException("Database, username, and password are required");
            }
            return new GenericPartitionedTableRepository<>(this);
        }
    }
    
    /**
     * Create a new builder
     */
    public static <T extends ShardingEntity<K>, K> Builder<T, K> builder(Class<T> entityClass, Class<K> keyClass) {
        return new Builder<>(entityClass, keyClass);
    }
}