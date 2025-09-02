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
import com.telcobright.db.logging.Logger;
import com.telcobright.db.logging.ConsoleLogger;

/**
 * Generic Multi-Table Repository implementation
 * Creates separate tables for each day based on sharding key
 * Each daily table is internally partitioned by hour (24 partitions per day)
 * 
 * Table Structure:
 * - Daily tables: database.tablePrefix_YYYYMMDD (e.g., test.sms_20250807)
 * - Hourly partitions within each table: h00, h01, h02, ..., h23
 * - Partitioning function: HOUR(sharding_column)
 * 
 * Entities must implement ShardingEntity<K> to ensure they have
 * required 'id' and 'created_at' fields.
 * 
 * @param <T> Entity type that implements ShardingEntity<K>
 * @param <K> Primary key type
 */
public class GenericMultiTableRepository<T extends ShardingEntity<K>, K> implements ShardingRepository<T, K> {
    private final Logger logger;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    private final ConnectionProvider connectionProvider;
    private final String database;
    private final String tablePrefix;
    private final int partitionRetentionPeriod;
    private final boolean autoManagePartitions;
    private final LocalTime partitionAdjustmentTime;
    private final boolean initializePartitionsOnStart;
    private final EntityMetadata<T, K> metadata;
    private final Class<T> entityClass;
    private final Class<K> keyClass;
    private final MonitoringService monitoringService;
    
    private ScheduledExecutorService scheduler;
    private volatile long lastMaintenanceRun = 0;
    private static final long MAINTENANCE_COOLDOWN_MS = 60000; // 1 minute minimum between maintenance runs
    
    private GenericMultiTableRepository(Builder<T, K> builder) {
        this.database = builder.database;
        this.partitionRetentionPeriod = builder.partitionRetentionPeriod;
        this.autoManagePartitions = builder.autoManagePartitions;
        this.partitionAdjustmentTime = builder.partitionAdjustmentTime;
        this.initializePartitionsOnStart = builder.initializePartitionsOnStart;
        this.entityClass = builder.entityClass;
        this.keyClass = builder.keyClass;
        
        // Initialize entity metadata (performs reflection once)
        this.metadata = new EntityMetadata<>(entityClass, keyClass);
        
        // Use provided table prefix or derive from entity
        this.tablePrefix = builder.tablePrefix != null ? builder.tablePrefix : metadata.getTableName();
        
        // Initialize logger
        this.logger = builder.logger != null ? builder.logger : 
            new ConsoleLogger("MultiTableRepo." + tablePrefix);
        
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
            RepositoryMetrics metrics = new RepositoryMetrics("MultiTable", tablePrefix, 
                    builder.monitoringConfig.getInstanceId());
            MetricsCollector metricsCollector = new MetricsCollector(connectionProvider, database);
            this.monitoringService = new DefaultMonitoringService(builder.monitoringConfig, metrics, metricsCollector);
            this.monitoringService.start();
        } else {
            this.monitoringService = null;
        }
        
        // Initialize tables for the retention period on startup
        if (initializePartitionsOnStart) {
            try {
                initializeTablesForRetentionPeriod();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to initialize tables for retention period", e);
            }
        }
        
        // Initialize scheduler for maintenance tasks
        if (autoManagePartitions) {
            // Create scheduler for both scheduled and on-demand maintenance
            scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread thread = new Thread(r, "MultiTableRepository-Maintenance-" + tablePrefix);
                thread.setDaemon(true);
                return thread;
            });
            
            // Only schedule periodic runs if a specific time is configured
            if (partitionAdjustmentTime != null) {
                schedulePeriodicMaintenance();
            }
        }
    }
    
    /**
     * Insert entity into appropriate daily table
     * Note: Target table must exist (created during startup), otherwise SQLException will be thrown
     */
    @Override
    public void insert(T entity) throws SQLException {
        LocalDateTime shardingKeyValue = metadata.getShardingKeyValue(entity);
        String tableName = getTableName(shardingKeyValue);
        String sql = String.format(metadata.getInsertSQL(), tableName);
        
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
     */
    @Override
    public void insertMultiple(List<T> entities) throws SQLException {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        
        // Group entities by date for batch insertion
        Map<LocalDateTime, List<T>> entitiesByDate = new HashMap<>();
        for (T entity : entities) {
            LocalDateTime shardingKeyValue = metadata.getShardingKeyValue(entity);
            LocalDateTime dateKey = shardingKeyValue.toLocalDate().atStartOfDay();
            entitiesByDate.computeIfAbsent(dateKey, k -> new ArrayList<>()).add(entity);
        }
        
        // Insert batches for each date
        for (Map.Entry<LocalDateTime, List<T>> entry : entitiesByDate.entrySet()) {
            LocalDateTime date = entry.getKey();
            List<T> batchEntities = entry.getValue();
            
            String tableName = getTableName(date);
            String sql = String.format(metadata.getInsertSQL(), tableName);
            
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
                
                for (T entity : batchEntities) {
                    metadata.setInsertParameters(stmt, entity);
                    stmt.addBatch();
                }
                
                stmt.executeBatch();
                
                // Set generated IDs if applicable
                if (metadata.getIdField() != null && metadata.getIdField().isAutoGenerated()) {
                    try (ResultSet keys = stmt.getGeneratedKeys()) {
                        for (T entity : batchEntities) {
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
    }
    
    /**
     * Find all entities by date range
     */
    @Override
    public List<T> findAllByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        
        String query = QueryDSL.select()
            .column("*")
            .from(tablePrefix)  // QueryDSL expects just the prefix, PartitionedQueryBuilder handles full table names
            .where(w -> w.dateRange(shardingColumn, startDate, endDate))
            .buildPartitioned(database, startDate, endDate);
        
        return executeQuery(query, null, rs -> metadata.mapResultSet(rs));
    }
    
    /**
     * Find entity by ID across all tables
     */
    @Override
    public T findById(K id) throws SQLException {
        List<String> tables = getExistingTables();
        
        for (String table : tables) {
            String sql = String.format(metadata.getSelectByIdSQL(), table);
            
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                setIdParameter(stmt, 1, id);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return metadata.mapResultSet(rs);
                    }
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
        // This method returns the first entity found in the date range
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
        
        List<T> results = new ArrayList<>();
        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        
        // Build list of relevant tables based on date range
        List<String> relevantTables = new ArrayList<>();
        LocalDateTime current = startDate.toLocalDate().atStartOfDay();
        while (!current.isAfter(endDate)) {
            String tableName = getTableName(current);
            if (tableExists(tableName)) {
                relevantTables.add(tableName);
            }
            current = current.plusDays(1);
        }
        
        // Search for IDs in relevant tables
        for (String table : relevantTables) {
            if (ids.isEmpty()) break; // All IDs found
            
            // Create IN clause with placeholders
            String placeholders = String.join(",", Collections.nCopies(ids.size(), "?"));
            String sql = String.format("SELECT * FROM %s WHERE %s IN (%s) AND %s >= ? AND %s <= ?", 
                                     table, idColumn, placeholders, shardingColumn, shardingColumn);
            
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
        }
        
        return results;
    }
    
    /**
     * Find all entities before a specific date
     */
    @Override
    public List<T> findAllBeforeDate(LocalDateTime beforeDate) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        List<T> results = new ArrayList<>();
        
        // Get all existing tables
        List<String> tables = getExistingTables();
        
        for (String table : tables) {
            // Extract date from table name
            String dateStr = table.substring(table.lastIndexOf('_') + 1);
            LocalDateTime tableDate = LocalDateTime.parse(dateStr + "000000", 
                DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            
            // Skip tables after the cutoff date
            if (tableDate.isAfter(beforeDate)) {
                continue;
            }
            
            String sql = String.format("SELECT * FROM %s WHERE %s < ?", table, shardingColumn);
            
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                stmt.setTimestamp(1, Timestamp.valueOf(beforeDate));
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        results.add(metadata.mapResultSet(rs));
                    }
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
        List<T> results = new ArrayList<>();
        
        // Get all existing tables
        List<String> tables = getExistingTables();
        
        for (String table : tables) {
            // Extract date from table name
            String dateStr = table.substring(table.lastIndexOf('_') + 1);
            LocalDateTime tableDate = LocalDateTime.parse(dateStr + "000000", 
                DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            
            // Skip tables before the cutoff date
            if (tableDate.isBefore(afterDate)) {
                continue;
            }
            
            String sql = String.format("SELECT * FROM %s WHERE %s > ?", table, shardingColumn);
            
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                stmt.setTimestamp(1, Timestamp.valueOf(afterDate));
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        results.add(metadata.mapResultSet(rs));
                    }
                }
            }
        }
        
        return results;
    }
    
    /**
     * Update entity by primary key across all tables
     */
    @Override
    public void updateById(K id, T entity) throws SQLException {
        // First, find which table contains this ID
        T existingEntity = findById(id);
        if (existingEntity == null) {
            throw new SQLException("Entity with ID " + id + " not found in any table");
        }
        
        // Get the sharding key value from the existing entity to determine the table
        LocalDateTime shardingKeyValue = metadata.getShardingKeyValue(existingEntity);
        String tableName = getTableName(shardingKeyValue);
        
        // Generate and execute UPDATE statement
        String sql = String.format(metadata.getUpdateByIdSQL(), tableName);
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            metadata.setUpdateParameters(stmt, entity, id);
            
            int rowsUpdated = stmt.executeUpdate();
            if (rowsUpdated == 0) {
                throw new SQLException("No rows updated for ID: " + id);
            }
        }
    }
    
    /**
     * Update entity by primary key within a specific date range
     */
    @Override
    public void updateByIdAndDateRange(K id, T entity, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        
        // Get tables that could contain data in this date range
        List<String> tables = getTablesForDateRange(startDate, endDate);
        
        boolean updated = false;
        for (String table : tables) {
            // Try to update in each relevant table
            String sql = String.format(metadata.getUpdateByIdSQL() + " AND %s >= ? AND %s <= ?", 
                                     table, shardingColumn, shardingColumn);
            
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                metadata.setUpdateParameters(stmt, entity, id);
                // Add date range parameters
                int paramCount = stmt.getParameterMetaData().getParameterCount();
                stmt.setTimestamp(paramCount - 1, Timestamp.valueOf(startDate));
                stmt.setTimestamp(paramCount, Timestamp.valueOf(endDate));
                
                int rowsUpdated = stmt.executeUpdate();
                if (rowsUpdated > 0) {
                    updated = true;
                    break; // ID should be unique across all tables
                }
            } catch (SQLException e) {
                // Table might not exist, continue to next
                if (!e.getMessage().contains("doesn't exist")) {
                    throw e;
                }
            }
        }
        
        if (!updated) {
            throw new SQLException("Entity with ID " + id + " not found in date range " + 
                                 startDate + " to " + endDate);
        }
    }
    
    /**
     * Find one entity with ID greater than the specified ID
     * Scans all tables chronologically to find the first entity with ID > specified ID
     */
    @Override
    public T findOneByIdGreaterThan(K id) throws SQLException {
        // Get all existing tables and sort them chronologically (oldest first)
        List<String> tables = getExistingTables();
        Collections.sort(tables); // Tables are named with dates, so alphabetical sort gives chronological order
        
        String idColumn = metadata.getIdField().getColumnName();
        
        // Scan each table chronologically to find the first entity with ID > specified ID
        for (String tableName : tables) {
            String sql = String.format(
                "SELECT * FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT 1",
                tableName, idColumn, idColumn
            );
            
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                setIdParameter(stmt, 1, id);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return metadata.mapResultSet(rs);
                    }
                }
            } catch (SQLException e) {
                // Table might not exist or be accessible, continue to next
                if (!e.getMessage().contains("doesn't exist")) {
                    logger.warn("Error querying table " + tableName + ": " + e.getMessage());
                }
            }
        }
        
        return null; // No entity found with ID greater than specified ID in any table
    }
    
    private List<String> getTablesForDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        List<String> tables = new ArrayList<>();
        LocalDateTime current = startDate.toLocalDate().atStartOfDay();
        
        while (!current.isAfter(endDate)) {
            String tableName = getTableName(current);
            tables.add(tableName);
            current = current.plusDays(1);
        }
        
        return tables;
    }
    
    
    private String getTableName(LocalDateTime date) {
        return database + "." + tablePrefix + "_" + date.format(DATE_FORMAT);
    }
    
    private void createTableIfNotExists(String tableName) throws SQLException {
        String createSQL = String.format(metadata.getCreateTableSQL(), tableName);
        
        // Add hourly partitioning for multi-table repositories
        // Each daily table is internally partitioned by hour (24 partitions per day)
        createSQL = addHourlyPartitioning(createSQL, tableName);
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(createSQL);
            logger.info("Ensured table exists with hourly partitioning: " + tableName);
        }
    }
    
    /**
     * Add hourly partitioning to a daily table
     * Each daily table gets 24 hour partitions (h00, h01, h02, ..., h23)
     */
    private String addHourlyPartitioning(String baseCreateSQL, String tableName) throws SQLException {
        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        
        // For multi-table repositories, MySQL requires the partitioning column to be part of PRIMARY KEY
        // Modify PRIMARY KEY to include sharding column  
        baseCreateSQL = baseCreateSQL.replace(idColumn + " BIGINT PRIMARY KEY AUTO_INCREMENT", 
                                             idColumn + " BIGINT AUTO_INCREMENT");
        
        // Replace the ENGINE clause with partitioning definition
        baseCreateSQL = baseCreateSQL.replace(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", 
                ", PRIMARY KEY (" + idColumn + ", " + shardingColumn + ")) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n" +
                "PARTITION BY RANGE (HOUR(" + shardingColumn + ")) (");
        
        // Add 24 hour partitions (h00 to h23)
        StringBuilder partitions = new StringBuilder();
        for (int hour = 0; hour < 24; hour++) {
            if (hour > 0) {
                partitions.append(",\n");
            }
            partitions.append(String.format("  PARTITION h%02d VALUES LESS THAN (%d)", hour, hour + 1));
        }
        
        baseCreateSQL += partitions.toString() + "\n)";
        
        return baseCreateSQL;
    }
    
    private List<String> getExistingTables() throws SQLException {
        String pattern = tablePrefix + "_%";
        String sql = "SELECT CONCAT(table_schema, '.', table_name) as full_name " +
                    "FROM information_schema.tables " +
                    "WHERE table_schema = ? AND table_name LIKE ? " +
                    "ORDER BY table_name DESC";
        
        List<String> tables = new ArrayList<>();
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, database);
            stmt.setString(2, pattern);
            
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    tables.add(rs.getString("full_name"));
                }
            }
        }
        
        return tables;
    }
    
    private boolean tableExists(String tableName) throws SQLException {
        String sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
        String[] parts = tableName.split("\\.");
        String schema = parts[0];
        String table = parts[1];
        
        try (Connection conn = connectionProvider.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, schema);
            stmt.setString(2, table);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next();
            }
        }
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
        long startTime = System.currentTimeMillis();
        int tablesCreated = 0;
        int tablesDeleted = 0;
        
        // Record maintenance job start
        if (monitoringService != null) {
            monitoringService.recordMaintenanceJobStart();
        }
        
        // Use MaintenanceConnection to get exclusive access during maintenance
        try (MaintenanceConnection maintenanceConn = connectionProvider.getMaintenanceConnection(
                "Automatic partition maintenance for " + tablePrefix)) {
            
            logger.info("Starting automatic maintenance for " + tablePrefix);
            
            LocalDateTime startDate = referenceDate.minusDays(partitionRetentionPeriod);
            LocalDateTime endDate = referenceDate.plusDays(partitionRetentionPeriod);
            
            // Count existing tables before creation
            List<String> beforeTables = getExistingTables();
            createTablesForDateRange(startDate, endDate);
            List<String> afterTables = getExistingTables();
            tablesCreated = afterTables.size() - beforeTables.size();
            
            // Count tables before deletion
            List<String> beforeDeletion = getExistingTables();
            LocalDateTime cutoffDate = referenceDate.minusDays(partitionRetentionPeriod);
            dropOldTables(cutoffDate);
            List<String> afterDeletion = getExistingTables();
            tablesDeleted = beforeDeletion.size() - afterDeletion.size();
            
            long duration = System.currentTimeMillis() - startTime;
            
            logger.info(String.format("Maintenance completed: created %d tables, deleted %d tables in %d ms", 
                        tablesCreated, tablesDeleted, duration));
            
            // Record success
            if (monitoringService != null) {
                monitoringService.recordMaintenanceJobSuccess(duration, tablesCreated, tablesDeleted);
            }
            
        } catch (SQLException e) {
            long duration = System.currentTimeMillis() - startTime;
            logger.error("Maintenance failed: " + e.getMessage());
            if (monitoringService != null) {
                monitoringService.recordMaintenanceJobFailure(duration, e);
            }
            throw e;
        }
    }
    
    public void createTablesForDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        LocalDateTime current = startDate;
        while (!current.isAfter(endDate)) {
            createTableIfNotExists(getTableName(current));
            current = current.plusDays(1);
        }
    }
    
    public void dropOldTables(LocalDateTime cutoffDate) throws SQLException {
        if (!autoManagePartitions) {
            return;
        }
        
        String cutoffDateStr = cutoffDate.format(DATE_FORMAT);
        List<String> tables = getExistingTables();
        
        for (String tableName : tables) {
            String dateStr = tableName.substring(tableName.lastIndexOf('_') + 1);
            if (dateStr.compareTo(cutoffDateStr) < 0) {
                dropTable(tableName);
            }
        }
    }
    
    private void dropTable(String tableName) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + tableName;
        
        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            logger.info("Dropped old table: " + tableName);
        }
    }
    
    /**
     * Initialize all tables needed for the retention period at startup.
     * This ensures all tables exist before any insert operations.
     */
    private void initializeTablesForRetentionPeriod() throws SQLException {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusDays(partitionRetentionPeriod);
        LocalDateTime endDate = now.plusDays(partitionRetentionPeriod);
        
        logger.info(String.format("Initializing tables for retention period: %s to %s (%d days)", 
                    startDate.toLocalDate(), endDate.toLocalDate(), 
                    partitionRetentionPeriod * 2 + 1));
        
        createTablesForDateRange(startDate, endDate);
        
        logger.info("Completed table initialization for retention period");
    }
    
    private void schedulePeriodicMaintenance() {
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
                logger.error("Failed to perform scheduled maintenance: " + e.getMessage());
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
        // Stop monitoring
        if (monitoringService != null) {
            monitoringService.stop();
        }
        
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
        }
    }
    
    /**
     * Get monitoring service for metrics access (if enabled)
     */
    public MonitoringService getMonitoringService() {
        return monitoringService;
    }
    
    
    /**
     * Functional interface for mapping ResultSet to entity
     */
    @FunctionalInterface
    public interface ResultSetMapper<R> {
        R map(ResultSet rs) throws SQLException;
    }
    
    /**
     * Builder for GenericMultiTableRepository
     */
    public static class Builder<T extends ShardingEntity<K>, K> {
        private final Class<T> entityClass;
        private final Class<K> keyClass;
        private String host = "localhost";
        private int port = 3306;
        private String database;
        private String username;
        private String password;
        private String tablePrefix;
        private int partitionRetentionPeriod = 7;
        private boolean autoManagePartitions = true;
        private LocalTime partitionAdjustmentTime = LocalTime.of(4, 0);
        private boolean initializePartitionsOnStart = true;
        private MonitoringConfig monitoringConfig;
        private Logger logger;
        
        
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
        
        public Builder<T, K> tablePrefix(String tablePrefix) {
            this.tablePrefix = tablePrefix;
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
        
        public Builder<T, K> logger(Logger logger) {
            this.logger = logger;
            return this;
        }
        
        
        public GenericMultiTableRepository<T, K> build() {
            if (database == null || username == null || password == null) {
                throw new IllegalStateException("Database, username, and password are required");
            }
            return new GenericMultiTableRepository<>(this);
        }
    }
    
    /**
     * Create a new builder
     */
    public static <T extends ShardingEntity<K>, K> Builder<T, K> builder(Class<T> entityClass, Class<K> keyClass) {
        return new Builder<>(entityClass, keyClass);
    }
}