package com.telcobright.core.repository;
import com.telcobright.api.ShardingRepository;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.metadata.EntityMetadata;
import com.telcobright.core.metadata.FieldMetadata;
import com.telcobright.core.monitoring.*;
import com.telcobright.core.pagination.Page;
import com.telcobright.core.pagination.PageRequest;
import com.telcobright.core.query.QueryDSL;
import com.telcobright.core.connection.ConnectionProvider;
import com.telcobright.core.connection.ConnectionProvider.MaintenanceConnection;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import com.telcobright.core.logging.Logger;
import com.telcobright.core.logging.ConsoleLogger;
import com.telcobright.core.enums.PartitionRange;
import com.telcobright.core.enums.PartitionColumnType;
import com.telcobright.core.persistence.PersistenceProvider;
import com.telcobright.core.persistence.PersistenceProviderFactory;

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
 * Entities must implement ShardingEntity to ensure they have
 * required getId/setId and partition value accessor methods.
 *
 * @param <T> Entity type that implements ShardingEntity
 * @param <P> Partition column value type (must be Comparable)
 */
public class GenericMultiTableRepository<T extends ShardingEntity<P>, P extends Comparable<? super P>> implements ShardingRepository<T, P> {
    private final Logger logger;
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * Defines the granularity of table creation
     */
    public enum TableGranularity {
        HOURLY("yyyy_MM_dd_HH"),  // One table per hour
        DAILY("yyyy_MM_dd"),      // One table per day (default)
        MONTHLY("yyyy_MM");       // One table per month

        private final String pattern;

        TableGranularity(String pattern) {
            this.pattern = pattern;
        }

        public DateTimeFormatter getFormatter() {
            return DateTimeFormatter.ofPattern(pattern);
        }
    }
    
    private final ConnectionProvider connectionProvider;
    private final String database;
    private final String tablePrefix;
    private final int partitionRetentionPeriod;
    private final boolean autoManagePartitions;
    private final LocalTime partitionAdjustmentTime;
    private final boolean initializePartitionsOnStart;
    private final EntityMetadata<T> metadata;
    private final Class<T> entityClass;
    private final MonitoringService monitoringService;
    private final TableGranularity tableGranularity;
    private final String charset;
    private final String collation;
    private final PartitionRange partitionRange;
    private final String partitionColumn;
    private final PartitionColumnType partitionColumnType;
    private final PersistenceProvider persistenceProvider;

    // Nested partition support
    private final boolean nestedPartitionsEnabled;
    private final int nestedPartitionCount;

    private ScheduledExecutorService scheduler;
    private volatile long lastMaintenanceRun = 0;
    private static final long MAINTENANCE_COOLDOWN_MS = 60000; // 1 minute minimum between maintenance runs

    // Track min/max partition boundaries for range validation
    private volatile LocalDateTime minPartitionDate;
    private volatile LocalDateTime maxPartitionDate;
    
    private GenericMultiTableRepository(Builder<T, P> builder) {
        this.database = builder.database;
        this.partitionRetentionPeriod = builder.partitionRetentionPeriod;
        this.autoManagePartitions = builder.autoManagePartitions;
        this.partitionAdjustmentTime = builder.partitionAdjustmentTime;
        this.initializePartitionsOnStart = builder.initializePartitionsOnStart;
        this.entityClass = builder.entityClass;
        this.tableGranularity = builder.tableGranularity;
        this.charset = builder.charset;
        this.collation = builder.collation;
        this.partitionRange = builder.partitionRange;
        this.partitionColumn = builder.partitionColumn;
        this.partitionColumnType = builder.partitionColumnType;

        // Initialize nested partition configuration
        this.nestedPartitionsEnabled = builder.nestedPartitionsEnabled;
        this.nestedPartitionCount = builder.nestedPartitionCount;

        // Initialize entity metadata (performs reflection once)
        this.metadata = new EntityMetadata<>(entityClass);
        
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

        // Initialize PersistenceProvider (defaults to MySQL)
        this.persistenceProvider = builder.persistenceProvider != null ?
            builder.persistenceProvider :
            PersistenceProviderFactory.getProvider(PersistenceProvider.DatabaseType.MYSQL);
        
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
     * Follows MySQL partition behavior:
     * - Values below minimum range go into the lowest table
     * - Values above maximum range throw an exception
     */
    @Override
    public void insert(T entity) throws SQLException {
        // Validate partition boundaries for time-based partitioning
        if (partitionRange == null || isTimeBased(partitionRange)) {
            LocalDateTime entityDate = metadata.getShardingKeyValue(entity);
            validateAndAdjustPartitionValue(entityDate);
        }

        String tableName = getTableNameForEntity(entity);

        // Ensure table exists for non-time-based partitioning
        if (!isTimeBased(partitionRange)) {
            createTableIfNotExists(tableName);
        }

        // Use PersistenceProvider for insert
        try (Connection conn = connectionProvider.getConnection()) {
            persistenceProvider.executeInsert(conn, entity, tableName, metadata);
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

        // Validate partition boundaries for all entities first
        if (partitionRange == null || isTimeBased(partitionRange)) {
            for (T entity : entities) {
                LocalDateTime entityDate = metadata.getShardingKeyValue(entity);
                validateAndAdjustPartitionValue(entityDate);
            }
        }

        // Group entities by table for batch insertion
        Map<String, List<T>> entitiesByTable = new HashMap<>();
        for (T entity : entities) {
            String tableName = getTableNameForEntity(entity);
            entitiesByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(entity);
        }

        // Insert batches for each table
        for (Map.Entry<String, List<T>> entry : entitiesByTable.entrySet()) {
            String tableName = entry.getKey();
            List<T> batchEntities = entry.getValue();

            // Ensure table exists for non-time-based partitioning
            if (!isTimeBased(partitionRange)) {
                createTableIfNotExists(tableName);
            }

            // Use PersistenceProvider for optimized bulk insert
            try (Connection conn = connectionProvider.getConnection()) {
                int inserted = persistenceProvider.executeBulkInsert(conn, batchEntities, tableName, metadata);
                logger.debug("Bulk inserted " + inserted + " records into " + tableName);
            }
        }
    }
    
    /**
     * Find all entities by date range
     */
    @Override
    public List<T> findAllByPartitionRange(P startValue, P endValue) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        List<T> results = new ArrayList<>();

        // Get relevant tables for the date range
        List<String> relevantTables = new ArrayList<>();
        if (startValue instanceof LocalDateTime && endValue instanceof LocalDateTime) {
            relevantTables = getTablesForDateRange((LocalDateTime) startValue, (LocalDateTime) endValue);
        } else {
            // For non-date types, query all tables
            relevantTables = getExistingTables();
        }

        // Query each table
        for (String tableName : relevantTables) {
            String sql = String.format("SELECT * FROM %s WHERE %s >= ? AND %s < ?",
                tableName, shardingColumn, shardingColumn);

            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {

                // Set parameters based on type
                if (startValue instanceof LocalDateTime) {
                    stmt.setTimestamp(1, java.sql.Timestamp.valueOf((LocalDateTime) startValue));
                    stmt.setTimestamp(2, java.sql.Timestamp.valueOf((LocalDateTime) endValue));
                } else {
                    stmt.setObject(1, startValue);
                    stmt.setObject(2, endValue);
                }

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        results.add(metadata.mapResultSet(rs));
                    }
                }
            } catch (SQLException e) {
                // Table might not exist, continue to next
                if (!e.getMessage().contains("doesn't exist")) {
                    throw e;
                }
            }
        }

        return results;
    }
    
    /**
     * Find entity by ID across all tables
     */
    @Override
    public T findById(String id) throws SQLException {
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
    public T findByIdAndPartitionColRange(String id, P startValue, P endValue) throws SQLException {
        if (id == null) {
            return null;
        }

        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();

        // Get relevant tables for the date range
        List<String> relevantTables = getTablesForDateRange((LocalDateTime) startValue, (LocalDateTime) endValue);

        // Log for debugging
        // logger.info("Searching for ID " + id + " in date range " + startValue + " to " + endValue);
        // logger.info("Relevant tables: " + relevantTables);

        // Search each table for the entity with matching ID
        for (String tableName : relevantTables) {
            // tableName already includes database prefix from getTablesForDateRange

            // Check if the table exists
            if (!tableExists(tableName)) {
                logger.warn("Table does not exist: " + tableName);
                continue;
            }

            // Query by both ID and date range for optimal partition pruning
            String sql = String.format(
                "SELECT * FROM %s WHERE %s = ? AND %s >= ? AND %s < ?",
                tableName, idColumn, shardingColumn, shardingColumn
            );

            // logger.info("Executing SQL: " + sql);
            // logger.info("Parameters: id=" + id + ", startValue=" + startValue + ", endValue=" + endValue);

            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {

                stmt.setString(1, id);

                // Set date range parameters based on partition column type
                if (startValue instanceof LocalDateTime) {
                    stmt.setTimestamp(2, Timestamp.valueOf((LocalDateTime) startValue));
                    stmt.setTimestamp(3, Timestamp.valueOf((LocalDateTime) endValue));
                } else if (startValue instanceof Long) {
                    stmt.setLong(2, (Long) startValue);
                    stmt.setLong(3, (Long) endValue);
                } else if (startValue instanceof Integer) {
                    stmt.setInt(2, (Integer) startValue);
                    stmt.setInt(3, (Integer) endValue);
                } else if (startValue instanceof String) {
                    stmt.setString(2, (String) startValue);
                    stmt.setString(3, (String) endValue);
                } else {
                    throw new SQLException("Unsupported partition column type: " + startValue.getClass().getName());
                }

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        T result = metadata.mapResultSet(rs);
                        // logger.info("Found result within date range: " + result);
                        return result;
                    } else {
                        // logger.info("No results found in table " + tableName);
                    }
                }
            } catch (SQLException e) {
                // Log and continue to next table
                logger.warn("Error searching table " + tableName + ": " + e.getMessage());
                if (!e.getMessage().contains("doesn't exist") && !e.getMessage().contains("Unknown column")) {
                    throw e;
                }
            }
        }

        return null; // Not found in any table within the date range
    }
    
    /**
     * Find all entities by IDs within a date range
     */
    @Override
    public List<T> findAllByIdsAndPartitionColRange(List<String> ids, P startValue, P endValue) throws SQLException {
        if (ids == null || ids.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<T> results = new ArrayList<>();
        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        
        // Build list of relevant tables based on date range
        List<String> relevantTables = new ArrayList<>();
        // TODO: Handle generic partition value types for table iteration
        if (startValue instanceof LocalDateTime) {
            LocalDateTime current = ((LocalDateTime) startValue).toLocalDate().atStartOfDay();
            while (!current.isAfter((LocalDateTime) endValue)) {
                String tableName = getTableName(current);
                if (tableExists(tableName)) {
                    relevantTables.add(tableName);
                }
                current = current.plusDays(1);
            }
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
                for (String id : ids) {
                    setIdParameter(stmt, paramIndex++, id);
                }
                
                // Set date range parameters
                // TODO: Handle generic partition value types
                if (startValue instanceof LocalDateTime) {
                    stmt.setTimestamp(paramIndex++, Timestamp.valueOf((LocalDateTime) startValue));
                    stmt.setTimestamp(paramIndex, Timestamp.valueOf((LocalDateTime) endValue));
                } else {
                    stmt.setObject(paramIndex++, startValue);
                    stmt.setObject(paramIndex, endValue);
                }
                
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
    public List<T> findAllBeforePartitionValue(P beforeValue) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        List<T> results = new ArrayList<>();

        // Get all existing tables
        List<String> tables = getExistingTables();

        for (String table : tables) {
            // Skip non-time-based tables
            if (!isTimeBased(partitionRange)) {
                continue;
            }

            // Extract and parse date from table name based on partition range
            LocalDateTime tableDateTime = extractTableDateTime(table);
            if (tableDateTime == null) {
                continue;
            }

            // Determine if we should skip this table based on partition range
            // TODO: Handle generic partition value types
            if (beforeValue instanceof LocalDateTime && shouldSkipTableBefore(tableDateTime, (LocalDateTime) beforeValue)) {
                continue;
            }

            // Query the table for records before the specified time
            String sql = String.format("SELECT * FROM %s WHERE %s < ?", table, shardingColumn);

            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {

                // TODO: Handle generic partition value types
                if (beforeValue instanceof LocalDateTime) {
                    stmt.setTimestamp(1, Timestamp.valueOf((LocalDateTime) beforeValue));
                } else {
                    stmt.setObject(1, beforeValue);
                }

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
    public List<T> findAllAfterPartitionValue(P afterValue) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        List<T> results = new ArrayList<>();

        // Get all existing tables
        List<String> tables = getExistingTables();

        for (String table : tables) {
            // Skip non-time-based tables
            if (!isTimeBased(partitionRange)) {
                continue;
            }

            // Extract and parse date from table name based on partition range
            LocalDateTime tableDateTime = extractTableDateTime(table);
            if (tableDateTime == null) {
                continue;
            }

            // Determine if we should skip this table based on partition range
            // TODO: Handle generic partition value types
            if (afterValue instanceof LocalDateTime && shouldSkipTableAfter(tableDateTime, (LocalDateTime) afterValue)) {
                continue;
            }

            // Query the table for records after the specified time
            String sql = String.format("SELECT * FROM %s WHERE %s > ?", table, shardingColumn);

            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {

                // TODO: Handle generic partition value types
                if (afterValue instanceof LocalDateTime) {
                    stmt.setTimestamp(1, Timestamp.valueOf((LocalDateTime) afterValue));
                } else {
                    stmt.setObject(1, afterValue);
                }

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
    public void updateById(String id, T entity) throws SQLException {
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
    public void updateByIdAndPartitionColRange(String id, T entity, P startValue, P endValue) throws SQLException {
        String shardingColumn = metadata.getShardingKeyField().getColumnName();
        String idColumn = metadata.getIdField().getColumnName();

        // Get tables that could contain data in this date range
        List<String> tables = new ArrayList<>();
        if (startValue instanceof LocalDateTime && endValue instanceof LocalDateTime) {
            tables = getTablesForDateRange((LocalDateTime) startValue, (LocalDateTime) endValue);
        } else {
            tables = getExistingTables();
        }

        boolean updated = false;
        for (String table : tables) {
            // Build UPDATE SQL with date range check
            StringBuilder sqlBuilder = new StringBuilder("UPDATE ").append(table).append(" SET ");
            List<FieldMetadata> fields = metadata.getFields();
            boolean first = true;
            for (FieldMetadata field : fields) {
                if (!field.isId()) {
                    if (!first) sqlBuilder.append(", ");
                    sqlBuilder.append(field.getColumnName()).append(" = ?");
                    first = false;
                }
            }
            sqlBuilder.append(" WHERE ").append(idColumn).append(" = ?");
            sqlBuilder.append(" AND ").append(shardingColumn).append(" >= ? AND ").append(shardingColumn).append(" < ?");

            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sqlBuilder.toString())) {

                // Set field values
                int paramIndex = 1;
                for (FieldMetadata field : fields) {
                    if (!field.isId()) {
                        Object value = field.getValue(entity);
                        stmt.setObject(paramIndex++, value);
                    }
                }

                // Set WHERE clause parameters
                stmt.setString(paramIndex++, id);
                if (startValue instanceof LocalDateTime) {
                    stmt.setTimestamp(paramIndex++, Timestamp.valueOf((LocalDateTime) startValue));
                    stmt.setTimestamp(paramIndex, Timestamp.valueOf((LocalDateTime) endValue));
                } else {
                    stmt.setObject(paramIndex++, startValue);
                    stmt.setObject(paramIndex, endValue);
                }

                int rowsUpdated = stmt.executeUpdate();
                if (rowsUpdated > 0) {
                    updated = true;
                    break; // ID should be unique across all tables
                }
            } catch (SQLException e) {
                // Table might not exist, continue to next
                if (!e.getMessage().contains("doesn't exist")) {
                    logger.warn("Error updating in table " + table + ": " + e.getMessage());
                }
            }
        }

        if (!updated) {
            throw new SQLException("No rows updated for ID: " + id);
        }
    }
    
    /**
     * Find one entity with ID greater than the specified ID
     * Scans all tables chronologically to find the first entity with ID > specified ID
     */
    @Override
    public T findOneByIdGreaterThan(String id) throws SQLException {
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
    
    /**
     * Find batch of entities with ID greater than the specified ID
     * Iterates tables chronologically, accumulating results until batchSize is reached
     */
    @Override
    public List<T> findBatchByIdGreaterThan(String id, int batchSize) throws SQLException {
        List<T> results = new ArrayList<>();
        
        // Get all existing tables in chronological order
        List<String> tables = getExistingTables();
        Collections.sort(tables); // Sort in chronological order (oldest first)
        
        String idColumn = metadata.getIdField().getColumnName();
        
        // Iterate through tables chronologically
        for (String tableName : tables) {
            // Check if we've collected enough results
            if (results.size() >= batchSize) {
                break;
            }
            
            // Calculate remaining batch size
            int remaining = batchSize - results.size();
            
            // Build query for this table with remaining limit
            String sql = String.format(
                "SELECT * FROM %s WHERE %s > ? ORDER BY %s ASC LIMIT ?",
                tableName, idColumn, idColumn
            );
            
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                setIdParameter(stmt, 1, id);
                stmt.setInt(2, remaining);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        results.add(metadata.mapResultSet(rs));
                        
                        // Early exit if we've collected enough results
                        if (results.size() >= batchSize) {
                            break;
                        }
                    }
                }
            }
            
            // If we found results in this table and reached batchSize, stop searching
            if (results.size() >= batchSize) {
                break;
            }
        }
        
        return results;
    }
    
    private List<String> getTablesForDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        List<String> tables = new ArrayList<>();
        LocalDateTime current = startDate.toLocalDate().atStartOfDay();
        LocalDateTime endDay = endDate.toLocalDate().atStartOfDay();

        // Include all days from start to end (inclusive)
        while (!current.isAfter(endDay)) {
            String tableName = getTableName(current);
            tables.add(tableName);
            current = current.plusDays(1);
        }

        return tables;
    }
    

    private String getTableName(LocalDateTime date) {
        // For backward compatibility, if partitionRange is not set, use date-based
        if (partitionRange == null || isTimeBased(partitionRange)) {
            return getTableNameForDate(date);
        } else if (isHashBased(partitionRange)) {
            // For hash-based, use the entity's ID to determine bucket
            return getTableNameForHash(date.hashCode());
        } else if (isValueBased(partitionRange)) {
            // For value-based, use sequence number if available
            return getTableNameForValue(0L); // Will be overridden in getTableNameForEntity
        }
        return getTableNameForDate(date);
    }

    /**
     * Validates partition value and throws exception if above maximum.
     * Values below minimum are allowed (will use minimum table).
     */
    private void validateAndAdjustPartitionValue(LocalDateTime entityDate) throws SQLException {
        if (maxPartitionDate != null && entityDate.isAfter(maxPartitionDate)) {
            throw new SQLException(String.format(
                "Partition value %s is beyond maximum partition boundary %s. " +
                "Cannot insert data beyond the configured retention period.",
                entityDate, maxPartitionDate
            ));
        }
        // Values below minimum are allowed - they'll be inserted into the minimum table
    }

    private String getTableNameForEntity(T entity) throws SQLException {
        if (partitionRange == null || isTimeBased(partitionRange)) {
            LocalDateTime entityDate = metadata.getShardingKeyValue(entity);

            // Handle values below minimum partition - use the minimum table
            if (minPartitionDate != null && entityDate.isBefore(minPartitionDate)) {
                logger.debug(String.format(
                    "Entity date %s is before minimum partition %s, using minimum table",
                    entityDate, minPartitionDate
                ));
                return getTableNameForDate(minPartitionDate);
            }

            return getTableNameForDate(entityDate);
        } else if (isHashBased(partitionRange)) {
            // Hash the ID to determine bucket
            int buckets = getHashBuckets(partitionRange);
            int bucket = Math.abs(entity.getId().hashCode()) % buckets;
            return getTableNameForHash(bucket);
        } else if (isValueBased(partitionRange)) {
            // Try to extract a numeric value from the entity
            // First try to get a sequence number if the entity has one
            try {
                // Use reflection to check for common numeric fields
                java.lang.reflect.Method sequenceMethod = entity.getClass().getMethod("getSequenceNumber");
                if (sequenceMethod != null) {
                    Object value = sequenceMethod.invoke(entity);
                    if (value instanceof Number) {
                        return getTableNameForValue(((Number) value).longValue());
                    }
                }
            } catch (Exception e) {
                // No sequence number method, continue to fallback
            }
            // Fallback to extracting number from ID
            try {
                String id = entity.getId();
                // Extract numeric part from ID if it follows a pattern like "EVT_00001234"
                String numericPart = id.replaceAll("[^0-9]", "");
                if (!numericPart.isEmpty()) {
                    return getTableNameForValue(Long.parseLong(numericPart));
                }
            } catch (Exception e) {
                // Fallback to hash if can't extract number
                return getTableNameForHash(entity.getId().hashCode());
            }
        }
        return getTableNameForDate(metadata.getShardingKeyValue(entity));
    }

    private String getTableNameForDate(LocalDateTime date) {
        DateTimeFormatter formatter = getDateFormatter();
        return database + "." + tablePrefix + "_" + date.format(formatter);
    }

    private String getTableNameForHash(int bucket) {
        int buckets = getHashBuckets(partitionRange);
        int actualBucket = Math.abs(bucket) % buckets;
        return database + "." + tablePrefix + "_hash_" + actualBucket;
    }

    private String getTableNameForValue(long value) {
        long rangeSize = getValueRangeSize(partitionRange);
        long rangeStart = (value / rangeSize) * rangeSize;
        long rangeEnd = rangeStart + rangeSize - 1;
        return database + "." + tablePrefix + "_" + rangeStart + "_" + rangeEnd;
    }

    private DateTimeFormatter getDateFormatter() {
        if (partitionRange == PartitionRange.HOURLY) {
            return DateTimeFormatter.ofPattern("yyyyMMddHH");
        } else if (partitionRange == PartitionRange.MONTHLY) {
            return DateTimeFormatter.ofPattern("yyyyMM");
        } else if (partitionRange == PartitionRange.YEARLY) {
            return DateTimeFormatter.ofPattern("yyyy");
        }
        return DATE_FORMAT; // Default to daily
    }

    /**
     * Extract date/time from table name based on partition range
     */
    private LocalDateTime extractTableDateTime(String tableName) {
        try {
            String dateStr = tableName.substring(tableName.lastIndexOf('_') + 1);

            if (partitionRange == PartitionRange.HOURLY) {
                // Parse as yyyyMMddHH and set minutes/seconds to 0
                return LocalDateTime.parse(dateStr + "0000",
                    DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            } else if (partitionRange == PartitionRange.MONTHLY) {
                // Parse as yyyyMM and set to first day of month at midnight
                return LocalDateTime.parse(dateStr + "01000000",
                    DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            } else if (partitionRange == PartitionRange.YEARLY) {
                // Parse as yyyy and set to Jan 1st at midnight
                return LocalDateTime.parse(dateStr + "0101000000",
                    DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            } else {
                // Default: parse as yyyyMMdd (daily)
                return LocalDateTime.parse(dateStr + "000000",
                    DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            }
        } catch (Exception e) {
            // If parsing fails, return null to skip this table
            return null;
        }
    }

    /**
     * Determine if a table should be skipped for findAllAfterDate
     */
    private boolean shouldSkipTableAfter(LocalDateTime tableDateTime, LocalDateTime afterDate) {
        if (partitionRange == PartitionRange.HOURLY) {
            // Skip if table hour is before the afterDate's hour
            return tableDateTime.plusHours(1).isBefore(afterDate) ||
                   tableDateTime.plusHours(1).isEqual(afterDate);
        } else if (partitionRange == PartitionRange.MONTHLY) {
            // Skip if entire month is before afterDate
            return tableDateTime.plusMonths(1).isBefore(afterDate) ||
                   tableDateTime.plusMonths(1).isEqual(afterDate);
        } else if (partitionRange == PartitionRange.YEARLY) {
            // Skip if entire year is before afterDate
            return tableDateTime.plusYears(1).isBefore(afterDate) ||
                   tableDateTime.plusYears(1).isEqual(afterDate);
        } else {
            // Daily: Skip if entire day is before afterDate
            return tableDateTime.toLocalDate().isBefore(afterDate.toLocalDate());
        }
    }

    /**
     * Determine if a table should be skipped for findAllBeforeDate
     */
    private boolean shouldSkipTableBefore(LocalDateTime tableDateTime, LocalDateTime beforeDate) {
        if (partitionRange == PartitionRange.HOURLY) {
            // Skip if table hour is after or at the beforeDate's hour
            return tableDateTime.isAfter(beforeDate) ||
                   tableDateTime.isEqual(beforeDate);
        } else if (partitionRange == PartitionRange.MONTHLY) {
            // Skip if table month starts after beforeDate
            return tableDateTime.isAfter(beforeDate);
        } else if (partitionRange == PartitionRange.YEARLY) {
            // Skip if table year starts after beforeDate
            return tableDateTime.isAfter(beforeDate);
        } else {
            // Daily: Skip if table day is after beforeDate's day
            return tableDateTime.toLocalDate().isAfter(beforeDate.toLocalDate());
        }
    }

    private boolean isTimeBased(PartitionRange range) {
        return range == PartitionRange.HOURLY ||
               range == PartitionRange.DAILY ||
               range == PartitionRange.MONTHLY ||
               range == PartitionRange.YEARLY;
    }

    private boolean isHashBased(PartitionRange range) {
        return range != null && range.name().startsWith("HASH_");
    }

    private boolean isValueBased(PartitionRange range) {
        return range != null && range.name().startsWith("VALUE_RANGE_");
    }

    private int getHashBuckets(PartitionRange range) {
        if (range == null || !isHashBased(range)) return 16;
        switch (range) {
            case HASH_4: return 4;
            case HASH_8: return 8;
            case HASH_16: return 16;
            case HASH_32: return 32;
            case HASH_64: return 64;
            case HASH_128: return 128;
            default: return 16;
        }
    }

    private long getValueRangeSize(PartitionRange range) {
        if (range == null || !isValueBased(range)) return 10000;
        switch (range) {
            case VALUE_RANGE_1K: return 1000L;
            case VALUE_RANGE_10K: return 10000L;
            case VALUE_RANGE_100K: return 100000L;
            case VALUE_RANGE_1M: return 1000000L;
            case VALUE_RANGE_10M: return 10000000L;
            case VALUE_RANGE_100M: return 100000000L;
            default: return 10000L;
        }
    }
    
    private void createTableIfNotExists(String tableName) throws SQLException {
        // Use PersistenceProvider to generate CREATE TABLE SQL
        String createSQL = persistenceProvider.generateCreateTableSQL(tableName, metadata, charset, collation);

        // Add nested partitions if enabled
        if (nestedPartitionsEnabled) {
            createSQL = addNestedPartitions(createSQL, tableName);
        }

        try (Connection conn = connectionProvider.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(createSQL);
            // Only log if table was actually created (CREATE TABLE IF NOT EXISTS handles this)
            logger.debug("Ensured table exists: " + tableName +
                (nestedPartitionsEnabled ? " with " + nestedPartitionCount + " nested partitions" : ""));
        } catch (SQLException e) {
            // If it's a "table already exists" error and we're using partitions, ignore it
            if (!e.getMessage().contains("already exists")) {
                throw e;
            }
        }
    }
    
    /**
     * Extract date from table name in format tablename_YYYYMMDD
     */
    private String extractDateFromTableName(String tableName) {
        // Extract YYYYMMDD from table name like "events_20250920"
        String dateStr = tableName.substring(tableName.lastIndexOf('_') + 1);

        // Convert YYYYMMDD to YYYY-MM-DD
        if (dateStr.length() == 8) {
            return dateStr.substring(0, 4) + "-" +
                   dateStr.substring(4, 6) + "-" +
                   dateStr.substring(6, 8);
        }

        // Fallback to current date if unable to parse
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }

    /**
     * Add nested native partitions to a table for better query performance
     */
    private String addNestedPartitions(String baseCreateSQL, String tableName) throws SQLException {
        // First, ensure we don't already have partition syntax in the SQL
        if (baseCreateSQL.contains("PARTITION BY")) {
            return baseCreateSQL;
        }

        // Determine partition column - use the sharding key column
        String partCol = metadata.getShardingKeyField().getColumnName();
        String idCol = metadata.getIdField().getColumnName();

        // For RANGE partitioning in MySQL, the partition column must be part of the primary key
        // We need to modify the primary key to include both id and the partition column
        String modifiedSQL = baseCreateSQL;
        if (baseCreateSQL.contains("PRIMARY KEY")) {
            // Replace single-column primary key with composite key
            modifiedSQL = baseCreateSQL.replaceAll(
                idCol + "\\s+VARCHAR\\([^)]+\\)\\s+PRIMARY KEY",
                idCol + " VARCHAR(255)"
            );
        }

        // Remove the closing parenthesis and ENGINE clause temporarily
        String sqlWithoutEngine = modifiedSQL.replaceAll("\\)\\s*(ENGINE=.*)?$", "");
        if (sqlWithoutEngine.equals(modifiedSQL)) {
            // If nothing was removed, try other patterns
            sqlWithoutEngine = modifiedSQL.replaceAll(";$", "");
        }

        StringBuilder partitionSQL = new StringBuilder(sqlWithoutEngine);

        // Determine partition column type if not explicitly set
        PartitionColumnType effectiveType = partitionColumnType;
        if (effectiveType == null) {
            // Try to infer from the field type
            Class<?> fieldType = metadata.getShardingKeyField().getType();
            if (fieldType == LocalDateTime.class || fieldType == java.util.Date.class || fieldType == Timestamp.class) {
                effectiveType = PartitionColumnType.LOCAL_DATE_TIME;
            } else if (fieldType == Long.class) {
                effectiveType = PartitionColumnType.LONG;
            } else if (fieldType == Integer.class) {
                effectiveType = PartitionColumnType.INTEGER;
            } else if (fieldType == String.class) {
                effectiveType = PartitionColumnType.STRING;
            }
        }

        // Add composite primary key for partition compatibility
        // Find the last comma before the closing parenthesis and add the primary key
        partitionSQL.append(",\n  PRIMARY KEY (").append(idCol).append(", ").append(partCol).append(")");

        // Add ENGINE clause
        partitionSQL.append("\n) ENGINE=InnoDB DEFAULT CHARSET=").append(charset)
                   .append(" COLLATE=").append(collation);

        // Add partition clause based on the partition column type
        if (effectiveType == PartitionColumnType.LOCAL_DATE_TIME) {
            // Extract the date from the table name (format: tablename_YYYYMMDD)
            String tableDate = extractDateFromTableName(tableName);

            // For date-based columns, create hourly partitions with actual datetime values
            // MySQL RANGE COLUMNS partitioning allows direct datetime values
            partitionSQL.append("\nPARTITION BY RANGE COLUMNS(").append(partCol).append(") (");

            // Create 24 hourly partitions for the day
            // Each partition covers one hour of the day
            int maxHours = Math.min(nestedPartitionCount, 24);
            for (int hour = 1; hour <= maxHours; hour++) {
                if (hour > 1) partitionSQL.append(",");

                // Partition p_h01 for 00:00:00 to 00:59:59 (VALUES LESS THAN 01:00:00)
                // Partition p_h02 for 01:00:00 to 01:59:59 (VALUES LESS THAN 02:00:00)
                // ... and so on
                String partitionBoundary;

                if (hour < 24) {
                    // Regular hourly partitions
                    partitionBoundary = String.format("%s %02d:00:00", tableDate, hour);
                } else {
                    // Last partition (p_h24) for 23:00:00 to 23:59:59 needs next day's 00:00:00
                    LocalDateTime currentDate = LocalDateTime.parse(tableDate + " 00:00:00",
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    LocalDateTime nextDay = currentDate.plusDays(1);
                    partitionBoundary = nextDay.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                }

                partitionSQL.append("\n  PARTITION p_h").append(String.format("%02d", hour))
                           .append(" VALUES LESS THAN ('").append(partitionBoundary).append("')");
            }
            partitionSQL.append("\n)");
        } else {
            // For other types, create hash partitions
            partitionSQL.append("\nPARTITION BY HASH(").append(partCol).append(")")
                       .append("\nPARTITIONS ").append(nestedPartitionCount);
        }

        return partitionSQL.toString();
    }

    /**
     * Add hourly partitioning to a daily table
     * Each daily table gets 24 hour partitions (h00, h01, h02, ..., h23)
     *
     * Note: For simplicity, multi-table mode does NOT add hourly partitions within each daily table.
     * Each day gets its own table, which provides sufficient granularity for most use cases.
     * If hourly partitioning is needed, use HOURLY table granularity instead.
     */
    private String addHourlyPartitioning(String baseCreateSQL, String tableName) throws SQLException {
        // For now, disable hourly partitioning within daily tables
        // This was causing issues with String IDs and adds unnecessary complexity
        // Users can use HOURLY table granularity if they need hour-level separation

        // Just add charset and collation if not present
        if (!baseCreateSQL.contains("CHARSET=")) {
            baseCreateSQL = baseCreateSQL.replace(") ENGINE=InnoDB",
                ") ENGINE=InnoDB DEFAULT CHARSET=" + charset + " COLLATE=" + collation);
        }

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
    
    private void setIdParameter(PreparedStatement stmt, int index, String id) throws SQLException {
        stmt.setString(index, id);
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
            // Increment based on partition range
            if (partitionRange == PartitionRange.HOURLY) {
                current = current.plusHours(1);
            } else if (partitionRange == PartitionRange.MONTHLY) {
                current = current.plusMonths(1);
            } else if (partitionRange == PartitionRange.YEARLY) {
                current = current.plusYears(1);
            } else {
                // Default to daily
                current = current.plusDays(1);
            }
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
        if (partitionRange == null || isTimeBased(partitionRange)) {
            // Time-based partitioning - create tables for date range
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime startDate, endDate;

            // Calculate date range based on partition type
            if (partitionRange == PartitionRange.HOURLY) {
                // For hourly, create tables for +/- 48 hours
                startDate = now.minusHours(48);
                endDate = now.plusHours(48);
                logger.info(String.format("Initializing hourly tables for: %s to %s",
                            startDate, endDate));
            } else if (partitionRange == PartitionRange.MONTHLY) {
                // For monthly, create tables for +/- 3 months
                startDate = now.minusMonths(3);
                endDate = now.plusMonths(3);
                logger.info(String.format("Initializing monthly tables for: %s to %s",
                            startDate.toLocalDate(), endDate.toLocalDate()));
            } else if (partitionRange == PartitionRange.YEARLY) {
                // For yearly, create tables for +/- 2 years
                startDate = now.minusYears(2);
                endDate = now.plusYears(2);
                logger.info(String.format("Initializing yearly tables for: %d to %d",
                            startDate.getYear(), endDate.getYear()));
            } else {
                // Default to daily
                startDate = now.minusDays(partitionRetentionPeriod);
                endDate = now.plusDays(partitionRetentionPeriod);
                logger.info(String.format("Initializing daily tables for retention period: %s to %s (%d days)",
                            startDate.toLocalDate(), endDate.toLocalDate(),
                            partitionRetentionPeriod * 2 + 1));
            }

            // Store the min/max partition boundaries
            this.minPartitionDate = startDate;
            this.maxPartitionDate = endDate;

            createTablesForDateRange(startDate, endDate);
        } else if (isHashBased(partitionRange)) {
            // Hash-based partitioning - create fixed number of buckets
            int buckets = getHashBuckets(partitionRange);
            logger.info("Initializing " + buckets + " hash-based tables");

            for (int i = 0; i < buckets; i++) {
                String tableName = getTableNameForHash(i);
                createTableIfNotExists(tableName);
            }
        } else if (isValueBased(partitionRange)) {
            // Value-based partitioning - create initial set of range tables
            long rangeSize = getValueRangeSize(partitionRange);
            int numTables = Math.min(10, partitionRetentionPeriod); // Create up to 10 initial tables
            logger.info("Initializing " + numTables + " value-range tables (size: " + rangeSize + ")");

            for (int i = 0; i < numTables; i++) {
                long rangeStart = i * rangeSize;
                String tableName = getTableNameForValue(rangeStart);
                createTableIfNotExists(tableName);
            }
        }

        logger.info("Completed table initialization");
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
    public void deleteById(String id) throws SQLException {
        // Need to delete from all tables as we don't know which one contains the record
        String idColumn = metadata.getIdField().getColumnName();
        List<String> tables = getExistingTables();

        for (String table : tables) {
            String sql = "DELETE FROM " + table + " WHERE " + idColumn + " = ?";
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, id);
                stmt.executeUpdate();
            }
        }
    }

    @Override
    public void deleteByIdAndPartitionColRange(String id, P startValue, P endValue) throws SQLException {
        String idColumn = metadata.getIdField().getColumnName();
        String shardingColumn = metadata.getShardingKeyField().getColumnName();

        // Get tables that could contain data in this date range
        List<String> tables = new ArrayList<>();
        if (startValue instanceof LocalDateTime && endValue instanceof LocalDateTime) {
            tables = getTablesForDateRange((LocalDateTime) startValue, (LocalDateTime) endValue);
        } else {
            tables = getExistingTables();
        }

        for (String table : tables) {
            String sql = "DELETE FROM " + table + " WHERE " + idColumn + " = ? AND " +
                         shardingColumn + " >= ? AND " + shardingColumn + " < ?";
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, id);

                if (startValue instanceof LocalDateTime) {
                    stmt.setTimestamp(2, Timestamp.valueOf((LocalDateTime) startValue));
                    stmt.setTimestamp(3, Timestamp.valueOf((LocalDateTime) endValue));
                } else {
                    stmt.setObject(2, startValue);
                    stmt.setObject(3, endValue);
                }
                stmt.executeUpdate();
            }
        }
    }

    @Override
    public void deleteAllByPartitionRange(P startValue, P endValue) throws SQLException {
        // TODO: Handle generic partition value types for table range
        List<String> tables = new ArrayList<>();
        if (startValue instanceof LocalDateTime && endValue instanceof LocalDateTime) {
            tables = getTablesForDateRange((LocalDateTime) startValue, (LocalDateTime) endValue);
        }

        for (String table : tables) {
            String shardingColumn = metadata.getShardingKeyField().getColumnName();
            String sql = "DELETE FROM " + table + " WHERE " + shardingColumn + " >= ? AND " + shardingColumn + " < ?";
            try (Connection conn = connectionProvider.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                // TODO: Handle generic partition value types
                if (startValue instanceof LocalDateTime) {
                    stmt.setTimestamp(1, Timestamp.valueOf((LocalDateTime) startValue));
                    stmt.setTimestamp(2, Timestamp.valueOf((LocalDateTime) endValue));
                } else {
                    stmt.setObject(1, startValue);
                    stmt.setObject(2, endValue);
                }
                stmt.executeUpdate();
            }
        }
    }

    @Override
    public void shutdown() {
        // Stop monitoring
        if (monitoringService != null) {
            monitoringService.stop();
        }

        // Shutdown scheduler immediately
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdownNow(); // Use shutdownNow() for immediate termination
            try {
                // Wait only 2 seconds for termination
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    // Force shutdown if still not terminated
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
    static class Builder<T extends ShardingEntity<P>, P extends Comparable<? super P>> {
        private final Class<T> entityClass;
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
        private TableGranularity tableGranularity = TableGranularity.DAILY;
        private String charset = "utf8mb4";
        private String collation = "utf8mb4_bin";
        private PartitionRange partitionRange = PartitionRange.DAILY;
        private String partitionColumn;
        private PartitionColumnType partitionColumnType;
        private PersistenceProvider persistenceProvider;

        // Nested partition support
        private boolean nestedPartitionsEnabled = false;
        private int nestedPartitionCount = 20; // Default for non-date types

        Builder(Class<T> entityClass) {
            this.entityClass = entityClass;
        }
        
        public Builder<T, P> host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder<T, P> port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder<T, P> database(String database) {
            this.database = database;
            return this;
        }
        
        public Builder<T, P> username(String username) {
            this.username = username;
            return this;
        }
        
        public Builder<T, P> password(String password) {
            this.password = password;
            return this;
        }
        
        public Builder<T, P> tablePrefix(String tablePrefix) {
            this.tablePrefix = tablePrefix;
            return this;
        }

        public Builder<T, P> baseTableName(String baseTableName) {
            this.tablePrefix = baseTableName;
            return this;
        }

        public Builder<T, P> tableRetentionDays(int days) {
            this.partitionRetentionPeriod = days;
            return this;
        }

        public Builder<T, P> tableGranularity(TableGranularity granularity) {
            this.tableGranularity = granularity;
            return this;
        }

        public Builder<T, P> autoCreateTables(boolean autoCreate) {
            this.initializePartitionsOnStart = autoCreate;
            return this;
        }

        public Builder<T, P> charset(String charset) {
            if (charset != null && !charset.trim().isEmpty()) {
                this.charset = charset;
            }
            return this;
        }

        public Builder<T, P> collation(String collation) {
            if (collation != null && !collation.trim().isEmpty()) {
                this.collation = collation;
            }
            return this;
        }
        
        public Builder<T, P> partitionRetentionPeriod(int days) {
            this.partitionRetentionPeriod = days;
            return this;
        }
        
        public Builder<T, P> autoManagePartitions(boolean enable) {
            this.autoManagePartitions = enable;
            return this;
        }
        
        public Builder<T, P> partitionAdjustmentTime(int hour, int minute) {
            this.partitionAdjustmentTime = LocalTime.of(hour, minute);
            return this;
        }
        
        public Builder<T, P> partitionAdjustmentTime(LocalTime time) {
            this.partitionAdjustmentTime = time;
            return this;
        }
        
        public Builder<T, P> initializePartitionsOnStart(boolean initialize) {
            this.initializePartitionsOnStart = initialize;
            return this;
        }
        
        public Builder<T, P> monitoring(MonitoringConfig monitoringConfig) {
            this.monitoringConfig = monitoringConfig;
            return this;
        }
        
        public Builder<T, P> logger(Logger logger) {
            this.logger = logger;
            return this;
        }

        public Builder<T, P> partitionRange(PartitionRange range) {
            this.partitionRange = range;
            return this;
        }

        public Builder<T, P> partitionColumn(String column) {
            this.partitionColumn = column;
            return this;
        }

        public Builder<T, P> partitionColumnType(PartitionColumnType type) {
            this.partitionColumnType = type;
            return this;
        }

        public Builder<T, P> persistenceProvider(PersistenceProvider provider) {
            this.persistenceProvider = provider;
            return this;
        }

        /**
         * Enable nested partitions within each table.
         * When enabled, each table will have native database partitions for better query performance.
         */
        public Builder<T, P> withNestedPartitions(boolean enabled) {
            this.nestedPartitionsEnabled = enabled;
            return this;
        }

        /**
         * Set the number of nested partitions per table.
         * For date-based tables, use 24 for hourly partitions.
         * For other types, default is 20.
         */
        public Builder<T, P> withNestedPartitionCount(int count) {
            if (count < 1 || count > 1024) {
                throw new IllegalArgumentException("Nested partition count must be between 1 and 1024");
            }
            this.nestedPartitionCount = count;
            return this;
        }

        public GenericMultiTableRepository<T, P> build() {
            if (database == null || username == null || password == null) {
                throw new IllegalStateException("Database, username, and password are required");
            }
            return new GenericMultiTableRepository<T, P>(this);
        }
    }
    
    /**
     * Create a new builder
     */
    // Package-private factory method - only SplitVerseRepository can use this
    static <T extends ShardingEntity<P>, P extends Comparable<? super P>> Builder<T, P> builder(Class<T> entityClass) {
        return new Builder<T, P>(entityClass);
    }
}