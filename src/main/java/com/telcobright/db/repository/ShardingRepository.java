package com.telcobright.db.repository;

import com.telcobright.db.metadata.ColumnMetadata;
import com.telcobright.db.metadata.EntityMetadata;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

/**
 * Generic repository for sharded entities using Apache ShardingSphere
 * Automatically infers SQL operations from entity annotations
 */
public class ShardingRepository<T> {
    
    private final DataSource shardingSphereDataSource;
    private final EntityMetadata<T> metadata;
    private final TableManager tableManager;
    
    public ShardingRepository(DataSource shardingSphereDataSource, Class<T> entityClass) {
        this.shardingSphereDataSource = shardingSphereDataSource;
        this.metadata = EntityMetadata.of(entityClass);
        this.tableManager = new TableManager(shardingSphereDataSource, metadata);
        
        // Initialize tables/partitions if auto-manage is enabled
        if (metadata.isAutoManagePartition()) {
            tableManager.initializeTablesForRetentionWindow();
        }
    }
    
    /**
     * Insert entity - Routes to correct date-based table
     */
    public void insert(T entity) throws SQLException {
        // Get the shard key value (date) to determine target table
        ColumnMetadata shardKeyColumn = metadata.getShardKeyColumn();
        if (shardKeyColumn == null) {
            throw new IllegalStateException("No shard key column found for entity");
        }
        
        Object shardKeyValue = shardKeyColumn.getValue(entity);
        if (!(shardKeyValue instanceof LocalDateTime)) {
            throw new IllegalStateException("Shard key must be LocalDateTime for date-based sharding");
        }
        
        LocalDateTime dateTime = (LocalDateTime) shardKeyValue;
        String targetTable = metadata.getTableNameForDate(dateTime);
        
        // Generate INSERT SQL for the specific table
        String sql = generateInsertSqlForTable(targetTable);
        
        try (Connection conn = shardingSphereDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            // Set parameters for non-primary key columns
            int paramIndex = 1;
            for (ColumnMetadata column : metadata.getColumns()) {
                if (!column.isPrimaryKey()) {
                    Object value = column.getValue(entity);
                    setParameter(stmt, paramIndex++, value);
                }
            }
            
            stmt.executeUpdate();
        }
    }
    
    /**
     * Find entities by date range - Queries across multiple date-based tables
     */
    public List<T> findByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        List<T> allResults = new ArrayList<>();
        List<String> tableNames = generateTableNamesForDateRange(startDate, endDate);
        
        for (String tableName : tableNames) {
            // Query each table for entities in the date range
            String whereClause = metadata.getShardKey() + " BETWEEN ? AND ?";
            String sql = generateSelectSqlForTable(tableName, whereClause);
            
            try (Connection conn = shardingSphereDataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                stmt.setTimestamp(1, Timestamp.valueOf(startDate));
                stmt.setTimestamp(2, Timestamp.valueOf(endDate));
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        allResults.add(mapResultSetToEntity(rs));
                    }
                }
            } catch (SQLException e) {
                // Table might not exist - ignore and continue
                if (!e.getMessage().contains("doesn't exist")) {
                    throw e;
                }
            }
        }
        
        return allResults;
    }
    
    /**
     * Count entities in date range - Counts across multiple date-based tables
     */
    public long count(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        long totalCount = 0;
        List<String> tableNames = generateTableNamesForDateRange(startDate, endDate);
        
        for (String tableName : tableNames) {
            String sql = "SELECT COUNT(*) FROM " + tableName + 
                        " WHERE " + metadata.getShardKey() + " BETWEEN ? AND ?";
            
            try (Connection conn = shardingSphereDataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                stmt.setTimestamp(1, Timestamp.valueOf(startDate));
                stmt.setTimestamp(2, Timestamp.valueOf(endDate));
                
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        totalCount += rs.getLong(1);
                    }
                }
            } catch (SQLException e) {
                // Table might not exist - ignore and continue
                if (!e.getMessage().contains("doesn't exist")) {
                    throw e;
                }
            }
        }
        
        return totalCount;
    }
    
    /**
     * Find entities by any field - Scans across all retention tables
     */
    public List<T> findByField(String fieldName, Object value) throws SQLException {
        ColumnMetadata column = metadata.getColumnByFieldName(fieldName);
        if (column == null) {
            throw new IllegalArgumentException("Field '" + fieldName + "' not found in entity");
        }
        
        // Scan across all tables in retention window
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusDays(metadata.getRetentionSpanDays());
        LocalDateTime endDate = now.plusDays(metadata.getRetentionSpanDays());
        
        List<T> allResults = new ArrayList<>();
        List<String> tableNames = generateTableNamesForDateRange(startDate, endDate);
        
        for (String tableName : tableNames) {
            String whereClause = column.getColumnName() + " = ?";
            String sql = generateSelectSqlForTable(tableName, whereClause);
            
            try (Connection conn = shardingSphereDataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                setParameter(stmt, 1, value);
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        allResults.add(mapResultSetToEntity(rs));
                    }
                }
            } catch (SQLException e) {
                // Table might not exist - ignore and continue
                if (!e.getMessage().contains("doesn't exist")) {
                    throw e;
                }
            }
        }
        
        return allResults;
    }
    
    /**
     * Execute custom query - Scans across all retention tables
     */
    public List<T> query(String whereClause, Object... parameters) throws SQLException {
        // Scan across all tables in retention window
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusDays(metadata.getRetentionSpanDays());
        LocalDateTime endDate = now.plusDays(metadata.getRetentionSpanDays());
        
        List<T> allResults = new ArrayList<>();
        List<String> tableNames = generateTableNamesForDateRange(startDate, endDate);
        
        for (String tableName : tableNames) {
            String sql = generateSelectSqlForTable(tableName, whereClause);
            
            try (Connection conn = shardingSphereDataSource.getConnection();
                 PreparedStatement stmt = conn.prepareStatement(sql)) {
                
                for (int i = 0; i < parameters.length; i++) {
                    setParameter(stmt, i + 1, parameters[i]);
                }
                
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        allResults.add(mapResultSetToEntity(rs));
                    }
                }
            } catch (SQLException e) {
                // Table might not exist - ignore and continue
                if (!e.getMessage().contains("doesn't exist")) {
                    throw e;
                }
            }
        }
        
        return allResults;
    }
    
    /**
     * Find entity by ID - scans across all tables in retention window
     * 
     * WARNING: Performance Impact
     * - May scan up to (retentionSpanDays * 2) tables 
     * - Cannot leverage partition pruning since ID doesn't contain date info
     * - Use findByIdAndDateRange() when possible to limit scope
     * 
     * Example: 7-day retention = up to 14 tables scanned
     */
    public T findById(Object id) throws SQLException {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime startDate = now.minusDays(metadata.getRetentionSpanDays());
        LocalDateTime endDate = now.plusDays(metadata.getRetentionSpanDays());
        
        return findByIdAndDateRange(id, startDate, endDate);
    }
    
    /**
     * Find entity by ID within a specific date range - limits table scan scope
     * 
     * RECOMMENDED: Use this instead of findById() when you know approximate date
     * - Only scans tables within the specified date range
     * - Much more efficient for large retention windows
     * - Better performance for ID lookups with date context
     */
    public T findByIdAndDateRange(Object id, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        ColumnMetadata primaryKeyColumn = metadata.getPrimaryKeyColumn();
        if (primaryKeyColumn == null) {
            throw new IllegalStateException("Entity " + metadata.getEntityClass().getSimpleName() + " has no primary key defined");
        }
        
        // Generate table names for the date range
        List<String> tableNames = generateTableNamesForDateRange(startDate, endDate);
        
        // Search across all tables in the date range
        for (String tableName : tableNames) {
            T result = findByIdInTable(id, tableName, primaryKeyColumn);
            if (result != null) {
                return result;
            }
        }
        
        return null; // Not found in any table
    }
    
    /**
     * Get table manager for manual table operations
     */
    public TableManager getTableManager() {
        return tableManager;
    }
    
    /**
     * Generate list of table names for a date range
     */
    private List<String> generateTableNamesForDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        List<String> tableNames = new ArrayList<>();
        LocalDateTime current = startDate.toLocalDate().atStartOfDay();
        
        while (!current.isAfter(endDate)) {
            String tableName = metadata.getTableNameForDate(current);
            tableNames.add(tableName);
            current = current.plusDays(1);
        }
        
        return tableNames;
    }
    
    /**
     * Find entity by ID in a specific table
     */
    private T findByIdInTable(Object id, String tableName, ColumnMetadata primaryKeyColumn) throws SQLException {
        String sql = generateSelectSqlForTable(tableName, primaryKeyColumn.getColumnName() + " = ?");
        
        try (Connection conn = shardingSphereDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            setParameter(stmt, 1, id);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToEntity(rs);
                }
                return null;
            }
        } catch (SQLException e) {
            // Table might not exist - ignore and continue
            if (e.getMessage().contains("doesn't exist")) {
                return null;
            }
            throw e;
        }
    }
    
    /**
     * Generate INSERT SQL for a specific table
     */
    private String generateInsertSqlForTable(String tableName) {
        List<String> columnNames = new ArrayList<>();
        List<String> placeholders = new ArrayList<>();
        
        for (ColumnMetadata column : metadata.getColumns()) {
            if (!column.isPrimaryKey()) { // Skip auto-increment primary keys
                columnNames.add(column.getColumnName());
                placeholders.add("?");
            }
        }
        
        return "INSERT INTO " + tableName + " (" + 
               String.join(", ", columnNames) + ") VALUES (" + 
               String.join(", ", placeholders) + ")";
    }
    
    /**
     * Generate SELECT SQL for a specific table
     */
    private String generateSelectSqlForTable(String tableName, String whereClause) {
        String columnNames = metadata.getColumns().stream()
            .map(ColumnMetadata::getColumnName)
            .reduce((a, b) -> a + ", " + b)
            .orElse("*");
        
        String sql = "SELECT " + columnNames + " FROM " + tableName;
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sql += " WHERE " + whereClause;
        }
        return sql;
    }
    
    private void setParameter(PreparedStatement stmt, int index, Object value) throws SQLException {
        if (value == null) {
            stmt.setNull(index, Types.NULL);
        } else if (value instanceof String) {
            stmt.setString(index, (String) value);
        } else if (value instanceof Long) {
            stmt.setLong(index, (Long) value);
        } else if (value instanceof Integer) {
            stmt.setInt(index, (Integer) value);
        } else if (value instanceof LocalDateTime) {
            stmt.setTimestamp(index, Timestamp.valueOf((LocalDateTime) value));
        } else if (value instanceof Boolean) {
            stmt.setBoolean(index, (Boolean) value);
        } else {
            stmt.setObject(index, value);
        }
    }
    
    private T mapResultSetToEntity(ResultSet rs) throws SQLException {
        try {
            T entity = metadata.getEntityClass().getDeclaredConstructor().newInstance();
            
            for (ColumnMetadata column : metadata.getColumns()) {
                Object value = getValueFromResultSet(rs, column);
                column.setValue(entity, value);
            }
            
            return entity;
        } catch (Exception e) {
            throw new SQLException("Failed to map result set to entity", e);
        }
    }
    
    private Object getValueFromResultSet(ResultSet rs, ColumnMetadata column) throws SQLException {
        String columnName = column.getColumnName();
        Class<?> fieldType = column.getField().getType();
        
        if (fieldType == Long.class || fieldType == long.class) {
            long value = rs.getLong(columnName);
            return rs.wasNull() ? null : value;
        } else if (fieldType == Integer.class || fieldType == int.class) {
            int value = rs.getInt(columnName);
            return rs.wasNull() ? null : value;
        } else if (fieldType == String.class) {
            return rs.getString(columnName);
        } else if (fieldType == LocalDateTime.class) {
            Timestamp timestamp = rs.getTimestamp(columnName);
            return timestamp != null ? timestamp.toLocalDateTime() : null;
        } else if (fieldType == Boolean.class || fieldType == boolean.class) {
            boolean value = rs.getBoolean(columnName);
            return rs.wasNull() ? null : value;
        } else {
            return rs.getObject(columnName);
        }
    }
}