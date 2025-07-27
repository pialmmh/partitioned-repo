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
    private TableManager tableManager;
    
    public ShardingRepository(DataSource shardingSphereDataSource, Class<T> entityClass) {
        this.shardingSphereDataSource = shardingSphereDataSource;
        this.metadata = EntityMetadata.of(entityClass);
        // TableManager will be set by factory with proper parameters
        this.tableManager = null;
        
        // Note: Table initialization is handled by the factory to ensure proper DataSource is used
    }
    
    /**
     * Insert entity - ShardingSphere automatically routes to correct table/partition
     */
    public void insert(T entity) throws SQLException {
        String sql = metadata.generateInsertSql();
        
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
     * Find entities by date range - ShardingSphere handles cross-table/partition queries
     */
    public List<T> findByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String whereClause = metadata.getShardKey() + " BETWEEN ? AND ?";
        String sql = metadata.generateSelectSql(whereClause);
        
        try (Connection conn = shardingSphereDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(startDate));
            stmt.setTimestamp(2, Timestamp.valueOf(endDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                List<T> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapResultSetToEntity(rs));
                }
                return results;
            }
        }
    }
    
    /**
     * Count entities in date range - ShardingSphere handles cross-table/partition aggregation
     */
    public long count(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + metadata.getTableName() + 
                    " WHERE " + metadata.getShardKey() + " BETWEEN ? AND ?";
        
        try (Connection conn = shardingSphereDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setTimestamp(1, Timestamp.valueOf(startDate));
            stmt.setTimestamp(2, Timestamp.valueOf(endDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        }
    }
    
    /**
     * Find entities by any field - ShardingSphere handles cross-table/partition queries
     */
    public List<T> findByField(String fieldName, Object value) throws SQLException {
        ColumnMetadata column = metadata.getColumnByFieldName(fieldName);
        if (column == null) {
            throw new IllegalArgumentException("Field '" + fieldName + "' not found in entity");
        }
        
        String whereClause = column.getColumnName() + " = ?";
        String sql = metadata.generateSelectSql(whereClause);
        
        try (Connection conn = shardingSphereDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            setParameter(stmt, 1, value);
            
            try (ResultSet rs = stmt.executeQuery()) {
                List<T> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapResultSetToEntity(rs));
                }
                return results;
            }
        }
    }
    
    /**
     * Execute custom query - ShardingSphere handles cross-table/partition queries
     */
    public List<T> query(String whereClause, Object... parameters) throws SQLException {
        String sql = metadata.generateSelectSql(whereClause);
        
        try (Connection conn = shardingSphereDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < parameters.length; i++) {
                setParameter(stmt, i + 1, parameters[i]);
            }
            
            try (ResultSet rs = stmt.executeQuery()) {
                List<T> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapResultSetToEntity(rs));
                }
                return results;
            }
        }
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
        ColumnMetadata primaryKeyColumn = metadata.getPrimaryKeyColumn();
        if (primaryKeyColumn == null) {
            throw new IllegalStateException("Entity " + metadata.getEntityClass().getSimpleName() + " has no primary key defined");
        }
        
        // Let ShardingSphere handle the routing - it will scan all configured tables
        String whereClause = primaryKeyColumn.getColumnName() + " = ?";
        List<T> results = query(whereClause, id);
        
        return results.isEmpty() ? null : results.get(0);
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
        
        // Use ShardingSphere with date range to limit table scanning
        String whereClause = primaryKeyColumn.getColumnName() + " = ? AND " + 
                           metadata.getShardKey() + " BETWEEN ? AND ?";
        
        try (Connection conn = shardingSphereDataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(
                 metadata.generateSelectSql(whereClause))) {
            
            stmt.setObject(1, id);
            stmt.setTimestamp(2, Timestamp.valueOf(startDate));
            stmt.setTimestamp(3, Timestamp.valueOf(endDate));
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapResultSetToEntity(rs);
                }
                return null;
            }
        }
    }
    
    /**
     * Get table manager for manual table operations
     */
    public TableManager getTableManager() {
        return tableManager;
    }
    
    /**
     * Set a custom table manager (used by factory to inject actual DataSource)
     */
    public void setTableManager(TableManager tableManager) {
        this.tableManager = tableManager;
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