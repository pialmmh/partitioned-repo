package com.telcobright.db.metadata;

import com.telcobright.db.annotation.Column;
import com.telcobright.db.annotation.ShardingMode;
import com.telcobright.db.annotation.ShardingTable;

import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metadata extractor for sharded entities using reflection
 */
public class EntityMetadata<T> {
    
    private static final Map<Class<?>, EntityMetadata<?>> METADATA_CACHE = new ConcurrentHashMap<>();
    
    private final Class<T> entityClass;
    private final String tableName;
    private final ShardingMode shardingMode;
    private final String shardKey;
    private final int retentionSpanDays;
    private final String partitionAdjustmentTime;
    private final boolean autoManagePartition;
    
    private final List<ColumnMetadata> columns;
    private final Map<String, ColumnMetadata> columnByFieldName;
    private final Map<String, ColumnMetadata> columnByColumnName;
    private final ColumnMetadata primaryKeyColumn;
    private final ColumnMetadata shardKeyColumn;
    
    @SuppressWarnings("unchecked")
    public static <T> EntityMetadata<T> of(Class<T> entityClass) {
        return (EntityMetadata<T>) METADATA_CACHE.computeIfAbsent(entityClass, EntityMetadata::extractMetadata);
    }
    
    private static <T> EntityMetadata<T> extractMetadata(Class<T> entityClass) {
        ShardingTable tableAnnotation = entityClass.getAnnotation(ShardingTable.class);
        if (tableAnnotation == null) {
            throw new IllegalArgumentException("Class " + entityClass.getSimpleName() + " must be annotated with @ShardingTable");
        }
        
        return new EntityMetadata<>(entityClass, tableAnnotation);
    }
    
    private EntityMetadata(Class<T> entityClass, ShardingTable tableAnnotation) {
        this.entityClass = entityClass;
        this.tableName = tableAnnotation.value();
        this.shardingMode = tableAnnotation.mode();
        this.shardKey = tableAnnotation.shardKey();
        this.retentionSpanDays = tableAnnotation.retentionSpanDays();
        this.partitionAdjustmentTime = tableAnnotation.partitionAdjustmentTime();
        this.autoManagePartition = tableAnnotation.autoManagePartition();
        
        // Extract column metadata
        List<ColumnMetadata> columnList = new ArrayList<>();
        Map<String, ColumnMetadata> fieldMap = new HashMap<>();
        Map<String, ColumnMetadata> columnMap = new HashMap<>();
        ColumnMetadata primaryKey = null;
        ColumnMetadata shardKeyCol = null;
        
        for (Field field : entityClass.getDeclaredFields()) {
            field.setAccessible(true);
            
            Column columnAnnotation = field.getAnnotation(Column.class);
            ColumnMetadata columnMetadata = new ColumnMetadata(field, columnAnnotation);
            
            columnList.add(columnMetadata);
            fieldMap.put(field.getName(), columnMetadata);
            columnMap.put(columnMetadata.getColumnName(), columnMetadata);
            
            if (columnMetadata.isPrimaryKey()) {
                primaryKey = columnMetadata;
            }
            
            if (columnMetadata.getColumnName().equals(this.shardKey)) {
                shardKeyCol = columnMetadata;
            }
        }
        
        this.columns = Collections.unmodifiableList(columnList);
        this.columnByFieldName = Collections.unmodifiableMap(fieldMap);
        this.columnByColumnName = Collections.unmodifiableMap(columnMap);
        this.primaryKeyColumn = primaryKey;
        this.shardKeyColumn = shardKeyCol;
        
        if (this.shardKeyColumn == null) {
            throw new IllegalArgumentException("Shard key column '" + this.shardKey + "' not found in entity " + entityClass.getSimpleName());
        }
    }
    
    /**
     * Create a copy of this metadata with a different sharding mode
     * Used by builder to override annotation settings
     */
    public EntityMetadata<T> withShardingMode(ShardingMode newShardingMode) {
        return new EntityMetadata<>(
            this.entityClass,
            this.tableName,
            newShardingMode,
            this.shardKey,
            this.retentionSpanDays,
            this.partitionAdjustmentTime,
            this.autoManagePartition,
            this.columns,
            this.columnByFieldName,
            this.columnByColumnName,
            this.primaryKeyColumn,
            this.shardKeyColumn
        );
    }
    
    /**
     * Private constructor for creating copies with overridden values
     */
    private EntityMetadata(Class<T> entityClass, String tableName, ShardingMode shardingMode,
                          String shardKey, int retentionSpanDays, String partitionAdjustmentTime,
                          boolean autoManagePartition, List<ColumnMetadata> columns,
                          Map<String, ColumnMetadata> columnByFieldName,
                          Map<String, ColumnMetadata> columnByColumnName,
                          ColumnMetadata primaryKeyColumn, ColumnMetadata shardKeyColumn) {
        this.entityClass = entityClass;
        this.tableName = tableName;
        this.shardingMode = shardingMode;
        this.shardKey = shardKey;
        this.retentionSpanDays = retentionSpanDays;
        this.partitionAdjustmentTime = partitionAdjustmentTime;
        this.autoManagePartition = autoManagePartition;
        this.columns = columns;
        this.columnByFieldName = columnByFieldName;
        this.columnByColumnName = columnByColumnName;
        this.primaryKeyColumn = primaryKeyColumn;
        this.shardKeyColumn = shardKeyColumn;
    }
    
    // Getters
    public Class<T> getEntityClass() { return entityClass; }
    public String getTableName() { return tableName; }
    public ShardingMode getShardingMode() { return shardingMode; }
    public String getShardKey() { return shardKey; }
    public int getRetentionSpanDays() { return retentionSpanDays; }
    public String getPartitionAdjustmentTime() { return partitionAdjustmentTime; }
    public boolean isAutoManagePartition() { return autoManagePartition; }
    public List<ColumnMetadata> getColumns() { return columns; }
    public ColumnMetadata getPrimaryKeyColumn() { return primaryKeyColumn; }
    public ColumnMetadata getShardKeyColumn() { return shardKeyColumn; }
    
    public ColumnMetadata getColumnByFieldName(String fieldName) {
        return columnByFieldName.get(fieldName);
    }
    
    public ColumnMetadata getColumnByColumnName(String columnName) {
        return columnByColumnName.get(columnName);
    }
    
    /**
     * Generate table name for a specific date
     */
    public String getTableNameForDate(LocalDateTime date) {
        // Format: tablename_YYYYMMDD
        String dateFormat = date.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        return tableName + "_" + dateFormat;
    }
    
    /**
     * Generate CREATE TABLE SQL
     */
    public String generateCreateTableSql(String actualTableName) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS ").append(actualTableName).append(" (");
        
        List<String> columnDefs = new ArrayList<>();
        List<String> indexes = new ArrayList<>();
        
        for (ColumnMetadata column : columns) {
            columnDefs.add(column.getColumnDefinition());
            
            if (column.isIndexed() && !column.isPrimaryKey()) {
                indexes.add("INDEX idx_" + column.getColumnName() + " (" + column.getColumnName() + ")");
            }
        }
        
        sql.append(String.join(", ", columnDefs));
        
        if (!indexes.isEmpty()) {
            sql.append(", ").append(String.join(", ", indexes));
        }
        
        sql.append(")");
        return sql.toString();
    }
    
    /**
     * Generate INSERT SQL
     */
    public String generateInsertSql() {
        List<String> nonPrimaryColumns = columns.stream()
            .filter(col -> !col.isPrimaryKey())
            .map(ColumnMetadata::getColumnName)
            .toList();
        
        String columnNames = String.join(", ", nonPrimaryColumns);
        String placeholders = String.join(", ", Collections.nCopies(nonPrimaryColumns.size(), "?"));
        
        return "INSERT INTO " + tableName + " (" + columnNames + ") VALUES (" + placeholders + ")";
    }
    
    /**
     * Generate SELECT SQL
     */
    public String generateSelectSql(String whereClause) {
        String columnNames = columns.stream()
            .map(ColumnMetadata::getColumnName)
            .reduce((a, b) -> a + ", " + b)
            .orElse("*");
        
        String sql = "SELECT " + columnNames + " FROM " + tableName;
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sql += " WHERE " + whereClause;
        }
        return sql;
    }
}