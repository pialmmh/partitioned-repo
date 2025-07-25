package com.telcobright.db.service;

import com.telcobright.db.sharding.ShardingConfig;
import com.telcobright.db.sharding.ShardingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class QueryBuilderService {
    
    private static final Logger logger = LoggerFactory.getLogger(QueryBuilderService.class);
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    private final ShardingConfig config;
    
    public QueryBuilderService(ShardingConfig config) {
        this.config = config;
    }
    
    public String buildSelectQuery(LocalDateTime startDate, LocalDateTime endDate) {
        if (config.getMode() == ShardingMode.PARTITIONED_TABLE) {
            return buildPartitionedTableSelectQuery(startDate, endDate);
        } else {
            return buildMultiTableSelectQuery(startDate, endDate);
        }
    }
    
    public String buildCountQuery(LocalDateTime startDate, LocalDateTime endDate) {
        if (config.getMode() == ShardingMode.PARTITIONED_TABLE) {
            return buildPartitionedTableCountQuery(startDate, endDate);
        } else {
            return buildMultiTableCountQuery(startDate, endDate);
        }
    }
    
    public String buildGroupByQuery(String selectFields, String whereClause, String groupByFields, 
                                   LocalDateTime startDate, LocalDateTime endDate) {
        if (config.getMode() == ShardingMode.PARTITIONED_TABLE) {
            return buildPartitionedTableGroupByQuery(selectFields, whereClause, groupByFields, startDate, endDate);
        } else {
            return buildMultiTableGroupByQuery(selectFields, whereClause, groupByFields, startDate, endDate);
        }
    }
    
    public String buildDeleteQuery(LocalDateTime startDate, LocalDateTime endDate) {
        if (config.getMode() == ShardingMode.PARTITIONED_TABLE) {
            return buildPartitionedTableDeleteQuery(startDate, endDate);
        } else {
            return buildMultiTableDeleteQuery(startDate, endDate);
        }
    }
    
    private String buildPartitionedTableSelectQuery(LocalDateTime startDate, LocalDateTime endDate) {
        return String.format("SELECT * FROM %s WHERE %s BETWEEN ? AND ?", 
                           config.getEntityName(), config.getShardKey());
    }
    
    private String buildPartitionedTableCountQuery(LocalDateTime startDate, LocalDateTime endDate) {
        return String.format("SELECT COUNT(*) FROM %s WHERE %s BETWEEN ? AND ?", 
                           config.getEntityName(), config.getShardKey());
    }
    
    private String buildPartitionedTableGroupByQuery(String selectFields, String whereClause, 
                                                   String groupByFields, LocalDateTime startDate, 
                                                   LocalDateTime endDate) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(selectFields);
        sql.append(" FROM ").append(config.getEntityName());
        sql.append(" WHERE ").append(config.getShardKey()).append(" BETWEEN ? AND ?");
        
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sql.append(" AND (").append(whereClause).append(")");
        }
        
        if (groupByFields != null && !groupByFields.trim().isEmpty()) {
            sql.append(" GROUP BY ").append(groupByFields);
        }
        
        return sql.toString();
    }
    
    private String buildPartitionedTableDeleteQuery(LocalDateTime startDate, LocalDateTime endDate) {
        return String.format("DELETE FROM %s WHERE %s BETWEEN ? AND ?", 
                           config.getEntityName(), config.getShardKey());
    }
    
    private String buildMultiTableSelectQuery(LocalDateTime startDate, LocalDateTime endDate) {
        List<String> tableNames = getTableNamesForDateRange(startDate, endDate);
        
        if (tableNames.isEmpty()) {
            return "SELECT * FROM dual WHERE FALSE";
        }
        
        StringJoiner unionQuery = new StringJoiner(" UNION ALL ");
        
        for (String tableName : tableNames) {
            String tableQuery = String.format("SELECT * FROM %s WHERE %s BETWEEN ? AND ?", 
                                             tableName, config.getShardKey());
            unionQuery.add(tableQuery);
        }
        
        return unionQuery.toString();
    }
    
    private String buildMultiTableCountQuery(LocalDateTime startDate, LocalDateTime endDate) {
        List<String> tableNames = getTableNamesForDateRange(startDate, endDate);
        
        if (tableNames.isEmpty()) {
            return "SELECT 0 as count";
        }
        
        StringJoiner unionQuery = new StringJoiner(" UNION ALL ");
        
        for (String tableName : tableNames) {
            String tableQuery = String.format("SELECT COUNT(*) as count FROM %s WHERE %s BETWEEN ? AND ?", 
                                             tableName, config.getShardKey());
            unionQuery.add(tableQuery);
        }
        
        return String.format("SELECT SUM(count) FROM (%s) t", unionQuery.toString());
    }
    
    private String buildMultiTableGroupByQuery(String selectFields, String whereClause, 
                                             String groupByFields, LocalDateTime startDate, 
                                             LocalDateTime endDate) {
        List<String> tableNames = getTableNamesForDateRange(startDate, endDate);
        
        if (tableNames.isEmpty()) {
            return "SELECT " + selectFields + " FROM dual WHERE FALSE";
        }
        
        StringJoiner unionQuery = new StringJoiner(" UNION ALL ");
        
        for (String tableName : tableNames) {
            StringBuilder tableQuery = new StringBuilder();
            tableQuery.append("SELECT ").append(selectFields);
            tableQuery.append(" FROM ").append(tableName);
            tableQuery.append(" WHERE ").append(config.getShardKey()).append(" BETWEEN ? AND ?");
            
            if (whereClause != null && !whereClause.trim().isEmpty()) {
                tableQuery.append(" AND (").append(whereClause).append(")");
            }
            
            if (groupByFields != null && !groupByFields.trim().isEmpty()) {
                tableQuery.append(" GROUP BY ").append(groupByFields);
            }
            
            unionQuery.add(tableQuery.toString());
        }
        
        if (groupByFields != null && !groupByFields.trim().isEmpty()) {
            return String.format("SELECT %s FROM (%s) t GROUP BY %s", 
                               selectFields, unionQuery.toString(), groupByFields);
        } else {
            return unionQuery.toString();
        }
    }
    
    private String buildMultiTableDeleteQuery(LocalDateTime startDate, LocalDateTime endDate) {
        List<String> tableNames = getTableNamesForDateRange(startDate, endDate);
        
        StringJoiner deleteQueries = new StringJoiner("; ");
        
        for (String tableName : tableNames) {
            String deleteQuery = String.format("DELETE FROM %s WHERE %s BETWEEN ? AND ?", 
                                             tableName, config.getShardKey());
            deleteQueries.add(deleteQuery);
        }
        
        return deleteQueries.toString();
    }
    
    private List<String> getTableNamesForDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        List<String> tableNames = new ArrayList<>();
        LocalDate current = startDate.toLocalDate();
        LocalDate end = endDate.toLocalDate();
        
        while (!current.isAfter(end)) {
            String tableName = config.getEntityName() + current.format(DATE_FORMAT);
            tableNames.add(tableName);
            current = current.plusDays(1);
        }
        
        return tableNames;
    }
}