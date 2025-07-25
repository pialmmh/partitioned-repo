package com.telcobright.db.repository;

import com.telcobright.db.sharding.ShardingConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class MultiTableRepository<T> extends AbstractShardingRepository<T> {
    
    public MultiTableRepository(ShardingConfig config) {
        super(config);
    }
    
    @Override
    public void insert(T entity) {
        String tableName = getTableNameForEntity(entity);
        String sql = getInsertSql().replace(config.getEntityName(), tableName);
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            setInsertParameters(stmt, entity);
            stmt.executeUpdate();
            
        } catch (SQLException e) {
            logger.error("Error inserting entity into table {}", tableName, e);
            throw new RuntimeException("Failed to insert entity into table " + tableName, e);
        }
    }
    
    @Override
    public void insertBatch(List<T> entities) {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        
        Map<String, List<T>> entitiesByTable = new HashMap<>();
        
        for (T entity : entities) {
            String tableName = getTableNameForEntity(entity);
            entitiesByTable.computeIfAbsent(tableName, k -> new ArrayList<>()).add(entity);
        }
        
        for (Map.Entry<String, List<T>> entry : entitiesByTable.entrySet()) {
            insertBatchIntoTable(entry.getKey(), entry.getValue());
        }
    }
    
    private void insertBatchIntoTable(String tableName, List<T> entities) {
        String sql = getInsertSql().replace(config.getEntityName(), tableName);
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            conn.setAutoCommit(false);
            
            for (T entity : entities) {
                setInsertParameters(stmt, entity);
                stmt.addBatch();
            }
            
            stmt.executeBatch();
            conn.commit();
            
        } catch (SQLException e) {
            logger.error("Error batch inserting entities into table {}", tableName, e);
            throw new RuntimeException("Failed to batch insert entities into table " + tableName, e);
        }
    }
    
    @Override
    public List<T> findByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        List<T> allResults = new ArrayList<>();
        List<String> tableNames = getTableNamesForDateRange(startDate, endDate);
        
        for (String tableName : tableNames) {
            List<T> tableResults = findFromTable(tableName, startDate, endDate);
            allResults.addAll(tableResults);
        }
        
        return allResults;
    }
    
    private List<T> findFromTable(String tableName, LocalDateTime startDate, LocalDateTime endDate) {
        String query = String.format("SELECT * FROM %s WHERE %s BETWEEN ? AND ?", 
                                    tableName, config.getShardKey());
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            setDateParameters(stmt, startDate, endDate);
            
            try (ResultSet rs = stmt.executeQuery()) {
                List<T> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(mapResultSetToEntity(rs));
                }
                return results;
            }
            
        } catch (SQLException e) {
            logger.error("Error finding entities from table {}", tableName, e);
            throw new RuntimeException("Failed to find entities from table " + tableName, e);
        }
    }
    
    protected String getTableNameForDate(LocalDateTime date) {
        return config.getEntityName() + date.format(DATE_FORMAT);
    }
    
    protected List<String> getTableNamesForDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        List<String> tableNames = new ArrayList<>();
        LocalDateTime current = startDate.toLocalDate().atStartOfDay();
        
        while (!current.isAfter(endDate)) {
            tableNames.add(getTableNameForDate(current));
            current = current.plusDays(1);
        }
        
        return tableNames;
    }
    
    protected abstract String getTableNameForEntity(T entity);
    
    protected abstract LocalDateTime getDateFromEntity(T entity);
}