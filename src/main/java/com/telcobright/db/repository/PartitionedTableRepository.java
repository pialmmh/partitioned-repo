package com.telcobright.db.repository;

import com.telcobright.db.sharding.ShardingConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class PartitionedTableRepository<T> extends AbstractShardingRepository<T> {
    
    public PartitionedTableRepository(ShardingConfig config) {
        super(config);
    }
    
    @Override
    public void insert(T entity) {
        String sql = getInsertSql();
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            setInsertParameters(stmt, entity);
            stmt.executeUpdate();
            
        } catch (SQLException e) {
            logger.error("Error inserting entity", e);
            throw new RuntimeException("Failed to insert entity", e);
        }
    }
    
    @Override
    public void insertBatch(List<T> entities) {
        if (entities == null || entities.isEmpty()) {
            return;
        }
        
        String sql = getInsertSql();
        
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
            logger.error("Error batch inserting entities", e);
            throw new RuntimeException("Failed to batch insert entities", e);
        }
    }
    
    @Override
    public List<T> findByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        String query = queryBuilderService.buildSelectQuery(startDate, endDate);
        
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
            logger.error("Error finding entities by date range", e);
            throw new RuntimeException("Failed to find entities by date range", e);
        }
    }
    
    protected String getLogicalTableName() {
        return config.getEntityName();
    }
}