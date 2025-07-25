package com.telcobright.db.repository;

import com.telcobright.db.sharding.ShardingConfig;
import com.telcobright.db.service.PartitionManagementService;
import com.telcobright.db.service.QueryBuilderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractShardingRepository<T> implements ShardingRepository<T> {
    
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    protected final ShardingConfig config;
    protected final DataSource dataSource;
    protected final PartitionManagementService partitionService;
    protected final QueryBuilderService queryBuilderService;
    
    protected static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    
    public AbstractShardingRepository(ShardingConfig config) {
        this.config = config;
        this.dataSource = config.getDataSource();
        this.partitionService = new PartitionManagementService(config);
        this.queryBuilderService = new QueryBuilderService(config);
    }
    
    @Override
    public void initializePartitions() {
        partitionService.initializePartitions();
    }
    
    @Override
    public void cleanupExpiredPartitions() {
        if (config.isAutoManagePartition()) {
            partitionService.cleanupExpiredPartitions();
        }
    }
    
    @Override
    public long count(LocalDateTime startDate, LocalDateTime endDate) {
        String query = queryBuilderService.buildCountQuery(startDate, endDate);
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            setDateParameters(stmt, startDate, endDate);
            
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        } catch (SQLException e) {
            logger.error("Error counting records", e);
            throw new RuntimeException("Failed to count records", e);
        }
        
        return 0;
    }
    
    @Override
    public List<Map<String, Object>> executeGroupByQuery(String selectFields, String whereClause,
                                                        String groupByFields, LocalDateTime startDate,
                                                        LocalDateTime endDate) {
        String query = queryBuilderService.buildGroupByQuery(selectFields, whereClause, groupByFields, startDate, endDate);
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            setDateParameters(stmt, startDate, endDate);
            
            try (ResultSet rs = stmt.executeQuery()) {
                return mapResultSetToList(rs);
            }
        } catch (SQLException e) {
            logger.error("Error executing group by query", e);
            throw new RuntimeException("Failed to execute group by query", e);
        }
    }
    
    @Override
    public void deleteByDateRange(LocalDateTime startDate, LocalDateTime endDate) {
        String query = queryBuilderService.buildDeleteQuery(startDate, endDate);
        
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {
            
            setDateParameters(stmt, startDate, endDate);
            
            int deletedRows = stmt.executeUpdate();
            logger.info("Deleted {} rows for date range {} to {}", deletedRows, startDate, endDate);
            
        } catch (SQLException e) {
            logger.error("Error deleting records", e);
            throw new RuntimeException("Failed to delete records", e);
        }
    }
    
    protected void setDateParameters(PreparedStatement stmt, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        int paramIndex = 1;
        stmt.setObject(paramIndex++, startDate);
        stmt.setObject(paramIndex, endDate);
    }
    
    protected List<Map<String, Object>> mapResultSetToList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();
        int columnCount = rs.getMetaData().getColumnCount();
        
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = rs.getMetaData().getColumnName(i);
                Object value = rs.getObject(i);
                row.put(columnName, value);
            }
            results.add(row);
        }
        
        return results;
    }
    
    protected abstract String getInsertSql();
    
    protected abstract void setInsertParameters(PreparedStatement stmt, T entity) throws SQLException;
    
    protected abstract T mapResultSetToEntity(ResultSet rs) throws SQLException;
    
    protected abstract String getTableSchema();
}