package com.telcobright.db.repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

public interface ShardingRepository<T> {
    
    void insert(T entity);
    
    void insertBatch(List<T> entities);
    
    List<T> findByDateRange(LocalDateTime startDate, LocalDateTime endDate);
    
    List<Map<String, Object>> executeGroupByQuery(String selectFields, String whereClause, 
                                                 String groupByFields, LocalDateTime startDate, 
                                                 LocalDateTime endDate);
    
    long count(LocalDateTime startDate, LocalDateTime endDate);
    
    void deleteByDateRange(LocalDateTime startDate, LocalDateTime endDate);
    
    void initializePartitions();
    
    void cleanupExpiredPartitions();
}