package com.telcobright.db.repository;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Common interface for sharding-aware repositories
 * 
 * @param <T> Entity type
 * @param <K> Primary key type
 */
public interface ShardingRepository<T, K> {
    
    /**
     * Insert a single entity
     */
    void insert(T entity) throws SQLException;
    
    /**
     * Insert multiple entities with batch optimization
     */
    void insertMultiple(List<T> entities) throws SQLException;
    
    /**
     * Find all entities within a date range
     */
    List<T> findAllByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException;
    
    /**
     * Find entity by primary key
     */
    T findById(K id) throws SQLException;
    
    /**
     * Find first entity within a date range
     */
    T findByIdAndDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException;
    
    /**
     * Find all entities with specific IDs within a date range
     */
    List<T> findAllByIdsAndDateRange(List<K> ids, LocalDateTime startDate, LocalDateTime endDate) throws SQLException;
    
    /**
     * Find all entities before a specific date
     */
    List<T> findAllBeforeDate(LocalDateTime beforeDate) throws SQLException;
    
    /**
     * Find all entities after a specific date
     */
    List<T> findAllAfterDate(LocalDateTime afterDate) throws SQLException;
    
    /**
     * Shutdown the repository and release resources
     */
    void shutdown();
}