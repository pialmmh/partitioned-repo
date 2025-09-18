package com.telcobright.api;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.pagination.Page;
import com.telcobright.core.pagination.PageRequest;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Common interface for sharding-aware repositories
 * 
 * Enforces that all entities used with this repository must implement ShardingEntity,
 * which guarantees the presence of required getId/setId and getCreatedAt/setCreatedAt methods.
 * 
 * All IDs are String type (externally generated - NO AUTO_INCREMENT).
 * 
 * @param <T> Entity type that must implement ShardingEntity
 */
public interface ShardingRepository<T extends ShardingEntity> {
    
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
    T findById(String id) throws SQLException;
    
    /**
     * Find first entity within a date range
     */
    T findByIdAndDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException;
    
    /**
     * Find all entities with specific IDs within a date range
     */
    List<T> findAllByIdsAndDateRange(List<String> ids, LocalDateTime startDate, LocalDateTime endDate) throws SQLException;
    
    /**
     * Find all entities before a specific date
     */
    List<T> findAllBeforeDate(LocalDateTime beforeDate) throws SQLException;
    
    /**
     * Find all entities after a specific date
     */
    List<T> findAllAfterDate(LocalDateTime afterDate) throws SQLException;
    
    /**
     * Update entity by primary key
     */
    void updateById(String id, T entity) throws SQLException;
    
    /**
     * Update entity by primary key within a specific date range
     */
    void updateByIdAndDateRange(String id, T entity, LocalDateTime startDate, LocalDateTime endDate) throws SQLException;
    
    /**
     * Find one entity with ID greater than the specified ID.
     * For single partitioned table: Scans full table across all partitions.
     * For multi-table: Scans all tables chronologically to find the first entity with ID > specified ID.
     * Useful for cursor-based iteration and finding next record.
     * 
     * @param id The ID to search greater than
     * @return The first entity found with ID greater than specified ID, or null if none found
     */
    T findOneByIdGreaterThan(String id) throws SQLException;
    
    /**
     * Find batch of entities with ID greater than the specified ID.
     * For single partitioned table: Executes single query with LIMIT across all partitions.
     * For multi-table: Iterates tables chronologically, accumulating results until batchSize is reached.
     * Useful for cursor-based pagination and batch processing.
     *
     * @param id The ID to search greater than
     * @param batchSize Maximum number of entities to return
     * @return List of entities with ID greater than specified ID, up to batchSize
     */
    List<T> findBatchByIdGreaterThan(String id, int batchSize) throws SQLException;

    /**
     * Delete entity by primary key
     */
    void deleteById(String id) throws SQLException;

    /**
     * Delete entity by primary key within a specific date range
     */
    void deleteByIdAndDateRange(String id, LocalDateTime startDate, LocalDateTime endDate) throws SQLException;

    /**
     * Delete all entities within a date range
     */
    void deleteAllByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException;

    /**
     * Shutdown the repository and release resources
     */
    void shutdown();
}