package com.telcobright.db.repository;

import com.telcobright.db.entity.ShardingEntity;
import com.telcobright.db.pagination.Page;
import com.telcobright.db.pagination.PageRequest;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Common interface for sharding-aware repositories
 * 
 * Enforces that all entities used with this repository must implement ShardingEntity<K>,
 * which guarantees the presence of required 'id' and 'created_at' fields.
 * 
 * @param <T> Entity type that must implement ShardingEntity
 * @param <K> Primary key type
 */
public interface ShardingRepository<T extends ShardingEntity<K>, K> {
    
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
     * Update entity by primary key
     */
    void updateById(K id, T entity) throws SQLException;
    
    /**
     * Update entity by primary key within a specific date range
     */
    void updateByIdAndDateRange(K id, T entity, LocalDateTime startDate, LocalDateTime endDate) throws SQLException;
    
    /**
     * Find one entity with ID greater than the specified ID.
     * For single partitioned table: Scans full table across all partitions.
     * For multi-table: Scans all tables chronologically to find the first entity with ID > specified ID.
     * Useful for cursor-based iteration and finding next record.
     * 
     * @param id The ID to search greater than
     * @return The first entity found with ID greater than specified ID, or null if none found
     */
    T findOneByIdGreaterThan(K id) throws SQLException;
    
    /**
     * Shutdown the repository and release resources
     */
    void shutdown();
}