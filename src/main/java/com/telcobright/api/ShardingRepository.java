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
 * which guarantees the presence of required getId/setId and partition column accessors.
 *
 * All IDs are String type (externally generated - NO AUTO_INCREMENT).
 * Partition columns can be any Comparable type (LocalDateTime, Long, String, etc.)
 *
 * @param <T> Entity type that must implement ShardingEntity
 * @param <P> Partition column value type (must be Comparable)
 */
public interface ShardingRepository<T extends ShardingEntity<P>, P extends Comparable<? super P>> {
    
    /**
     * Insert a single entity
     */
    void insert(T entity) throws SQLException;
    
    /**
     * Insert multiple entities with batch optimization
     */
    void insertMultiple(List<T> entities) throws SQLException;
    
    /**
     * Find all entities within a partition value range
     */
    List<T> findAllByPartitionRange(P startValue, P endValue) throws SQLException;

    /**
     * Find all entities within a date range     * @deprecated Use findAllByPartitionRange for generic support
     */
    @Deprecated
    default List<T> findAllByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        try {
            @SuppressWarnings("unchecked")
            P start = (P) startDate;
            @SuppressWarnings("unchecked")
            P end = (P) endDate;
            return findAllByPartitionRange(start, end);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This repository does not use LocalDateTime for partitioning", e);
        }
    }
    
    /**
     * Find entity by primary key
     */
    T findById(String id) throws SQLException;
    
    /**
     * Find first entity within a partition value range
     */
    T findByIdAndPartitionColRange(String id, P startValue, P endValue) throws SQLException;

    /**
     * Find first entity within a date range     * @deprecated Use findByIdAndPartitionColRange for generic support
     */
    @Deprecated
    default T findByIdAndDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        try {
            @SuppressWarnings("unchecked")
            P start = (P) startDate;
            @SuppressWarnings("unchecked")
            P end = (P) endDate;
            return findByIdAndPartitionColRange(null, start, end);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This repository does not use LocalDateTime for partitioning", e);
        }
    }
    
    /**
     * Find all entities with specific IDs within a partition value range
     */
    List<T> findAllByIdsAndPartitionColRange(List<String> ids, P startValue, P endValue) throws SQLException;

    /**
     * Find all entities with specific IDs within a date range     * @deprecated Use findAllByIdsAndPartitionColRange for generic support
     */
    @Deprecated
    default List<T> findAllByIdsAndDateRange(List<String> ids, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        try {
            @SuppressWarnings("unchecked")
            P start = (P) startDate;
            @SuppressWarnings("unchecked")
            P end = (P) endDate;
            return findAllByIdsAndPartitionColRange(ids, start, end);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This repository does not use LocalDateTime for partitioning", e);
        }
    }
    
    /**
     * Find all entities before a specific partition value
     */
    List<T> findAllBeforePartitionValue(P beforeValue) throws SQLException;

    /**
     * Find all entities before a specific date     * @deprecated Use findAllBeforePartitionValue for generic support
     */
    @Deprecated
    default List<T> findAllBeforeDate(LocalDateTime beforeDate) throws SQLException {
        try {
            @SuppressWarnings("unchecked")
            P before = (P) beforeDate;
            return findAllBeforePartitionValue(before);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This repository does not use LocalDateTime for partitioning", e);
        }
    }
    
    /**
     * Find all entities after a specific partition value
     */
    List<T> findAllAfterPartitionValue(P afterValue) throws SQLException;

    /**
     * Find all entities after a specific date     * @deprecated Use findAllAfterPartitionValue for generic support
     */
    @Deprecated
    default List<T> findAllAfterDate(LocalDateTime afterDate) throws SQLException {
        try {
            @SuppressWarnings("unchecked")
            P after = (P) afterDate;
            return findAllAfterPartitionValue(after);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This repository does not use LocalDateTime for partitioning", e);
        }
    }
    
    /**
     * Update entity by primary key
     */
    void updateById(String id, T entity) throws SQLException;
    
    /**
     * Update entity by primary key within a specific partition value range
     */
    void updateByIdAndPartitionColRange(String id, T entity, P startValue, P endValue) throws SQLException;

    /**
     * Update entity by primary key within a specific date range     * @deprecated Use updateByIdAndPartitionColRange for generic support
     */
    @Deprecated
    default void updateByIdAndDateRange(String id, T entity, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        try {
            @SuppressWarnings("unchecked")
            P start = (P) startDate;
            @SuppressWarnings("unchecked")
            P end = (P) endDate;
            updateByIdAndPartitionColRange(id, entity, start, end);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This repository does not use LocalDateTime for partitioning", e);
        }
    }
    
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
     * Delete entity by primary key within a specific partition value range
     */
    void deleteByIdAndPartitionColRange(String id, P startValue, P endValue) throws SQLException;

    /**
     * Delete entity by primary key within a specific date range     * @deprecated Use deleteByIdAndPartitionColRange for generic support
     */
    @Deprecated
    default void deleteByIdAndDateRange(String id, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        try {
            @SuppressWarnings("unchecked")
            P start = (P) startDate;
            @SuppressWarnings("unchecked")
            P end = (P) endDate;
            deleteByIdAndPartitionColRange(id, start, end);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This repository does not use LocalDateTime for partitioning", e);
        }
    }

    /**
     * Delete all entities within a partition value range
     */
    void deleteAllByPartitionRange(P startValue, P endValue) throws SQLException;

    /**
     * Delete all entities within a date range     * @deprecated Use deleteAllByPartitionRange for generic support
     */
    @Deprecated
    default void deleteAllByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        try {
            @SuppressWarnings("unchecked")
            P start = (P) startDate;
            @SuppressWarnings("unchecked")
            P end = (P) endDate;
            deleteAllByPartitionRange(start, end);
        } catch (ClassCastException e) {
            throw new UnsupportedOperationException("This repository does not use LocalDateTime for partitioning", e);
        }
    }

    /**
     * Shutdown the repository and release resources
     */
    void shutdown();
}