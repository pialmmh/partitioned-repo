package com.telcobright.api;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.repository.GenericMultiTableRepository;
import com.telcobright.core.repository.GenericPartitionedTableRepository;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;

/**
 * Public API proxy for sharding repositories.
 * This class provides a unified interface for both multi-table and partitioned table strategies.
 * 
 * @param <T> Entity type that must implement ShardingEntity
 */
public class RepositoryProxy<T extends ShardingEntity> implements ShardingRepository<T> {
    
    private final ShardingRepository<T> delegate;
    private final RepositoryType type;
    
    public enum RepositoryType {
        MULTI_TABLE,
        PARTITIONED_TABLE
    }
    
    private RepositoryProxy(ShardingRepository<T> delegate, RepositoryType type) {
        this.delegate = delegate;
        this.type = type;
    }
    
    /**
     * Create a proxy for a multi-table repository
     */
    public static <T extends ShardingEntity> RepositoryProxy<T> forMultiTable(
            GenericMultiTableRepository<T> repository) {
        return new RepositoryProxy<>(repository, RepositoryType.MULTI_TABLE);
    }
    
    /**
     * Create a proxy for a partitioned table repository
     */
    public static <T extends ShardingEntity> RepositoryProxy<T> forPartitionedTable(
            GenericPartitionedTableRepository<T> repository) {
        return new RepositoryProxy<>(repository, RepositoryType.PARTITIONED_TABLE);
    }
    
    /**
     * Get the repository type (MULTI_TABLE or PARTITIONED_TABLE)
     */
    public RepositoryType getType() {
        return type;
    }
    
    @Override
    public void insert(T entity) throws SQLException {
        delegate.insert(entity);
    }
    
    @Override
    public void insertMultiple(List<T> entities) throws SQLException {
        delegate.insertMultiple(entities);
    }
    
    @Override
    public List<T> findAllByDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        return delegate.findAllByDateRange(startDate, endDate);
    }
    
    @Override
    public T findById(String id) throws SQLException {
        return delegate.findById(id);
    }
    
    @Override
    public T findByIdAndDateRange(LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        return delegate.findByIdAndDateRange(startDate, endDate);
    }
    
    @Override
    public List<T> findAllByIdsAndDateRange(List<String> ids, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        return delegate.findAllByIdsAndDateRange(ids, startDate, endDate);
    }
    
    @Override
    public List<T> findAllBeforeDate(LocalDateTime beforeDate) throws SQLException {
        return delegate.findAllBeforeDate(beforeDate);
    }
    
    @Override
    public List<T> findAllAfterDate(LocalDateTime afterDate) throws SQLException {
        return delegate.findAllAfterDate(afterDate);
    }
    
    @Override
    public void updateById(String id, T entity) throws SQLException {
        delegate.updateById(id, entity);
    }
    
    @Override
    public void updateByIdAndDateRange(String id, T entity, LocalDateTime startDate, LocalDateTime endDate) throws SQLException {
        delegate.updateByIdAndDateRange(id, entity, startDate, endDate);
    }
    
    @Override
    public T findOneByIdGreaterThan(String id) throws SQLException {
        return delegate.findOneByIdGreaterThan(id);
    }
    
    @Override
    public List<T> findBatchByIdGreaterThan(String id, int batchSize) throws SQLException {
        return delegate.findBatchByIdGreaterThan(id, batchSize);
    }
    
    @Override
    public void shutdown() {
        delegate.shutdown();
    }
    
    /**
     * Get the underlying repository implementation
     * Use with caution - this exposes internal implementation details
     */
    @SuppressWarnings("unchecked")
    public <R extends ShardingRepository<T>> R getDelegate() {
        return (R) delegate;
    }
}