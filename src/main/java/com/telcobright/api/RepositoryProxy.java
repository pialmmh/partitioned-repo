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
 * @param <P> Partition column value type (must be Comparable)
 */
public class RepositoryProxy<T extends ShardingEntity<P>, P extends Comparable<? super P>> implements ShardingRepository<T, P> {
    
    private final ShardingRepository<T, P> delegate;
    private final RepositoryType type;
    
    public enum RepositoryType {
        MULTI_TABLE,
        PARTITIONED_TABLE
    }
    
    private RepositoryProxy(ShardingRepository<T, P> delegate, RepositoryType type) {
        this.delegate = delegate;
        this.type = type;
    }
    
    /**
     * Create a proxy for a multi-table repository
     */
    public static <T extends ShardingEntity<P>, P extends Comparable<P>> RepositoryProxy<T, P> forMultiTable(
            GenericMultiTableRepository<T, P> repository) {
        return new RepositoryProxy<T, P>(repository, RepositoryType.MULTI_TABLE);
    }
    
    /**
     * Create a proxy for a partitioned table repository
     */
    public static <T extends ShardingEntity<P>, P extends Comparable<P>> RepositoryProxy<T, P> forPartitionedTable(
            GenericPartitionedTableRepository<T, P> repository) {
        return new RepositoryProxy<T, P>(repository, RepositoryType.PARTITIONED_TABLE);
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
    public List<T> findAllByPartitionRange(P startValue, P endValue) throws SQLException {
        return delegate.findAllByPartitionRange(startValue, endValue);
    }
    
    @Override
    public T findById(String id) throws SQLException {
        return delegate.findById(id);
    }
    
    @Override
    public T findByIdAndPartitionRange(String id, P startValue, P endValue) throws SQLException {
        return delegate.findByIdAndPartitionRange(id, startValue, endValue);
    }
    
    @Override
    public List<T> findAllByIdsAndPartitionRange(List<String> ids, P startValue, P endValue) throws SQLException {
        return delegate.findAllByIdsAndPartitionRange(ids, startValue, endValue);
    }
    
    @Override
    public List<T> findAllBeforePartitionValue(P beforeValue) throws SQLException {
        return delegate.findAllBeforePartitionValue(beforeValue);
    }
    
    @Override
    public List<T> findAllAfterPartitionValue(P afterValue) throws SQLException {
        return delegate.findAllAfterPartitionValue(afterValue);
    }
    
    @Override
    public void updateById(String id, T entity) throws SQLException {
        delegate.updateById(id, entity);
    }
    
    @Override
    public void updateByIdAndPartitionRange(String id, T entity, P startValue, P endValue) throws SQLException {
        delegate.updateByIdAndPartitionRange(id, entity, startValue, endValue);
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
    public void deleteById(String id) throws SQLException {
        delegate.deleteById(id);
    }

    @Override
    public void deleteByIdAndPartitionRange(String id, P startValue, P endValue) throws SQLException {
        delegate.deleteByIdAndPartitionRange(id, startValue, endValue);
    }

    @Override
    public void deleteAllByPartitionRange(P startValue, P endValue) throws SQLException {
        delegate.deleteAllByPartitionRange(startValue, endValue);
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
    public <R extends ShardingRepository<T, P>> R getDelegate() {
        return (R) delegate;
    }
}