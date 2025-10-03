package com.telcobright.core.aggregation;

import java.util.HashMap;
import java.util.Map;

/**
 * Partial aggregation result from a single shard.
 * Contains raw aggregated data before cross-shard merging.
 */
public class PartialResult {
    private final Map<String, Object> groupByValues;
    private final Map<String, Object> aggregateValues;

    public PartialResult() {
        this.groupByValues = new HashMap<>();
        this.aggregateValues = new HashMap<>();
    }

    public void addGroupByValue(String column, Object value) {
        groupByValues.put(column, value);
    }

    public void addAggregateValue(String alias, Object value) {
        aggregateValues.put(alias, value);
    }

    public Map<String, Object> getGroupByValues() {
        return groupByValues;
    }

    public Map<String, Object> getAggregateValues() {
        return aggregateValues;
    }

    public GroupKey getGroupKey() {
        return new GroupKey(groupByValues);
    }

    @Override
    public String toString() {
        return "PartialResult{groupBy=" + groupByValues + ", aggregates=" + aggregateValues + '}';
    }
}
