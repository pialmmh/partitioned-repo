package com.telcobright.core.aggregation;

import java.util.*;

/**
 * Composite key for grouping aggregation results.
 * Used to merge partial results from different shards.
 */
public class GroupKey {
    private final Map<String, Object> keyValues;

    public GroupKey(Map<String, Object> keyValues) {
        this.keyValues = new LinkedHashMap<>(keyValues);
    }

    public Map<String, Object> getKeyValues() {
        return keyValues;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GroupKey groupKey = (GroupKey) o;
        return Objects.equals(keyValues, groupKey.keyValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyValues);
    }

    @Override
    public String toString() {
        return keyValues.toString();
    }
}
