package com.telcobright.core.aggregation;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * Result of an aggregation query containing dimension values (GROUP BY columns)
 * and metric values (aggregate functions).
 */
public class AggregationResult {
    private final Map<String, Object> dimensions;
    private final Map<String, Object> metrics;

    public AggregationResult() {
        this.dimensions = new HashMap<>();
        this.metrics = new HashMap<>();
    }

    public AggregationResult(Map<String, Object> dimensions, Map<String, Object> metrics) {
        this.dimensions = dimensions;
        this.metrics = metrics;
    }

    public void addDimension(String name, Object value) {
        dimensions.put(name, value);
    }

    public void addMetric(String name, Object value) {
        metrics.put(name, value);
    }

    public Map<String, Object> getDimensions() {
        return dimensions;
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public Object getDimension(String name) {
        return dimensions.get(name);
    }

    public Object getMetric(String name) {
        return metrics.get(name);
    }

    public String getString(String name) {
        Object value = dimensions.getOrDefault(name, metrics.get(name));
        return value != null ? value.toString() : null;
    }

    public Long getLong(String name) {
        Object value = metrics.get(name);
        if (value instanceof Long) return (Long) value;
        if (value instanceof Number) return ((Number) value).longValue();
        return null;
    }

    public BigDecimal getBigDecimal(String name) {
        Object value = metrics.get(name);
        if (value instanceof BigDecimal) return (BigDecimal) value;
        if (value instanceof Number) return new BigDecimal(value.toString());
        return null;
    }

    @Override
    public String toString() {
        return "AggregationResult{dimensions=" + dimensions + ", metrics=" + metrics + '}';
    }
}
