package com.telcobright.core.aggregation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Map;

/**
 * Accumulates partial aggregate results for merging across shards.
 */
public class AggregateAccumulator {
    private final Map<String, Object> values = new HashMap<>();

    // For AVG calculation
    private final Map<String, BigDecimal> avgSums = new HashMap<>();
    private final Map<String, Long> avgCounts = new HashMap<>();

    public void accumulate(AggregateColumn column, Object value) {
        String alias = column.getAlias();

        switch (column.getFunction()) {
            case SUM:
                accumulateSum(alias, value);
                break;
            case COUNT:
                accumulateCount(alias, value);
                break;
            case MAX:
                accumulateMax(alias, value);
                break;
            case MIN:
                accumulateMin(alias, value);
                break;
            case AVG:
                // AVG is handled separately with sum and count
                throw new UnsupportedOperationException("AVG should be decomposed to SUM and COUNT");
            default:
                throw new IllegalArgumentException("Unknown aggregate function: " + column.getFunction());
        }
    }

    public void accumulateAvg(String alias, BigDecimal sum, Long count) {
        avgSums.merge(alias, sum, BigDecimal::add);
        avgCounts.merge(alias, count, Long::sum);
    }

    private void accumulateSum(String alias, Object value) {
        if (value == null) return;

        BigDecimal numValue = toBigDecimal(value);
        values.merge(alias, numValue, (old, newVal) ->
            ((BigDecimal) old).add((BigDecimal) newVal));
    }

    private void accumulateCount(String alias, Object value) {
        if (value == null) return;

        Long numValue = toLong(value);
        values.merge(alias, numValue, (old, newVal) ->
            ((Long) old) + ((Long) newVal));
    }

    private void accumulateMax(String alias, Object value) {
        if (value == null) return;

        if (!values.containsKey(alias)) {
            values.put(alias, value);
        } else {
            Comparable current = (Comparable) values.get(alias);
            Comparable newValue = (Comparable) value;
            if (newValue.compareTo(current) > 0) {
                values.put(alias, value);
            }
        }
    }

    private void accumulateMin(String alias, Object value) {
        if (value == null) return;

        if (!values.containsKey(alias)) {
            values.put(alias, value);
        } else {
            Comparable current = (Comparable) values.get(alias);
            Comparable newValue = (Comparable) value;
            if (newValue.compareTo(current) < 0) {
                values.put(alias, value);
            }
        }
    }

    public Map<String, Object> getValues() {
        Map<String, Object> result = new HashMap<>(values);

        // Calculate AVG values
        for (String alias : avgSums.keySet()) {
            BigDecimal sum = avgSums.get(alias);
            Long count = avgCounts.get(alias);
            if (count > 0) {
                BigDecimal avg = sum.divide(new BigDecimal(count), 4, RoundingMode.HALF_UP);
                result.put(alias, avg);
            }
        }

        return result;
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) return (BigDecimal) value;
        if (value instanceof Number) return new BigDecimal(value.toString());
        return new BigDecimal(value.toString());
    }

    private Long toLong(Object value) {
        if (value instanceof Long) return (Long) value;
        if (value instanceof Number) return ((Number) value).longValue();
        return Long.parseLong(value.toString());
    }
}
