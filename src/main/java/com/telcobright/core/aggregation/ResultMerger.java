package com.telcobright.core.aggregation;

import com.telcobright.core.logging.Logger;
import com.telcobright.core.query.SortExpression;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Merges partial aggregation results from multiple shards into final results.
 */
public class ResultMerger {
    private final Logger logger;

    public ResultMerger(Logger logger) {
        this.logger = logger;
    }

    /**
     * Merge partial results from all shards.
     */
    public List<AggregationResult> merge(
        List<PartialResult> partialResults,
        AggregationQuery query
    ) {
        logger.debug("Merging " + partialResults.size() + " partial results");

        // Group partial results by GROUP BY key
        Map<GroupKey, AggregateAccumulator> accumulators = new HashMap<>();

        for (PartialResult partial : partialResults) {
            GroupKey groupKey = partial.getGroupKey();
            AggregateAccumulator accumulator = accumulators.computeIfAbsent(groupKey, k -> new AggregateAccumulator());

            // Accumulate each aggregate column
            for (AggregateColumn aggCol : query.getAggregateColumns()) {
                if (aggCol.getFunction() == AggregateFunction.AVG) {
                    // Handle AVG specially
                    BigDecimal sum = (BigDecimal) partial.getAggregateValues().get("__" + aggCol.getAlias() + "_sum");
                    Long count = ((Number) partial.getAggregateValues().get("__" + aggCol.getAlias() + "_count")).longValue();
                    accumulator.accumulateAvg(aggCol.getAlias(), sum, count);
                } else {
                    Object value = partial.getAggregateValues().get(aggCol.getAlias());
                    accumulator.accumulate(aggCol, value);
                }
            }
        }

        // Convert to AggregationResult objects
        List<AggregationResult> results = new ArrayList<>();
        for (Map.Entry<GroupKey, AggregateAccumulator> entry : accumulators.entrySet()) {
            AggregationResult result = new AggregationResult(
                entry.getKey().getKeyValues(),
                entry.getValue().getValues()
            );
            results.add(result);
        }

        logger.debug("Merged into " + results.size() + " final results");

        // Apply HAVING filter
        if (query.getHavingClause() != null) {
            results = applyHaving(results, query.getHavingClause());
        }

        // Apply ORDER BY
        if (query.getSortExpression() != null) {
            results = applySorting(results, query.getSortExpression());
        }

        // Apply LIMIT
        if (query.getLimit() != null) {
            results = results.stream()
                .limit(query.getLimit())
                .collect(Collectors.toList());
        }

        return results;
    }

    private List<AggregationResult> applyHaving(List<AggregationResult> results, String havingClause) {
        // TODO: Implement HAVING clause evaluation
        // For now, throw exception for complex HAVING clauses
        logger.warn("HAVING clause not yet implemented: " + havingClause);
        throw new UnsupportedOperationException("HAVING clause evaluation not yet implemented. " +
            "Please filter results in application code for now.");
    }

    private List<AggregationResult> applySorting(List<AggregationResult> results, SortExpression sortExpression) {
        if (sortExpression == null || sortExpression.getSortFields().isEmpty()) {
            return results;
        }

        // Simple sorting implementation
        List<AggregationResult> sorted = new ArrayList<>(results);

        sorted.sort((r1, r2) -> {
            for (SortExpression.SortField field : sortExpression.getSortFields()) {
                String column = field.getFieldName();

                // Try to find value in dimensions first, then metrics
                Object val1 = r1.getDimensions().getOrDefault(column, r1.getMetrics().get(column));
                Object val2 = r2.getDimensions().getOrDefault(column, r2.getMetrics().get(column));

                if (val1 == null && val2 == null) continue;
                if (val1 == null) return 1;
                if (val2 == null) return -1;

                int comparison;
                if (val1 instanceof Comparable) {
                    comparison = ((Comparable) val1).compareTo(val2);
                } else {
                    comparison = val1.toString().compareTo(val2.toString());
                }

                if (comparison != 0) {
                    return field.getOrder() == SortExpression.SortOrder.DESC ? -comparison : comparison;
                }
            }
            return 0;
        });

        return sorted;
    }
}
