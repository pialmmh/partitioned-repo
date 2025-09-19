package com.telcobright.core.pagination;

import com.telcobright.core.entity.ShardingEntity;
import com.telcobright.core.query.SortExpression;
import com.telcobright.core.logging.Logger;
import com.telcobright.core.logging.ConsoleLogger;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.lang.reflect.Method;

/**
 * Handles pagination across multiple shards with proper sorting and memory management
 *
 * Algorithm:
 * 1. Query each shard for up to maxResultCount records
 * 2. Merge all results maintaining sort order
 * 3. Apply pagination to get the requested page
 *
 * For example, if we need 100 records in page 2 (0-based):
 * - Fetch up to maxResultCount from each shard
 * - Merge and sort all results
 * - Skip first 200 records (page 0: 0-99, page 1: 100-199)
 * - Return next 100 records (page 2: 200-299)
 */
public class CrossShardPaginator<T extends ShardingEntity<?>> {

    private static final Logger logger = new ConsoleLogger("CrossShardPaginator");

    /**
     * Execute paginated query across multiple shards
     */
    public static <T extends ShardingEntity<?>> List<T> paginateAcrossShards(
            Map<String, Callable<List<T>>> shardQueries,
            PaginationConfig config,
            ExecutorService executor) throws Exception {

        // Step 1: Fetch data from all shards in parallel
        logger.info(String.format(
            "Fetching data from %d shards for page %d (size %d)",
            shardQueries.size(), config.getPageNumber(), config.getPageSize()
        ));

        List<Future<List<T>>> futures = new ArrayList<>();
        for (Map.Entry<String, Callable<List<T>>> entry : shardQueries.entrySet()) {
            String shardId = entry.getKey();
            Callable<List<T>> query = entry.getValue();

            Future<List<T>> future = executor.submit(() -> {
                try {
                    List<T> result = query.call();
                    logger.info(String.format("Shard %s returned %d records", shardId, result.size()));
                    return result;
                } catch (Exception e) {
                    logger.error("Error querying shard " + shardId + ": " + e.getMessage());
                    return new ArrayList<>();
                }
            });
            futures.add(future);
        }

        // Step 2: Collect all results
        List<T> allResults = new ArrayList<>();
        int totalRecords = 0;

        for (Future<List<T>> future : futures) {
            List<T> shardResults = future.get(30, TimeUnit.SECONDS); // 30 second timeout per shard
            allResults.addAll(shardResults);
            totalRecords += shardResults.size();

            // Stop if we've hit the max result count
            if (totalRecords >= config.getMaxResultCount()) {
                logger.warn(String.format(
                    "Hit maxResultCount limit of %d. Truncating results.",
                    config.getMaxResultCount()
                ));
                allResults = allResults.subList(0, config.getMaxResultCount());
                break;
            }
        }

        logger.info(String.format("Collected %d total records from all shards", allResults.size()));

        // Step 3: Sort the combined results
        if (config.getSortExpression() != null) {
            sortResults(allResults, config.getSortExpression());
        }

        // Step 4: Apply pagination
        int startIndex = config.getStartIndex();
        int endIndex = Math.min(config.getEndIndex(), allResults.size());

        if (startIndex >= allResults.size()) {
            logger.info(String.format(
                "Page %d is beyond available results (total: %d)",
                config.getPageNumber(), allResults.size()
            ));
            return new ArrayList<>();
        }

        List<T> pageResults = allResults.subList(startIndex, endIndex);

        logger.info(String.format(
            "Returning page %d: records %d-%d of %d total",
            config.getPageNumber(), startIndex, endIndex - 1, allResults.size()
        ));

        return new ArrayList<>(pageResults); // Return a copy to avoid subList issues
    }

    /**
     * Sort results based on sort expression
     */
    private static <T> void sortResults(List<T> results, SortExpression sortExpression) {
        if (sortExpression.getSortFields().isEmpty()) {
            return;
        }

        Comparator<T> comparator = null;

        for (SortExpression.SortField sortField : sortExpression.getSortFields()) {
            Comparator<T> fieldComparator = createFieldComparator(sortField);

            if (comparator == null) {
                comparator = fieldComparator;
            } else {
                comparator = comparator.thenComparing(fieldComparator);
            }
        }

        if (comparator != null) {
            results.sort(comparator);
        }
    }

    /**
     * Create a comparator for a specific field
     */
    @SuppressWarnings("unchecked")
    private static <T> Comparator<T> createFieldComparator(SortExpression.SortField sortField) {
        return (o1, o2) -> {
            try {
                // Get the field value using reflection
                Object value1 = getFieldValue(o1, sortField.getFieldName());
                Object value2 = getFieldValue(o2, sortField.getFieldName());

                // Handle nulls
                if (value1 == null && value2 == null) return 0;
                if (value1 == null) return sortField.getOrder() == SortExpression.SortOrder.ASC ? -1 : 1;
                if (value2 == null) return sortField.getOrder() == SortExpression.SortOrder.ASC ? 1 : -1;

                // Compare values
                int comparison;
                if (value1 instanceof Comparable) {
                    comparison = ((Comparable) value1).compareTo(value2);
                } else {
                    comparison = value1.toString().compareTo(value2.toString());
                }

                // Apply sort order
                return sortField.getOrder() == SortExpression.SortOrder.ASC ? comparison : -comparison;

            } catch (Exception e) {
                logger.error("Error comparing field " + sortField.getFieldName() + ": " + e.getMessage());
                return 0;
            }
        };
    }

    /**
     * Get field value using reflection
     */
    private static Object getFieldValue(Object obj, String fieldName) throws Exception {
        // Try direct field access first
        String getterName = "get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

        try {
            Method method = obj.getClass().getMethod(getterName);
            return method.invoke(obj);
        } catch (NoSuchMethodException e) {
            // Try with "is" prefix for boolean
            String isGetterName = "is" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
            try {
                Method method = obj.getClass().getMethod(isGetterName);
                return method.invoke(obj);
            } catch (NoSuchMethodException e2) {
                // Try direct field name as getter (e.g., for "id" -> "getId")
                if (fieldName.equals("id")) {
                    Method method = obj.getClass().getMethod("getId");
                    return method.invoke(obj);
                }
                throw new NoSuchMethodException("No getter found for field: " + fieldName);
            }
        }
    }

    /**
     * Calculate total pages available
     */
    public static int calculateTotalPages(int totalRecords, int pageSize) {
        return (totalRecords + pageSize - 1) / pageSize;
    }

    /**
     * Create a page info object with metadata
     */
    public static PageInfo createPageInfo(int pageNumber, int pageSize, int totalRecords) {
        return new PageInfo(
            pageNumber,
            pageSize,
            totalRecords,
            calculateTotalPages(totalRecords, pageSize),
            pageNumber > 0,
            (pageNumber + 1) * pageSize < totalRecords
        );
    }

    /**
     * Page metadata
     */
    public static class PageInfo {
        public final int pageNumber;
        public final int pageSize;
        public final int totalRecords;
        public final int totalPages;
        public final boolean hasPrevious;
        public final boolean hasNext;

        public PageInfo(int pageNumber, int pageSize, int totalRecords,
                       int totalPages, boolean hasPrevious, boolean hasNext) {
            this.pageNumber = pageNumber;
            this.pageSize = pageSize;
            this.totalRecords = totalRecords;
            this.totalPages = totalPages;
            this.hasPrevious = hasPrevious;
            this.hasNext = hasNext;
        }

        @Override
        public String toString() {
            return String.format(
                "Page %d of %d (size: %d, total records: %d, has prev: %s, has next: %s)",
                pageNumber + 1, totalPages, pageSize, totalRecords, hasPrevious, hasNext
            );
        }
    }
}