package com.telcobright.core.pagination;

import com.telcobright.core.query.SortExpression;

/**
 * Configuration for pagination across shards
 */
public class PaginationConfig {

    private static final int DEFAULT_MAX_RESULT_COUNT = 100_000;
    private static final int DEFAULT_PAGE_SIZE = 100;
    private static final long ESTIMATED_BYTES_PER_RECORD = 1024; // 1KB estimate per record

    private final int maxResultCount;
    private final int pageSize;
    private final int pageNumber; // 0-based
    private final SortExpression sortExpression;
    private final boolean validateMemory;

    private PaginationConfig(Builder builder) {
        this.maxResultCount = builder.maxResultCount;
        this.pageSize = builder.pageSize;
        this.pageNumber = builder.pageNumber;
        this.sortExpression = builder.sortExpression;
        this.validateMemory = builder.validateMemory;

        if (validateMemory) {
            validateMemoryCapacity();
        }
    }

    /**
     * Validates that JVM has enough memory to hold maxResultCount records
     */
    private void validateMemoryCapacity() {
        long maxMemory = Runtime.getRuntime().maxMemory();
        long requiredMemory = maxResultCount * ESTIMATED_BYTES_PER_RECORD;

        // Leave 20% buffer for JVM overhead
        long availableMemory = (long) (maxMemory * 0.8);

        if (requiredMemory > availableMemory) {
            throw new IllegalStateException(String.format(
                "Insufficient memory for pagination. Required: %d MB, Available: %d MB. " +
                "Reduce maxResultCount (%d) or increase JVM heap size.",
                requiredMemory / (1024 * 1024),
                availableMemory / (1024 * 1024),
                maxResultCount
            ));
        }
    }

    /**
     * Calculate the offset for pagination
     */
    public int getOffset() {
        return pageNumber * pageSize;
    }

    /**
     * Calculate the limit for the current page
     */
    public int getLimit() {
        return pageSize;
    }

    /**
     * Get the absolute start index for current page
     */
    public int getStartIndex() {
        return pageNumber * pageSize;
    }

    /**
     * Get the absolute end index for current page (exclusive)
     */
    public int getEndIndex() {
        int endIndex = getStartIndex() + pageSize;
        return Math.min(endIndex, maxResultCount);
    }

    /**
     * Check if a given index falls within the current page
     */
    public boolean isInCurrentPage(int index) {
        return index >= getStartIndex() && index < getEndIndex();
    }

    // Getters
    public int getMaxResultCount() { return maxResultCount; }
    public int getPageSize() { return pageSize; }
    public int getPageNumber() { return pageNumber; }
    public SortExpression getSortExpression() { return sortExpression; }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int maxResultCount = DEFAULT_MAX_RESULT_COUNT;
        private int pageSize = DEFAULT_PAGE_SIZE;
        private int pageNumber = 0;
        private SortExpression sortExpression = SortExpression.builder()
            .addAscending("id")  // Default sort by ID
            .build();
        private boolean validateMemory = true;

        public Builder withMaxResultCount(int maxResultCount) {
            if (maxResultCount <= 0) {
                throw new IllegalArgumentException("maxResultCount must be positive");
            }
            this.maxResultCount = maxResultCount;
            return this;
        }

        public Builder withPageSize(int pageSize) {
            if (pageSize <= 0) {
                throw new IllegalArgumentException("pageSize must be positive");
            }
            this.pageSize = pageSize;
            return this;
        }

        public Builder withPageNumber(int pageNumber) {
            if (pageNumber < 0) {
                throw new IllegalArgumentException("pageNumber must be non-negative");
            }
            this.pageNumber = pageNumber;
            return this;
        }

        public Builder withSortExpression(SortExpression sortExpression) {
            this.sortExpression = sortExpression;
            return this;
        }

        public Builder withMemoryValidation(boolean validateMemory) {
            this.validateMemory = validateMemory;
            return this;
        }

        public PaginationConfig build() {
            return new PaginationConfig(this);
        }
    }
}