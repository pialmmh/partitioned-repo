package com.telcobright.core.pagination;

/**
 * Pagination request parameters
 */
public class PageRequest {
    private final int page;        // 0-based page number
    private final int size;        // number of items per page
    private final String sortBy;   // column to sort by (optional)
    private final SortDirection sortDirection;
    
    public enum SortDirection {
        ASC, DESC
    }
    
    public PageRequest(int page, int size) {
        this(page, size, null, SortDirection.DESC);
    }
    
    public PageRequest(int page, int size, String sortBy, SortDirection sortDirection) {
        if (page < 0) {
            throw new IllegalArgumentException("Page number must be >= 0");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("Page size must be > 0");
        }
        if (size > 1000) {
            throw new IllegalArgumentException("Page size cannot exceed 1000 items");
        }
        
        this.page = page;
        this.size = size;
        this.sortBy = sortBy;
        this.sortDirection = sortDirection != null ? sortDirection : SortDirection.DESC;
    }
    
    public int getPage() { return page; }
    public int getSize() { return size; }
    public String getSortBy() { return sortBy; }
    public SortDirection getSortDirection() { return sortDirection; }
    
    public long getOffset() {
        return (long) page * size;
    }
    
    public boolean hasSorting() {
        return sortBy != null && !sortBy.trim().isEmpty();
    }
    
    @Override
    public String toString() {
        return String.format("PageRequest{page=%d, size=%d, sortBy='%s', direction=%s}", 
                           page, size, sortBy, sortDirection);
    }
}