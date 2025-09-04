package com.telcobright.core.pagination;

import java.util.List;

/**
 * Pagination result container
 */
public class Page<T> {
    private final List<T> content;
    private final int pageNumber;
    private final int pageSize;
    private final long totalElements;
    private final int totalPages;
    private final boolean first;
    private final boolean last;
    private final boolean hasNext;
    private final boolean hasPrevious;
    
    public Page(List<T> content, PageRequest pageRequest, long totalElements) {
        this.content = content;
        this.pageNumber = pageRequest.getPage();
        this.pageSize = pageRequest.getSize();
        this.totalElements = totalElements;
        this.totalPages = (int) Math.ceil((double) totalElements / pageSize);
        this.first = pageNumber == 0;
        this.last = pageNumber >= totalPages - 1;
        this.hasNext = pageNumber < totalPages - 1;
        this.hasPrevious = pageNumber > 0;
    }
    
    public List<T> getContent() { return content; }
    public int getPageNumber() { return pageNumber; }
    public int getPageSize() { return pageSize; }
    public long getTotalElements() { return totalElements; }
    public int getTotalPages() { return totalPages; }
    public boolean isFirst() { return first; }
    public boolean isLast() { return last; }
    public boolean hasNext() { return hasNext; }
    public boolean hasPrevious() { return hasPrevious; }
    
    public int getNumberOfElements() {
        return content.size();
    }
    
    public boolean isEmpty() {
        return content.isEmpty();
    }
    
    @Override
    public String toString() {
        return String.format("Page{number=%d, size=%d, totalElements=%d, totalPages=%d, content=%d items}", 
                           pageNumber, pageSize, totalElements, totalPages, content.size());
    }
}