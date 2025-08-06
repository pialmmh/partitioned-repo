package com.telcobright.db.example;

import com.telcobright.db.annotation.*;
import com.telcobright.db.entity.ShardingEntity;
import com.telcobright.db.metadata.EntityMetadata;
import com.telcobright.db.repository.GenericMultiTableRepository;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Demonstrates @Index annotation usage for automatic index creation.
 * Shows how to create various types of database indexes on entity fields.
 */
public class IndexAnnotationDemo {

    /**
     * User entity demonstrating various index types
     */
    @Table(name = "users")
    public static class UserEntity implements ShardingEntity<Long> {
        @Id
        @Column(name = "user_id", insertable = false)
        private Long userId;
        
        @ShardingKey
        @Column(name = "created_at", nullable = false)
        private LocalDateTime createdAt;
        
        // Simple index with auto-generated name: idx_email
        @Index
        @Column(name = "email", nullable = false)
        private String email;
        
        // Unique index for usernames
        @Index(unique = true)
        @Column(name = "username", nullable = false)
        private String username;
        
        // Custom named index with comment
        @Index(name = "user_status_idx", comment = "Index for filtering by user status")
        @Column(name = "status", nullable = false)
        private String status; // ACTIVE, INACTIVE, SUSPENDED
        
        // Index for frequently searched fields
        @Index
        @Column(name = "department_id")
        private Long departmentId;
        
        @Index
        @Column(name = "role", nullable = false)
        private String role; // ADMIN, USER, MANAGER
        
        // Regular field without index
        @Column(name = "full_name")
        private String fullName;
        
        @Column(name = "phone_number")
        private String phoneNumber;
        
        // Constructors
        public UserEntity() {}
        
        public UserEntity(String email, String username, String status, Long departmentId, String role, String fullName) {
            this.email = email;
            this.username = username;
            this.status = status;
            this.departmentId = departmentId;
            this.role = role;
            this.fullName = fullName;
            this.createdAt = LocalDateTime.now();
        }
        
        // Getters and setters
        public Long getUserId() { return userId; }
        public void setUserId(Long userId) { this.userId = userId; }
        
        public LocalDateTime getCreatedAt() { return createdAt; }
        public void setCreatedAt(LocalDateTime createdAt) { this.createdAt = createdAt; }
        
        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }
        
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        
        public Long getDepartmentId() { return departmentId; }
        public void setDepartmentId(Long departmentId) { this.departmentId = departmentId; }
        
        public String getRole() { return role; }
        public void setRole(String role) { this.role = role; }
        
        public String getFullName() { return fullName; }
        public void setFullName(String fullName) { this.fullName = fullName; }
        
        @Override
        public String toString() {
            return String.format("UserEntity{userId=%d, username='%s', email='%s', status='%s', role='%s'}", 
                               userId, username, email, status, role);
        }
    }

    /**
     * Product entity with different index patterns
     */
    @Table(name = "products")
    public static class ProductEntity implements ShardingEntity<String> {
        @Id
        @Column(name = "product_code", insertable = false)
        private String productCode;
        
        @ShardingKey
        @Column(name = "launch_date", nullable = false)
        private LocalDateTime launchDate;
        
        // Unique index for SKU codes
        @Index(unique = true, comment = "Unique SKU identifier")
        @Column(name = "sku", nullable = false)
        private String sku;
        
        // Index for category-based searches
        @Index(name = "product_category_idx")
        @Column(name = "category_id", nullable = false)
        private Long categoryId;
        
        // Index for price range queries
        @Index
        @Column(name = "price", nullable = false)
        private Double price;
        
        // Index for inventory management
        @Index(comment = "Stock level monitoring")
        @Column(name = "stock_quantity")
        private Integer stockQuantity;
        
        // Index for supplier lookups
        @Index
        @Column(name = "supplier_id")
        private Long supplierId;
        
        @Column(name = "product_name", nullable = false)
        private String productName;
        
        @Column(name = "description")
        private String description;
        
        // Constructors
        public ProductEntity() {}
        
        public ProductEntity(String productCode, String sku, Long categoryId, Double price, 
                           Integer stockQuantity, Long supplierId, String productName) {
            this.productCode = productCode;
            this.sku = sku;
            this.categoryId = categoryId;
            this.price = price;
            this.stockQuantity = stockQuantity;
            this.supplierId = supplierId;
            this.productName = productName;
            this.launchDate = LocalDateTime.now();
        }
        
        // Getters and setters
        public String getProductCode() { return productCode; }
        public void setProductCode(String productCode) { this.productCode = productCode; }
        
        public LocalDateTime getLaunchDate() { return launchDate; }
        public void setLaunchDate(LocalDateTime launchDate) { this.launchDate = launchDate; }
        
        public String getSku() { return sku; }
        public void setSku(String sku) { this.sku = sku; }
        
        public Long getCategoryId() { return categoryId; }
        public void setCategoryId(Long categoryId) { this.categoryId = categoryId; }
        
        public Double getPrice() { return price; }
        public void setPrice(Double price) { this.price = price; }
        
        public Integer getStockQuantity() { return stockQuantity; }
        public void setStockQuantity(Integer stockQuantity) { this.stockQuantity = stockQuantity; }
        
        public Long getSupplierId() { return supplierId; }
        public void setSupplierId(Long supplierId) { this.supplierId = supplierId; }
        
        public String getProductName() { return productName; }
        public void setProductName(String productName) { this.productName = productName; }
        
        public String getDescription() { return description; }
        public void setDescription(String description) { this.description = description; }
        
        @Override
        public String toString() {
            return String.format("ProductEntity{productCode='%s', sku='%s', productName='%s', price=%.2f}", 
                               productCode, sku, productName, price);
        }
    }

    public static void main(String[] args) {
        System.out.println("=== @Index Annotation Demo ===");
        System.out.println("Demonstrating automatic index creation from @Index annotations");
        System.out.println();

        demonstrateIndexCreation();
        
        System.out.println("✅ All entities with @Index annotations processed successfully!");
        System.out.println("Custom indexes will be automatically created when tables are generated.");
    }
    
    private static void demonstrateIndexCreation() {
        System.out.println("1. UserEntity Index Analysis:");
        analyzeEntityIndexes(UserEntity.class);
        
        System.out.println("\n2. ProductEntity Index Analysis:");
        analyzeEntityIndexes(ProductEntity.class);
        
        System.out.println("\n3. Generated CREATE TABLE Examples:");
        showCreateTableSQL();
    }
    
    private static void analyzeEntityIndexes(Class<?> entityClass) {
        System.out.printf("   Entity: %s%n", entityClass.getSimpleName());
        
        try {
            // Create EntityMetadata to analyze indexes
            EntityMetadata<?, ?> metadata;
            if (entityClass == UserEntity.class) {
                metadata = new EntityMetadata<>(UserEntity.class, Long.class);
            } else {
                metadata = new EntityMetadata<>(ProductEntity.class, String.class);
            }
            
            System.out.printf("   Table: %s%n", metadata.getTableName());
            System.out.println("   Detected indexes:");
            
            // Analyze each field for indexes
            for (var field : metadata.getFields()) {
                if (field.hasIndex()) {
                    var indexAnnotation = field.getIndexAnnotation();
                    String indexType = indexAnnotation.unique() ? "UNIQUE" : "REGULAR";
                    String indexName = field.getIndexName();
                    String comment = indexAnnotation.comment();
                    
                    System.out.printf("      - Field: %s -> Index: %s (%s)%s%n", 
                                    field.getColumnName(), 
                                    indexName, 
                                    indexType,
                                    comment.isEmpty() ? "" : " - " + comment);
                }
            }
            
        } catch (Exception e) {
            System.out.printf("   Error analyzing indexes: %s%n", e.getMessage());
        }
    }
    
    private static void showCreateTableSQL() {
        System.out.println("UserEntity CREATE TABLE SQL:");
        try {
            EntityMetadata<UserEntity, Long> userMetadata = new EntityMetadata<>(UserEntity.class, Long.class);
            String createSQL = userMetadata.getCreateTableSQL();
            System.out.println("   " + createSQL.replace("%s", userMetadata.getTableName()));
        } catch (Exception e) {
            System.out.printf("   Error generating SQL: %s%n", e.getMessage());
        }
        
        System.out.println("\nProductEntity CREATE TABLE SQL:");
        try {
            EntityMetadata<ProductEntity, String> productMetadata = new EntityMetadata<>(ProductEntity.class, String.class);
            String createSQL = productMetadata.getCreateTableSQL();
            System.out.println("   " + createSQL.replace("%s", productMetadata.getTableName()));
        } catch (Exception e) {
            System.out.printf("   Error generating SQL: %s%n", e.getMessage());
        }
    }
}