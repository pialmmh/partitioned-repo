# BasicFunctionalityTest

## TEST GOALS
- Validate basic CRUD operations
- Test single-shard functionality
- Verify entity metadata extraction
- Ensure proper SQL generation

## COVERAGE
- Entity annotation processing
- Basic insert/update/delete operations
- Find by ID functionality
- Repository initialization
- Connection management

## DESCRIPTION
This test suite validates the core functionality of Split-Verse with a single shard configuration. It ensures that basic CRUD operations work correctly and that the repository properly handles entity metadata extraction and SQL generation.

## TEST METHODS
1. **testInsertOperation** - Tests basic entity insertion
2. **testFindByIdOperation** - Tests entity retrieval by ID
3. **testUpdateOperation** - Tests entity update functionality
4. **testDeleteOperation** - Tests entity deletion
5. **testBulkInsertOperation** - Tests bulk insert functionality
6. **testFindAllOperation** - Tests retrieving all entities