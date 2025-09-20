# ApiValidationTest

## TEST GOALS
- Validate the SplitVerseRepository builder API
- Ensure proper validation of configuration parameters
- Test error handling for invalid configurations
- Verify fluent API contract and builder patterns

## COVERAGE
- Repository builder validation logic
- Configuration parameter constraints
- Error messages and exception types
- Fluent API method chaining
- Default value behavior

## DESCRIPTION
This test suite validates the API contract of SplitVerseRepository without requiring database connectivity. It ensures that the builder properly validates all configuration parameters and provides clear error messages for invalid configurations. These tests run quickly and don't require MySQL setup.

## TEST METHODS
1. **testRepositoryModeEnumValues** - Validates RepositoryMode enum values
2. **testShardingStrategyEnumValues** - Validates ShardingStrategy enum values
3. **testPartitionColumnTypeEnumValues** - Validates PartitionColumnType enum values
4. **testPartitionRangeEnumValues** - Validates PartitionRange enum values
5. **testBuilderWithMinimalConfiguration** - Tests minimal valid configuration
6. **testBuilderMethodChaining** - Validates fluent API method chaining