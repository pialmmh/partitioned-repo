# PartitionBoundaryTest

## TEST GOALS
Verify MySQL-style partition boundary behavior:
- Values below minimum range go into the lowest table
- Values above maximum range throw an exception
- Normal range values work as expected

## COVERAGE
- Boundary validation in insert operations
- Below-minimum value handling
- Above-maximum value rejection
- Edge cases around partition boundaries

## DESCRIPTION
This test validates that Split-Verse implements MySQL-style partition boundary handling correctly. When data falls outside the configured retention period, it should behave like MySQL native partitioning: old data goes to the minimum partition (MINVALUE behavior), while future data beyond the maximum is rejected (MAXVALUE behavior).

## TEST METHODS
1. **testNormalRangeInsert** - Tests insert within normal partition range
2. **testBelowMinimumRangeInsert** - Tests insert below minimum partition range (should succeed)
3. **testAboveMaximumRangeInsert** - Tests insert above maximum partition range (should fail)
4. **testEdgeCases** - Tests edge cases at partition boundaries
5. **testMultipleOldEvents** - Tests multiple below-minimum events