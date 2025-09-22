based on the sharding meta data stored in the registry, the max range of a query will automatically be adjusted.

for example, when user makes a findByIdAndPartitionColRange(id, minDate, maxDate):
    - we look at the maxDate value and compare with our stored max range for partitions
    - say we have max range '2025-09-23', but the query max param (let's call maxDate) is '2025-09-29'.
    the maxDate will be reduced to
        - for datetime: ('2025-09-29' + 1) second. remember we always compare the last boundary as (column < maxDate).
        - for numeric types: maxValue+1

