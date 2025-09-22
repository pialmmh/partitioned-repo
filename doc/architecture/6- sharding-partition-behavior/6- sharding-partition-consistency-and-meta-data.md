
the repository will have the same max and min range for 2nd level partitions, across shards.  
    - for multi-table:  min table range, max table range, same across all shards 
    - for native parition:  min partition range, max partition range, same across all shards.


On start and after maintenance, repository will refresh the meta data:
    - verify max and min partition (or multi-tables) ranges are same across shard
    - if not same, throw exception and kill the repo by writing logs.  
    - if same, we know the limit of min and max range for 2nd level partitions. 
    - save meta data for  min/max partition range limit

