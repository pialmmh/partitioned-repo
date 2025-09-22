when range partition is used at 2nd level:
case rows < min range: 
    - native partition mode: rows < min range gets stored automatically by mysql default
    - multi-table mode: rows < min range gets stored in the lowest table by range
case rows >= max range:
- native partition mode: mysql throws exception for rows >= max range.
- multi-table mode: we must throws exception for rows >= max range.
