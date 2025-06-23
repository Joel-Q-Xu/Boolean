SELECT *
FROM 
  lineitem, part
WHERE 
  (
    p_partkey = l_partkey
    AND p_brand = 'Brand#13'
    AND p_container in ('SM CASE','SM BOX','SM PACK','SM PKG') 
    AND l_quantity >= 1 and l_quantity <= 1 + 10 
    AND p_size between 1 and 5 
    AND l_shipmode in ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON' 
  )
  OR 
  (
    p_partkey = l_partkey
    AND p_brand = 'Brand#1'
    AND p_container in ('MED BAG','MED BOX','MED PKG','MED PACK')
    AND l_quantity >= 11 and l_quantity <= 11 + 10
    AND p_size between 1 and 10
    AND l_shipmode in ('AIR', 'AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  )
  OR 
  (
    p_partkey = l_partkey
    AND p_brand = 'Brand#2'
    AND p_container in ( 'LG CASE','LG BOX','LG PACK','LG PKG')
    AND l_quantity >= 21 and l_quantity <= 21 + 10
    AND p_size between 1 and 15
    AND l_shipmode in ('AIR','AIR REG')
    AND l_shipinstruct = 'DELIVER IN PERSON'
  );