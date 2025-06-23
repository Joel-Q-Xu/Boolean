SELECT *
    partsupp,
    part
WHERE
    p_partkey = ps_partkey
    AND p_brand <> 'Brand#13'  
    AND p_type not like 'TYPEPROMO BURNISHED COPPER'
    AND p_size in (1,2,3,4,5,6,7,8)
    AND ps_suppkey not in (358);