WITH l AS (SELECT * FROM read_parquet('/Users/konstantinas.m/projects/venn/bench/data/1m-d01/left.parquet')),
     r AS (SELECT * FROM read_parquet('/Users/konstantinas.m/projects/venn/bench/data/1m-d01/right.parquet')),
     j AS (SELECT l.id lid, r.id rid, l.b1 l_b1, r.b1 r_b1, l.b2 l_b2, r.b2 r_b2, l.d1 l_d1, r.d1 r_d1, l.d2 l_d2, r.d2 r_d2, l.f1 l_f1, r.f1 r_f1, l.f2 l_f2, r.f2 r_f2, l.f3 l_f3, r.f3 r_f3, l.i1 l_i1, r.i1 r_i1, l.i2 l_i2, r.i2 r_i2, l.i3 l_i3, r.i3 r_i3, l.s1 l_s1, r.s1 r_s1, l.s2 l_s2, r.s2 r_s2, l.t1 l_t1, r.t1 r_t1, l.t2 l_t2, r.t2 r_t2
           FROM l FULL OUTER JOIN r ON l.id = r.id)
SELECT count(*) FILTER (lid IS NULL)  AS added,
       count(*) FILTER (rid IS NULL)  AS removed,
       count(*) FILTER (lid IS NOT NULL AND rid IS NOT NULL AND (l_b1 IS DISTINCT FROM r_b1 OR l_b2 IS DISTINCT FROM r_b2 OR l_d1 IS DISTINCT FROM r_d1 OR l_d2 IS DISTINCT FROM r_d2 OR l_f1 IS DISTINCT FROM r_f1 OR l_f2 IS DISTINCT FROM r_f2 OR l_f3 IS DISTINCT FROM r_f3 OR l_i1 IS DISTINCT FROM r_i1 OR l_i2 IS DISTINCT FROM r_i2 OR l_i3 IS DISTINCT FROM r_i3 OR l_s1 IS DISTINCT FROM r_s1 OR l_s2 IS DISTINCT FROM r_s2 OR l_t1 IS DISTINCT FROM r_t1 OR l_t2 IS DISTINCT FROM r_t2)) AS changed
FROM j;
