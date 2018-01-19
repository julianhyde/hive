explain select distinct key from src1;

explain select distinct * from src1;

explain select distinct count(*) from src1 where key in (1,2,3);

explain select distinct count(*) from src1 where key in (1,2,3) group by key;

explain select distinct key, count(*) from src1 where key in (1,2,3) group by key;

explain select distinct * from (select * from src1) as T;

explain select distinct * from (select count(*) from src1) as T;

explain select distinct * from (select * from src1 where key in (1,2,3)) as T;

explain select distinct * from (select count(*) from src1 where key in (1,2,3)) as T;

explain select distinct * from (select distinct count(*) from src1 where key in (1,2,3)) as T;

explain select distinct sum(value) over () from src1;

explain select distinct * from (select sum(value) over () from src1) as T;

explain select distinct count(*)+1 from src1;

explain select distinct count(*)+key from src1 group by key;

explain select distinct count(a.value), count(b.value) from src1 a join src1 b on a.key=b.key;

explain select distinct count(a.value), count(b.value) from src1 a join src1 b on a.key=b.key group by a.key;

-- should not project the virtual BLOCK_OFFSET et all columns
explain select distinct * from (select distinct * from src1) as T;


