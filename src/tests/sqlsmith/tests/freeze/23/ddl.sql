CREATE TABLE supplier (s_suppkey INT, s_name CHARACTER VARYING, s_address CHARACTER VARYING, s_nationkey INT, s_phone CHARACTER VARYING, s_acctbal NUMERIC, s_comment CHARACTER VARYING, PRIMARY KEY (s_suppkey));
CREATE TABLE part (p_partkey INT, p_name CHARACTER VARYING, p_mfgr CHARACTER VARYING, p_brand CHARACTER VARYING, p_type CHARACTER VARYING, p_size INT, p_container CHARACTER VARYING, p_retailprice NUMERIC, p_comment CHARACTER VARYING, PRIMARY KEY (p_partkey));
CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost NUMERIC, ps_comment CHARACTER VARYING, PRIMARY KEY (ps_partkey, ps_suppkey));
CREATE TABLE customer (c_custkey INT, c_name CHARACTER VARYING, c_address CHARACTER VARYING, c_nationkey INT, c_phone CHARACTER VARYING, c_acctbal NUMERIC, c_mktsegment CHARACTER VARYING, c_comment CHARACTER VARYING, PRIMARY KEY (c_custkey));
CREATE TABLE orders (o_orderkey BIGINT, o_custkey INT, o_orderstatus CHARACTER VARYING, o_totalprice NUMERIC, o_orderdate DATE, o_orderpriority CHARACTER VARYING, o_clerk CHARACTER VARYING, o_shippriority INT, o_comment CHARACTER VARYING, PRIMARY KEY (o_orderkey));
CREATE TABLE lineitem (l_orderkey BIGINT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity NUMERIC, l_extendedprice NUMERIC, l_discount NUMERIC, l_tax NUMERIC, l_returnflag CHARACTER VARYING, l_linestatus CHARACTER VARYING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, l_shipinstruct CHARACTER VARYING, l_shipmode CHARACTER VARYING, l_comment CHARACTER VARYING, PRIMARY KEY (l_orderkey, l_linenumber));
CREATE TABLE nation (n_nationkey INT, n_name CHARACTER VARYING, n_regionkey INT, n_comment CHARACTER VARYING, PRIMARY KEY (n_nationkey));
CREATE TABLE region (r_regionkey INT, r_name CHARACTER VARYING, r_comment CHARACTER VARYING, PRIMARY KEY (r_regionkey));
CREATE TABLE person (id BIGINT, name CHARACTER VARYING, email_address CHARACTER VARYING, credit_card CHARACTER VARYING, city CHARACTER VARYING, state CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE auction (id BIGINT, item_name CHARACTER VARYING, description CHARACTER VARYING, initial_bid BIGINT, reserve BIGINT, date_time TIMESTAMP, expires TIMESTAMP, seller BIGINT, category BIGINT, extra CHARACTER VARYING, PRIMARY KEY (id));
CREATE TABLE bid (auction BIGINT, bidder BIGINT, price BIGINT, channel CHARACTER VARYING, url CHARACTER VARYING, date_time TIMESTAMP, extra CHARACTER VARYING);
CREATE TABLE alltypes1 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE TABLE alltypes2 (c1 BOOLEAN, c2 SMALLINT, c3 INT, c4 BIGINT, c5 REAL, c6 DOUBLE, c7 NUMERIC, c8 DATE, c9 CHARACTER VARYING, c10 TIME, c11 TIMESTAMP, c13 INTERVAL, c14 STRUCT<a INT>, c15 INT[], c16 CHARACTER VARYING[]);
CREATE MATERIALIZED VIEW m0 AS SELECT tumble_0.email_address AS col_0 FROM tumble(person, person.date_time, INTERVAL '93') AS tumble_0 GROUP BY tumble_0.email_address HAVING false;
CREATE MATERIALIZED VIEW m1 AS SELECT t_1.ps_availqty AS col_0, (t_1.ps_availqty / t_0.c3) AS col_1, '08Bj7IYeYf' AS col_2 FROM alltypes1 AS t_0 JOIN partsupp AS t_1 ON t_0.c3 = t_1.ps_suppkey GROUP BY t_0.c2, t_0.c15, t_1.ps_availqty, t_0.c16, t_1.ps_supplycost, t_0.c3, t_0.c5, t_0.c9 HAVING true;
CREATE MATERIALIZED VIEW m2 AS WITH with_0 AS (WITH with_1 AS (SELECT 'Uo5ttZFIyP' AS col_0, CAST(t_3.ps_partkey AS BOOLEAN) AS col_1, t_3.ps_suppkey AS col_2 FROM nation AS t_2 FULL JOIN partsupp AS t_3 ON t_2.n_nationkey = t_3.ps_suppkey WHERE true GROUP BY t_3.ps_partkey, t_3.ps_suppkey) SELECT (~ ((BIGINT '592'))) AS col_0 FROM with_1) SELECT (INT '952') AS col_0, '9dDyv84Zrl' AS col_1, false AS col_2 FROM with_0 WHERE false;
CREATE MATERIALIZED VIEW m3 AS SELECT 'mcGoExMWj6' AS col_0, t_0.item_name AS col_1, (CASE WHEN true THEN t_0.expires ELSE t_0.expires END) AS col_2 FROM auction AS t_0 WHERE ((-1704660683) <> (FLOAT '10')) GROUP BY t_0.item_name, t_0.description, t_0.expires;
CREATE MATERIALIZED VIEW m4 AS SELECT (REAL '163') AS col_0, sq_1.col_1 AS col_1, (168) AS col_2, (REAL '815') AS col_3 FROM (SELECT tumble_0.c13 AS col_0, (tumble_0.c5 - (REAL '-2147483648')) AS col_1, true AS col_2, tumble_0.c5 AS col_3 FROM tumble(alltypes2, alltypes2.c11, INTERVAL '32') AS tumble_0 GROUP BY tumble_0.c6, tumble_0.c8, tumble_0.c13, tumble_0.c7, tumble_0.c5, tumble_0.c2) AS sq_1 WHERE false GROUP BY sq_1.col_1;
CREATE MATERIALIZED VIEW m5 AS WITH with_0 AS (WITH with_1 AS (WITH with_2 AS (SELECT 'qkXkL6cAn2' AS col_0, TIME '05:08:19' AS col_1 FROM bid AS t_3 FULL JOIN part AS t_4 ON t_3.channel = t_4.p_type AND true WHERE false GROUP BY t_4.p_brand, t_4.p_comment, t_3.channel, t_4.p_type) SELECT TIMESTAMP '2022-07-19 03:17:11' AS col_0, CAST(NULL AS STRUCT<a REAL, b INTERVAL, c BOOLEAN>) AS col_1, TIME '05:08:19' AS col_2 FROM with_2 WHERE true) SELECT ((SMALLINT '597') / (SMALLINT '556')) AS col_0 FROM with_1) SELECT (REAL '701') AS col_0 FROM with_0;
CREATE MATERIALIZED VIEW m6 AS WITH with_0 AS (SELECT t_1.l_tax AS col_0 FROM lineitem AS t_1 JOIN orders AS t_2 ON t_1.l_partkey = t_2.o_shippriority GROUP BY t_2.o_shippriority, t_1.l_orderkey, t_1.l_quantity, t_1.l_returnflag, t_1.l_shipdate, t_1.l_partkey, t_1.l_tax, t_1.l_discount, t_2.o_totalprice, t_2.o_custkey, t_1.l_receiptdate, t_2.o_orderdate, t_2.o_orderpriority) SELECT (INTERVAL '3600') AS col_0 FROM with_0 WHERE (coalesce((CASE WHEN (coalesce(NULL, NULL, NULL, NULL, NULL, NULL, false, NULL, NULL, NULL)) THEN (((SMALLINT '691') # (SMALLINT '1')) = (BIGINT '9223372036854775807')) ELSE true END), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL));
CREATE MATERIALIZED VIEW m7 AS SELECT (REAL '0') AS col_0 FROM (SELECT t_0.col_0 AS col_0, (CASE WHEN false THEN (t_0.col_0 + t_0.col_0) ELSE (- (REAL '165')) END) AS col_1 FROM m4 AS t_0 GROUP BY t_0.col_0) AS sq_1 GROUP BY sq_1.col_0 HAVING false;
CREATE MATERIALIZED VIEW m8 AS SELECT (false) AS col_0 FROM part AS t_0 GROUP BY t_0.p_brand, t_0.p_mfgr, t_0.p_partkey, t_0.p_container;
CREATE MATERIALIZED VIEW m9 AS SELECT ((SMALLINT '344') + (627)) AS col_0, t_0.col_0 AS col_1 FROM m5 AS t_0 GROUP BY t_0.col_0 HAVING false;
