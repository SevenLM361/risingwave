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
CREATE MATERIALIZED VIEW m0 AS SELECT '06jQ5OqeK7' AS col_0, ((coalesce(NULL, NULL, NULL, NULL, t_0.ps_availqty, NULL, NULL, NULL, NULL, NULL)) >> t_0.ps_availqty) AS col_1 FROM partsupp AS t_0 WHERE true GROUP BY t_0.ps_partkey, t_0.ps_comment, t_0.ps_availqty;
CREATE MATERIALIZED VIEW m2 AS SELECT t_1.p_retailprice AS col_0 FROM nation AS t_0 RIGHT JOIN part AS t_1 ON t_0.n_name = t_1.p_comment AND true WHERE true GROUP BY t_1.p_retailprice HAVING true;
CREATE MATERIALIZED VIEW m3 AS SELECT (REAL '0') AS col_0 FROM customer AS t_0 FULL JOIN partsupp AS t_1 ON t_0.c_custkey = t_1.ps_availqty AND (CASE WHEN true THEN (true) ELSE ((INTERVAL '-604800') >= (((259) * (INTERVAL '60')) * t_0.c_nationkey)) END) WHERE (true) GROUP BY t_1.ps_supplycost;
CREATE MATERIALIZED VIEW m4 AS SELECT t_2.p_comment AS col_0, t_2.p_size AS col_1 FROM part AS t_2 GROUP BY t_2.p_comment, t_2.p_size HAVING false;
CREATE MATERIALIZED VIEW m5 AS SELECT tumble_0.category AS col_0, tumble_0.category AS col_1, (BIGINT '770') AS col_2, tumble_0.item_name AS col_3 FROM tumble(auction, auction.date_time, INTERVAL '29') AS tumble_0 GROUP BY tumble_0.category, tumble_0.item_name;
CREATE MATERIALIZED VIEW m6 AS SELECT t_0.ps_suppkey AS col_0 FROM partsupp AS t_0 WHERE true GROUP BY t_0.ps_partkey, t_0.ps_suppkey HAVING false;
CREATE MATERIALIZED VIEW m7 AS SELECT (BIGINT '0') AS col_0, t_2.col_2 AS col_1, t_2.col_2 AS col_2, (t_2.col_2 & (INT '825')) AS col_3 FROM m5 AS t_2 GROUP BY t_2.col_2 HAVING true;
CREATE MATERIALIZED VIEW m8 AS WITH with_0 AS (SELECT (279) AS col_0, (SMALLINT '0') AS col_1, (t_3.col_0 - ((INT '2147483647') >> (coalesce(NULL, ((INT '2147483647')), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)))) AS col_2 FROM m2 AS t_3 GROUP BY t_3.col_0 HAVING true) SELECT (FLOAT '20') AS col_0 FROM with_0 WHERE true;
CREATE MATERIALIZED VIEW m9 AS SELECT (BIGINT '318') AS col_0 FROM bid AS t_0 JOIN m5 AS t_1 ON t_0.bidder = t_1.col_2 GROUP BY t_1.col_0, t_0.price, t_0.bidder, t_0.url;
