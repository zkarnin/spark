-- This file is automatically generated by LogicalPlanToSQLSuite.
SELECT COUNT(a.value), b.KEY, a.KEY
<<<<<<< HEAD
FROM parquet_t1 a CROSS JOIN parquet_t1 b
GROUP BY a.KEY, b.KEY
HAVING MAX(a.KEY) > 0
--------------------------------------------------------------------------------
SELECT `gen_attr_0` AS `count(value)`, `gen_attr_1` AS `KEY`, `gen_attr_2` AS `KEY` FROM (SELECT `gen_attr_0`, `gen_attr_1`, `gen_attr_2` FROM (SELECT count(`gen_attr_4`) AS `gen_attr_0`, `gen_attr_1`, `gen_attr_2`, max(`gen_attr_2`) AS `gen_attr_3` FROM (SELECT `key` AS `gen_attr_2`, `value` AS `gen_attr_4` FROM `default`.`parquet_t1`) AS gen_subquery_0 CROSS JOIN (SELECT `key` AS `gen_attr_1`, `value` AS `gen_attr_5` FROM `default`.`parquet_t1`) AS gen_subquery_1 GROUP BY `gen_attr_2`, `gen_attr_1` HAVING (`gen_attr_3` > CAST(0 AS BIGINT))) AS gen_subquery_2) AS gen_subquery_3
=======
FROM parquet_t1 a, parquet_t1 b
GROUP BY a.KEY, b.KEY
HAVING MAX(a.KEY) > 0
--------------------------------------------------------------------------------
SELECT `gen_attr_0` AS `count(value)`, `gen_attr_1` AS `KEY`, `gen_attr_2` AS `KEY` FROM (SELECT `gen_attr_0`, `gen_attr_1`, `gen_attr_2` FROM (SELECT count(`gen_attr_4`) AS `gen_attr_0`, `gen_attr_1`, `gen_attr_2`, max(`gen_attr_2`) AS `gen_attr_3` FROM (SELECT `key` AS `gen_attr_2`, `value` AS `gen_attr_4` FROM `default`.`parquet_t1`) AS gen_subquery_0 INNER JOIN (SELECT `key` AS `gen_attr_1`, `value` AS `gen_attr_5` FROM `default`.`parquet_t1`) AS gen_subquery_1 GROUP BY `gen_attr_2`, `gen_attr_1` HAVING (`gen_attr_3` > CAST(0 AS BIGINT))) AS gen_subquery_2) AS gen_subquery_3
>>>>>>> tuning_adaptive
