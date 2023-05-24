# hive

## 1.udf
继承udf类，实现evaluator方法

## 2.udaf
https://blog.csdn.net/lidongmeng0213/article/details/110869457

objectinspector <br>
https://blog.csdn.net/weixin_42167895/article/details/108314139

## 3.udtf


## 4.hive拉链表
-- 创建初始化全量表
CREATE EXTERNAL TABLE `ods`.`user` (
user_num STRING COMMENT '用户编号',
mobile STRING COMMENT '手机号码',
reg_date STRING COMMENT '注册日期'
)
COMMENT '用户资料表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/ods/user';

-- 创建每日更新表
CREATE EXTERNAL TABLE `ods`.`user_update` (
user_num STRING COMMENT '用户编号',
mobile STRING COMMENT '手机号码',
reg_date STRING COMMENT '注册日期'
)
COMMENT '每日用户资料更新表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/ods/user_update';

-- 创建拉链表
CREATE EXTERNAL TABLE `dwd`.`user_zip`(
user_num STRING COMMENT '用户编号'
,mobile STRING COMMENT '手机号码'
,reg_date STRING COMMENT '注册日期'
,t_start_date STRING
,t_end_date STRING
) COMMENT '用户资料拉链表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/dwd/user_zip';

-- 测试数据
INSERT INTO TABLE `ods`.`user` PARTITION(dt='20230518')
SELECT '001','18888','20230518';

INSERT INTO TABLE `ods`.`user` PARTITION(dt='20230518')
SELECT '002','18887','20230518';

INSERT INTO TABLE `ods`.`user_update` PARTITION(dt='20230519')
SELECT '001','18887','20230519';

INSERT OVERWRITE TABLE `ods`.`user_update` PARTITION(dt='20230519')
SELECT p1.user_num
,p1.mobile
,p1.reg_date
FROM `ods`.`user_update` AS  p1
WHERE dt='20230519' and user_num is not null

初始化拉链表
INSERT INTO TABLE `dwd`.`user_zip`
SELECT p1.user_num
,p1.mobile
,p1.reg_date
,'2023-05-18' AS t_start_date
,'9999-12-31' AS t_end_date
FROM `ods`.`user` AS p1






-- 脚本 <br>
INSERT INTO TABLE `dwd`.`user_zip`
SELECT *
FROM (
SELECT p1.user_num
,p1.mobile
,p1.reg_date
,p1.t_start_date
,CASE
WHEN p1.t_end_date = "9999-12-31" AND p2.user_num THEN "2023-05-19"
ELSE p2.t_end_date
END AS t_end_data
FROM `dwd`.`user_zip` AS p1
LEFT JOIN `ods`.`user_update` AS p2
ON p1.user_num = p2.user_num
UNION
SELECT p3.user_num
,p3.mobile
,p3.reg_date
,'2023-05-19' AS t_start_date
,'9999-12-31' AS t_end_date
FROM `ods`.`user_update` AS p3
WHERE p3.dt='20230519'
) AS tmp


## 5.ORC与parquet的区别
![img_1.png](img_1.png)
-- parquet
parquet文件是以二进制方式存储的，不可以直接读取和修改。Parquet文件是自解析
