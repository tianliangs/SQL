-- #1-获取用户停留表
CREATE TABLE ljx_stay_month_202311 AS 
SELECT uid, stime, date, poi_id, city, province
FROM stay_month
WHERE province = '011' 
  AND city = 'V0110000'
  AND LEFT(date, 6) = '202311';

-- 获取经纬度
CREATE TABLE ljx_stay_month_poi_202311 AS 
SELECT a.uid, a.stime, b.weighted_centroid_lon, b.weighted_centroid_lat   
FROM ljx_stay_month_202311 a   
JOIN stay_poi b ON a.uid = b.uid   
               AND a.poi_id = b.poi_id 
               AND b.province = '011'   
               AND b.city = 'V0110000';

-- #2-将经纬度进行分箱
CREATE TABLE ljx_202311_bins AS 
SELECT uid,
       CONCAT(SUBSTRING(stime, 1, 15), '0:00') AS stime,
       CAST(((AVG(weighted_centroid_lon) - 114.5) / 0.005874204851473869) + 0.5 AS INT) AS loncol,
       CAST(((AVG(weighted_centroid_lat) - 39) / 0.004496605206422906) + 0.5 AS INT) AS latcol
FROM ljx_stay_month_poi_202311
GROUP BY uid, CONCAT(SUBSTRING(stime, 1, 15), '0:00');

-- #3-活动识别
-- 分箱后状态变化检测
CREATE TABLE ljx_bins202311_1 AS
SELECT uid,
       stime,
       loncol,
       latcol,
       SIGN(ABS(CAST(CONCAT(loncol, latcol) AS FLOAT) - LAG(CAST(CONCAT(loncol, latcol) AS FLOAT), 1) OVER (PARTITION BY uid ORDER BY stime))) AS statuschange
FROM ljx_202311_bins;

-- 分箱后状态排序
CREATE TABLE ljx_bins202311_2 AS
SELECT uid,
       stime,
       LEAD(stime, 1) OVER (PARTITION BY uid ORDER BY stime) AS etime,
       loncol,
       latcol,
       SUM(statuschange) OVER (PARTITION BY uid ORDER BY stime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS statusrank
FROM ljx_bins202311_1;

-- 合并活动链
CREATE TABLE ljx_bins202311_3 AS
SELECT uid,
       MIN(stime) AS stime,
       MAX(etime) AS etime,
       COUNT(stime) AS flag,
       loncol,
       latcol,
       statusrank
FROM ljx_bins202311_2
GROUP BY uid, loncol, latcol, statusrank;

-- 提取活动
CREATE TABLE ljx_202311_activity_all AS
SELECT uid,
       stime,
       etime,
       loncol,
       latcol,
       EXTRACT(EPOCH FROM (etime::timestamp - stime::timestamp)) AS duration,
       ROW_NUMBER() OVER (PARTITION BY uid ORDER BY stime) AS rank
FROM ljx_bins202311_3
WHERE EXTRACT(EPOCH FROM (etime::timestamp - stime::timestamp)) >= 1800;

-- 计算日夜停留时间
CREATE TABLE ljx_homeid_202311_1 AS 
SELECT uid, 
       loncol, 
       latcol, 
       SUM(GREATEST(GREATEST(LEAST(EXTRACT(EPOCH FROM etime::timestamp), EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 20:00:00'))::timestamp)), 
                  EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 08:00:00'))::timestamp)) - 
                  LEAST(GREATEST(EXTRACT(EPOCH FROM stime::timestamp), EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 08:00:00'))::timestamp)), 
                  EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 20:00:00'))::timestamp)) + 
             GREATEST(LEAST(EXTRACT(EPOCH FROM etime::timestamp), 
                            EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 20:00:00')::timestamp) + INTERVAL '1 day')), 
                            EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 08:00:00')::timestamp) + INTERVAL '1 day')) - 
             LEAST(GREATEST(EXTRACT(EPOCH FROM stime::timestamp), 
                            EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 08:00:00')::timestamp) + INTERVAL '1 day')), 
                  EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 20:00:00')::timestamp) + INTERVAL '1 day')), 
             0)) AS duration_day,
       SUM(duration - GREATEST(GREATEST(LEAST(EXTRACT(EPOCH FROM etime::timestamp), EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 20:00:00'))::timestamp)), 
                            EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 08:00:00'))::timestamp)) - 
                       LEAST(GREATEST(EXTRACT(EPOCH FROM stime::timestamp), EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 08:00:00'))::timestamp)), 
                            EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 20:00:00'))::timestamp)) + 
                       GREATEST(LEAST(EXTRACT(EPOCH FROM etime::timestamp), 
                            EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 20:00:00')::timestamp) + INTERVAL '1 day')), 
                            EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 08:00:00')::timestamp) + INTERVAL '1 day')) - 
                       LEAST(GREATEST(EXTRACT(EPOCH FROM stime::timestamp), 
                            EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 08:00:00')::timestamp) + INTERVAL '1 day')), 
                            EXTRACT(EPOCH FROM (CONCAT(SUBSTRING(stime, 1, 10), ' 20:00:00')::timestamp) + INTERVAL '1 day')), 
                  0)) AS duration_night 
FROM ljx_202311_activity_all 
GROUP BY uid, loncol, latcol;

-- 1. Creating table ljx_homeid_202311_2
CREATE TABLE ljx_homeid_202311_2 AS 
SELECT  
    uid,
    loncol,
    latcol,
    duration_day,
    duration_night,
    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY duration_day DESC) AS rank_day,
    ROW_NUMBER() OVER (PARTITION BY uid ORDER BY duration_night DESC) AS rank_night
FROM ljx_homeid_202311_1;

-- 2. Counting the number of grids each user appears in
CREATE TABLE ljx_user_showgrids_202311 AS
SELECT
    uid,
    COUNT(DISTINCT CONCAT(loncol, '_', latcol)) AS grids_count
FROM
    ljx_202311_activity_all
GROUP BY
    uid;

-- 3. Counting the number of days each user appears
CREATE TABLE ljx_user_showdays_202311 AS
SELECT
    uid,
    COUNT(DISTINCT DATE(stime)) AS days_count
FROM
    ljx_202311_activity_all
GROUP BY
    uid;

-- 4. Top five residential areas
CREATE TABLE ljx_user_home_attribute_202311 AS
SELECT DISTINCT
  uid,
  loncol,
  latcol,
  'H' AS tag_type,
  CASE rank_night
    WHEN 1 THEN 'H_0'
    WHEN 2 THEN 'H_1'
    WHEN 3 THEN 'H_2'
    WHEN 4 THEN 'H_3'
    WHEN 5 THEN 'H_4'
    ELSE NULL
  END AS activity_tag
FROM (
  SELECT
    a.uid,
    a.loncol,
    a.latcol,
    a.rank_night
  FROM ljx_homeid_202311_2 a
  JOIN ljx_user_showdays_202311 b ON a.uid = b.uid
  JOIN ljx_user_showgrids_202311 c ON a.uid = c.uid
  WHERE a.rank_night <= 5 AND
        a.duration_night / b.days_count >= 5 * 3600 AND
        b.days_count >= 20 AND
        c.grids_count > 1
) AS subquery_alias;

-- 5. Top five workplaces
CREATE TABLE ljx_user_showdate_202311 AS 
SELECT DISTINCT uid, DATE(stime) AS date 
FROM ljx_202311_activity_all;

CREATE TABLE ljx_user_showdays_work_202311 AS 
SELECT uid, COUNT(date) AS showdays_work
FROM ljx_user_showdate_202311 
WHERE EXTRACT(DOW FROM date) BETWEEN 1 AND 5 
GROUP BY uid;

CREATE TABLE ljx_user_girdw_202311 AS
  SELECT 
    uid,
    loncol,
    latcol,
    0 AS flag
  FROM  ljx_homeid_202311_2;

-- Inserting ljx_user_home_attribute data
INSERT INTO ljx_user_girdw_202311 (uid, loncol, latcol, flag)
  SELECT
    uid,
    loncol,
    latcol,
    CASE 
      WHEN activity_tag IN ('H_0', 'H_1', 'H_2', 'H_3', 'H_4', 'H_5') THEN 1 
      ELSE 0 
    END AS flag
  FROM ljx_user_home_attribute_202311;

CREATE TABLE ljx_user_gird_flagw_202311 AS
  SELECT 
    uid,
    loncol,
    latcol,
    SUM(flag) AS flag
  FROM ljx_user_girdw_202311
  GROUP BY uid, loncol, latcol;

-- Creating ljx_user_work_rank table
CREATE TABLE ljx_user_wrok_rank_202311 AS
  SELECT 
    a.uid,
    a.loncol,
    a.latcol,
    a.duration_day,
    ROW_NUMBER() OVER (PARTITION BY a.uid ORDER BY a.duration_day DESC) AS rank_day
  FROM ljx_homeid_202311_2 a
  JOIN ljx_user_gird_flagw_202311 b ON a.uid = b.uid AND a.loncol = b.loncol AND a.latcol = b.latcol
  WHERE flag = 0;

CREATE TABLE ljx_user_wrok_attribute_202311 AS
SELECT DISTINCT
  a.uid,
  a.loncol,
  a.latcol,
  'W' AS tag_type,
  CASE a.rank_day
    WHEN 1 THEN 'W_0'
    WHEN 2 THEN 'W_1'
    WHEN 3 THEN 'W_2'
    WHEN 4 THEN 'W_3'
    WHEN 5 THEN 'W_4'
    ELSE NULL
  END AS activity_tag
FROM 
  ljx_user_wrok_rank_202311 a
JOIN ljx_user_showdays_work_202311 b ON a.uid = b.uid
JOIN ljx_user_showdays_202311 c ON a.uid = c.uid
JOIN ljx_user_showgrids_202311 d ON a.uid = d.uid
WHERE a.rank_day <= 5 AND
      a.duration_day / b.showdays_work >= 2 * 3600 AND
      c.days_count >= 20 AND
      d.grids_count > 1;

-- 6. Other locations
CREATE TABLE ljx_user_girdo_202311 AS
  SELECT 
    uid,
    loncol,
    latcol,
    0 AS flag
  FROM ljx_homeid_202311_2;

-- Inserting ljx_user_home_attribute data
INSERT INTO ljx_user_girdo_202311 (uid, loncol, latcol, flag)
  SELECT
    uid,
    loncol,
    latcol,
    1 AS flag
  FROM ljx_user_home_attribute_202311;

-- Inserting ljx_user_wrok_attribute data
INSERT INTO ljx_user_girdo_202311 (uid, loncol, latcol, flag)
  SELECT
    uid,
    loncol,
    latcol,
    1 AS flag
  FROM ljx_user_wrok_attribute_202311;

-- Creating ljx_user_gird_flago table and calculating flags
CREATE TABLE ljx_user_gird_flago_202311 AS
  SELECT 
    uid,
    loncol,
    latcol,
    SUM(flag) AS flag
  FROM ljx_user_girdo_202311
  GROUP BY uid, loncol, latcol;

-- Creating ljx_user_other_rank table
CREATE TABLE ljx_user_other_rank_202311 AS
  SELECT 
    a.uid,
    a.loncol,
    a.latcol,
    DENSE_RANK() OVER (PARTITION BY a.uid ORDER BY GREATEST(a.duration_day, a.duration_night) DESC) AS rank_daynight
  FROM ljx_homeid_202311_2 a
  JOIN ljx_user_gird_flago_202311 b ON a.uid = b.uid AND a.loncol = b.loncol AND a.latcol = b.latcol
  WHERE flag = 0;

CREATE TABLE ljx_user_other_attribute_202311 AS
SELECT DISTINCT
  a.uid,
  a.loncol,
  a.latcol,
  'O' AS tag_type,
  CASE a.rank_daynight
    WHEN  1 THEN 'O_0'
    WHEN  2 THEN 'O_1'
    WHEN  3 THEN 'O_2'
    WHEN  4 THEN 'O_3'
    WHEN  5 THEN 'O_4'
    WHEN  6 THEN 'O_5'
    WHEN  7 THEN 'O_6'
    WHEN  8 THEN 'O_7'
    WHEN  9 THEN 'O_8'
    WHEN  10 THEN 'O_9'
    ELSE NULL
  END AS activity_tag
FROM 
  ljx_user_other_rank_202311 a
JOIN ljx_user_showdays_202311 c ON a.uid = c.uid
JOIN ljx_user_showgrids_202311 d ON a.uid = d.uid
WHERE
    a.rank_daynight <= 10 AND
    c.days_count >= 20 AND
    d.grids_count > 1;

CREATE TABLE ljx_home2work_info_202311 AS
SELECT
    h.uid,
    h.loncol AS hloncol,
    h.latcol AS hlatcol,
    h.activity_tag AS htype,
    w.loncol AS wloncol,
    w.latcol AS wlatcol,
    w.activity_tag AS wtype
FROM
    ljx_user_home_attribute_202311 h
JOIN
    ljx_user_wrok_attribute_202311 w
ON
    h.uid = w.uid;

-- Key location h2w
CREATE TABLE ljx_home2work_distribution_202311 AS
SELECT
    COUNT(uid) AS count,
    wloncol,
    wlatcol,
    wtype,
    hloncol,
    hlatcol,
    htype
FROM
    ljx_home2work_info_202311
GROUP BY 
    wloncol,
    wlatcol,
    wtype,
    hloncol,
    hlatcol,
    htype;

CREATE TABLE ljx_hw2other_info_202311 AS
SELECT
    h.uid,
    h.loncol AS hloncol,
    h.latcol AS hlatcol,
    h.activity_tag AS htype,
    w.loncol AS wloncol,
    w.latcol AS wlatcol,
    w.activity_tag AS wtype,
    o.loncol AS oloncol,
    o.latcol AS olatcol,
   
-- 1. 合并用户工作地、居住地、其他地信息
CREATE TABLE ljx_user_all_attribute_202311 AS
SELECT uid, loncol, latcol, activity_tag 
FROM ljx_user_home_attribute_202311
UNION ALL
SELECT uid, loncol, latcol, activity_tag 
FROM ljx_user_wrok_attribute_202311
UNION ALL
SELECT uid, loncol, latcol, activity_tag 
FROM ljx_user_other_attribute_202311;

-- 2. activity_tag
CREATE TABLE ljx_202311_activity_all_tag AS
SELECT
  a.uid,
  a.stime,
  a.etime,
  a.loncol,
  a.latcol,
  u.activity_tag,
  DENSE_RANK() OVER (ORDER BY a.uid) AS reindex
FROM ljx_202311_activity_all a
JOIN ljx_user_all_attribute_202311 u 
ON a.uid = u.uid AND a.loncol = u.loncol AND a.latcol = u.latcol;

-- 3. activity_tag_hour，统计用户活动及活动开始时间
CREATE TABLE ljx_202311_activity_all_tag_hour AS
SELECT
  reindex,
  SUBSTRING(stime FROM 12 FOR 2) AS shour,
  SUBSTRING(etime FROM 12 FOR 2) AS ehour,
  activity_tag AS type
FROM ljx_202311_activity_all_tag;

-- 4. activity_tag_hour_count，活动及活动类型计数
CREATE TABLE ljx_202311_activity_all_tag_hour_count AS
SELECT
  reindex,
  shour,
  ehour,
  type,
  COUNT(*) AS count
FROM ljx_202311_activity_all_tag_hour
GROUP BY 
  reindex,
  shour,
  ehour,
  type;

-- 5. activity_move，OD移动类型统计
CREATE TABLE ljx_202311_activity_move AS
SELECT 
  uid,
  reindex,
  etime AS stime,
  loncol AS sloncol,
  latcol AS slatcol,
  activity_tag AS stype,
  LEAD(stime, 1) OVER (PARTITION BY uid ORDER BY stime) AS etime,
  LEAD(loncol, 1) OVER (PARTITION BY uid ORDER BY stime) AS eloncol,
  LEAD(latcol, 1) OVER (PARTITION BY uid ORDER BY stime) AS elatcol,
  LEAD(activity_tag, 1) OVER (PARTITION BY uid ORDER BY stime) AS etype,
  EXTRACT(EPOCH FROM (LEAD(stime, 1) OVER (PARTITION BY uid ORDER BY stime) - etime)) AS duration
FROM ljx_202311_activity_all_tag;

-- 6. activity_move_hour, OD移动类型，开始时间
CREATE TABLE ljx_202311_activity_move_hour AS
SELECT
  reindex,
  stype,
  etype,
  SUBSTRING(stime FROM 12 FOR 2) AS shour
FROM ljx_202311_activity_move;

-- 7. activity_move_hour_count，OD移动类型、开始时间、计数
CREATE TABLE ljx_202311_activity_move_hour_count AS
SELECT
  reindex,
  stype,
  etype,
  shour,
  COUNT(*) AS count
FROM ljx_202311_activity_move_hour
GROUP BY 
  reindex,
  stype,
  etype,
  shour;
