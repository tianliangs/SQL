

#1-获取用户停留表
create table ljx_stay_month_202311 as select
        uid,
        stime,
        date,
        poi_id,
        city,
        province
    from
        stay_month
    where
        province = '011'  and 
        city = 'V0110000'and 
        SUBSTRING(date,1,6) in (
        '202311' );

#获取经纬度
create table ljx_stay_month_poi_202311 as select
        a.uid,
        a.stime,
        b.weighted_centroid_lon,
        b.weighted_centroid_lat   
    from
        ljx_stay_month_202311 a   
    join
        stay_poi b   
            on a.uid = b.uid   
            and a.poi_id = b.poi_id 
            and b.province = '011'   
            and b.city = 'V0110000' ;
        

#2-将经纬度进行分箱
create table ljx_202311_bins as select
        uid,
        concat(substring(stime,
        1,
        15),
        "0:00") stime,
        cast(((avg(weighted_centroid_lon)-114.5)/0.005874204851473869)+0.5 as int) loncol,
        cast(((avg(weighted_centroid_lat)-39)/0.004496605206422906)+0.5 as int) latcol
    from
        ljx_stay_month_poi_202311
    group by
        uid,
        concat(substring(stime,
        1,
        15),
        "0:00");  

#3-活动识别
#分箱后状态变化检测
create table ljx_bins202311_1 as
select 
uid,
stime,
loncol,
latcol,
sign(abs(float(concat(cast(loncol as string),cast(latcol as string)))-
lag(float(concat(cast(loncol as string),cast(latcol as string))),1) over( partition by uid order by stime))) statuschange
from ljx_202311_bins;
#分箱后状态排序
create table ljx_bins202311_2 as
select 
uid,
stime,
lead(stime,1) over(partition by uid order by stime) etime,
loncol,
latcol,
sum(statuschange) over(partition by uid order by stime rows between unbounded preceding and current row) statusrank
from ljx_bins202311_1;
#合并活动链
create table ljx_bins202311_3 as
select 
uid,
min(stime) stime,
max(etime) etime,
count(stime) flag,
loncol,
latcol,
statusrank
from ljx_bins202311_2
group by
uid,
loncol,
latcol,
statusrank;
#提取活动
create table ljx_202311_activity_all as
select 
uid,
stime,
etime,
loncol,
latcol,
to_unix_timestamp(etime)-to_unix_timestamp(stime) duration,
row_number() over(partition by uid order by stime) rank
from ljx_bins202311_3
where 
to_unix_timestamp(etime)-to_unix_timestamp(stime) >=1800;


#-计算日夜停留时间
create table ljx_homeid_202311_1 as select uid, loncol, latcol, 
sum( greatest(greatest(least(to_unix_timestamp(etime), to_unix_timestamp(concat(substring(stime,1,10),' 20:00:00'))), to_unix_timestamp(concat(substring(stime,1,10),' 08:00:00')))-least(greatest(to_unix_timestamp(stime), to_unix_timestamp(concat(substring(stime,1,10),' 08:00:00'))), to_unix_timestamp(concat(substring(stime,1,10),' 20:00:00')))+greatest(least(to_unix_timestamp(etime), to_unix_timestamp(concat(substring(stime,1,10),' 20:00:00'))+24*3600), to_unix_timestamp(concat(substring(stime,1,10),' 08:00:00'))+24*3600)-least(greatest(to_unix_timestamp(stime), to_unix_timestamp(concat(substring(stime,1,10),' 08:00:00'))+24*3600), to_unix_timestamp(concat(substring(stime,1,10),' 20:00:00'))+24*3600),0)) duration_day,
sum( duration - greatest(greatest(least(to_unix_timestamp(etime), to_unix_timestamp(concat(substring(stime,1,10),' 20:00:00'))), to_unix_timestamp(concat(substring(stime,1,10),' 08:00:00')))-least(greatest(to_unix_timestamp(stime), to_unix_timestamp(concat(substring(stime,1,10),' 08:00:00'))), to_unix_timestamp(concat(substring(stime,1,10),' 20:00:00')))+greatest(least(to_unix_timestamp(etime), to_unix_timestamp(concat(substring(stime,1,10),' 20:00:00'))+24*3600), to_unix_timestamp(concat(substring(stime,1,10),' 08:00:00'))+24*3600)-least(greatest(to_unix_timestamp(stime), to_unix_timestamp(concat(substring(stime,1,10),' 08:00:00'))+24*3600), to_unix_timestamp(concat(substring(stime,1,10),' 20:00:00'))+24*3600),0)) duration_night 
from ljx_202311_activity_all group by uid, loncol, latcol;

create table ljx_homeid_202311_2 as 
select  
uid,
loncol,
latcol,
duration_day,
duration_night,
row_number() over(partition by uid order by duration_day desc ) rank_day,
row_number() over(partition by uid order by duration_night desc ) rank_night
from ljx_homeid_202311_1;


#2-统计用户出现栅格数
create table ljx_user_showgrids_202311 AS
SELECT
    uid,
    COUNT(DISTINCT concat(loncol,'_',latcol)) AS grids_count
FROM
    ljx_202311_activity_all
GROUP BY
    uid;

#3-用户出现天数
create table ljx_user_showdays_202311 AS
SELECT
    uid,
    COUNT(DISTINCT DATE(stime)) AS days_count
FROM
    ljx_202311_activity_all
GROUP BY
    uid;

#4-居住地前五
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
        a.duration_night/b. days_count >=5*3600 AND
        b.days_count >= 20 AND
        c.grids_count > 1
) AS subquery_alias;

#5-工作地前五
create table ljx_user_showdate_202311 as select distinct uid,DATE(stime) date from ljx_202311_activity_all;
create table ljx_user_showdays_work_202311 as select uid,count(date) showdays_work
from ljx_user_showdate_202311 where DAYOFWEEK(date)>=2 and DAYOFWEEK(date)<=6 group by uid;

CREATE TABLE ljx_user_girdw_202311 AS
  SELECT 
    uid,
    loncol,
    latcol,
    0 AS flag
  FROM  ljx_homeid_202311_2;

-- 插入ljx_user_home_attribute数据
INSERT INTO ljx_user_girdw_202311 (uid, loncol, latcol, flag)
  SELECT
    uid,
    loncol,
    latcol,
    CASE 
      WHEN activity_tag = 'H_0' OR activity_tag = 'H_1' OR activity_tag = 'H_2' OR activity_tag = 'H_3' OR activity_tag = 'H_4' OR activity_tag = 'H_5' THEN 1 
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

-- 创建ljx_user_wrok_rank表
CREATE TABLE ljx_user_wrok_rank_202311 AS
  SELECT 
    a.uid,
    a.loncol,
    a.latcol,
     a.duration_day,
    row_number() over(partition by a.uid order by a.duration_day desc ) as rank_day
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
join ljx_user_showdays_work_202311  b ON a.uid = b.uid
  JOIN ljx_user_showdays_202311 c ON a.uid = c.uid
  JOIN ljx_user_showgrids_202311 d ON a.uid = d.uid
WHERE a.rank_day <= 5 and
      a.duration_day/b.showdays_work >= 2*3600 and
      c.days_count >= 20 AND
      d.grids_count > 1 ;

#6-其他地
#创建ljx_user_gird表并插入数据
CREATE TABLE ljx_user_girdo_202311 AS
  SELECT 
    uid,
    loncol,
    latcol,
    0 AS flag
  FROM ljx_homeid_202311_2;

#插入ljx_user_home_attribute数据
INSERT INTO ljx_user_girdo_202311 (uid, loncol, latcol, flag)
  SELECT
    uid,
    loncol,
    latcol,
    1 AS flag
  FROM ljx_user_home_attribute_202311;

#插入ljx_user_wrok_attribute数据
INSERT INTO ljx_user_girdo_202311 (uid, loncol, latcol, flag)
  SELECT
    uid,
    loncol,
    latcol,
    1 AS flag
  FROM ljx_user_wrok_attribute_202311;

#创建ljx_user_gird_flag表并计算flag
CREATE TABLE ljx_user_gird_flago_202311 AS
  SELECT 
    uid,
    loncol,
    latcol,
    SUM(flag) AS flag
  FROM ljx_user_girdo_202311
  GROUP BY uid, loncol, latcol;

#创建ljx_user_other_rank表
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
    a.rank_daynight <= 10 and
    c.days_count >= 20 AND
    d.grids_count > 1 ;

create table ljx_home2work_info_202311 as
SELECT
    h.UID,
    h.loncol as hloncol,
    h.latcol as hlatcol,
    h.activity_tag as htype,
    w.loncol as wloncol,
    w.latcol as wlatcol,
    w.activity_tag as wtype
FROM
    ljx_user_home_attribute_202311 h
JOIN
    ljx_user_wrok_attribute_202311 w
ON
    h.UID = w.UID;

#key location h2w
create table ljx_home2work_distribution_202311 as
SELECT
count(uid) as count,
wloncol,
wlatcol,
wtype,
hloncol,
hlatcol,
htype
from
ljx_home2work_info_202311
group by 
wloncol,
wlatcol,
wtype,
hloncol,
hlatcol,
htype;

CREATE TABLE ljx_hw2other_info_202311 AS
SELECT
    h.uid,
    h.loncol as hloncol,
    h.latcol as hlatcol,
    h.activity_tag as htype,
    w.loncol as wloncol,
    w.latcol as wlatcol,
    w.activity_tag as wtype,
    o.loncol as oloncol,
    o.latcol as olatcol,
    o.activity_tag as otype
FROM
    ljx_user_home_attribute_202311 h
JOIN
    ljx_user_wrok_attribute_202311 w
ON
    h.uid = w.uid
JOIN
    ljx_user_other_attribute_202311 o
ON
    w.uid = o.uid;

#keylocation hw2o
create table ljx_hw2other_distribution_202311 as
SELECT
count(uid) as count,
hloncol,
hlatcol,
htype,
wloncol,
wlatcol,
wtype,
oloncol,
olatcol,
otype
from
ljx_hw2other_info_202311
group by 
hloncol,
hlatcol,
htype,
wloncol,
wlatcol,
wtype,
oloncol,
olatcol,
otype;


#1.-合并activity_tag,合并用户工作地、居住地、其他地信息
CREATE TABLE ljx_user_all_attribute_202311 AS
SELECT uid, loncol, latcol, activity_tag FROM ljx_user_home_attribute_202311
UNION ALL
SELECT uid, loncol, latcol, activity_tag FROM ljx_user_wrok_attribute_202311
UNION ALL
SELECT uid, loncol, latcol,activity_tag FROM ljx_user_other_attribute_202311;

#2-activity_tag
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
JOIN ljx_user_all_attribute_202311 u ON a.uid = u.uid and a.loncol = u.loncol and a.latcol = u.latcol;

#3-activity_tag_hour，统计用户活动及活动开始时间
CREATE TABLE ljx_202311_activity_all_tag_hour AS
SELECT
  reindex,
  substring(stime,12,2) As shour,
  substring(etime,12,2) As ehour,
  activity_tag as type
FROM ljx_202311_activity_all_tag;


#4-activity_tag_hour_count，活动及活动类型计数
CREATE TABLE ljx_202311_activity_all_tag_hour_count AS
SELECT
  reindex,
  shour,
  ehour,
  type,
  count(*) as count
FROM ljx_202311_activity_all_tag_hour
group by 
  reindex,
  shour,
  ehour,
  type;

#5-activity_move，OD移动类型统计
create table ljx_202311_activity_move as
select 
uid,
reindex,
etime stime,
loncol sloncol,
latcol slatcol,
activity_tag stype,
lead(stime,1) over(partition by uid order by stime) etime,
lead(loncol,1) over(partition by uid order by stime) eloncol,
lead(latcol,1) over(partition by uid order by stime) elatcol,
lead(activity_tag,1) over(partition by uid order by stime) etype,
to_unix_timestamp(lead(stime,1) over(partition by uid order by stime)) - to_unix_timestamp(etime) duration
from ljx_202311_activity_all_tag;

#6-activity_move_hour,OD移动类型，开始时间
CREATE TABLE ljx_202311_activity_move_hour AS
SELECT
  reindex,
  stype,
  etype,
  substring(stime,12,2) As shour
FROM ljx_202311_activity_move;

#7-activity_move_hour_count，OD移动类型、开始时间、计数
CREATE TABLE ljx_202311_activity_move_hour_count AS
SELECT
  reindex,
  stype,
  etype,
  shour,
  count(*) as count
FROM ljx_202311_activity_move_hour
group by 
  reindex,
  stype,
  etype,
  shour;



