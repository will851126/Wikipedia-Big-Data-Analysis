CREATE DATABASE project1;

USE project1;

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 500000;
SET hive.exec.max.dynamic.partitions.pernode = 500000;
SET hive.strict.checks.cartesian.product = false;
SET hive.mapred.mode = nonstrict;

-- MOST VIEWS

CREATE EXTERNAL TABLE IF NOT EXISTS pageviews (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/Project1Hive';

LOAD DATA INPATH '/user/skyler/Project1Files/' INTO TABLE pageviews;

CREATE TABLE IF NOT EXISTS en_pageviews (
	page STRING,
	views INT) 
	PARTITIONED BY (lang STRING)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t';

INSERT INTO TABLE en_pageviews PARTITION (lang = 'en')
SELECT page, views FROM pageviews WHERE lang = 'en';

CREATE TABLE IF NOT EXISTS total_en_pageviews
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page) 
AS total_views FROM en_pageviews 
WHERE page != 'Main_Page' AND page != 'Special:Search' AND page != '-';

SELECT * FROM total_en_pageviews
WHERE total_views > 10000
ORDER BY total_views DESC;

-- HIGHEST FRACTION OF INTERNAL LINKS

CREATE EXTERNAL TABLE IF NOT EXISTS april_pageviews (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/april-data';

LOAD DATA INPATH '/user/skyler/Question2/' INTO TABLE april_pageviews;

CREATE TABLE IF NOT EXISTS a_en_pageviews (
	page STRING,
	views INT)
	PARTITIONED BY (lang STRING)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t';

INSERT INTO TABLE a_en_pageviews PARTITION (lang = 'en')
SELECT page, views FROM april_pageviews WHERE (lang = 'en');

INSERT INTO TABLE a_en_pageviews PARTITION (lang = 'en.m')
SELECT page, views FROM april_pageviews WHERE (lang = 'en.m');

SELECT * FROM a_en_pageviews WHERE page = 'Hotel_California';

CREATE TABLE IF NOT EXISTS total_a_pageviews
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS total_views FROM a_en_pageviews 
WHERE page != 'Main_Page' AND page != 'Special:Search' AND page != '-';

CREATE TABLE IF NOT EXISTS q2_views
AS SELECT * FROM total_a_pageviews 
WHERE total_views > 999
ORDER BY total_views DESC;

CREATE EXTERNAL TABLE IF NOT EXISTS april_clickstream (
	prev STRING,
	curr STRING,
	type STRING,
	occ INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LOCATION '/user/skyler/clickstream-table';

LOAD DATA INPATH '/user/skyler/april-clickstream' INTO TABLE april_clickstream;

CREATE TABLE IF NOT EXISTS internal_links (
	prev STRING,
	curr STRING,
	occ INT)
	PARTITIONED BY (type STRING)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t';

INSERT INTO TABLE internal_links PARTITION (type = 'link')
SELECT prev, curr, occ FROM april_clickstream WHERE type = 'link';

SELECT * FROM internal_links WHERE prev = 'Hotel_California';

CREATE TABLE IF NOT EXISTS total_internal
AS SELECT DISTINCT(prev), SUM(occ) OVER (PARTITION BY prev ORDER BY prev)
AS total_links FROM internal_links
WHERE prev != 'Main_Page';

CREATE TABLE IF NOT EXISTS clickstream_final
AS SELECT prev, total_links FROM total_internal ORDER BY total_links DESC;

CREATE TABLE IF NOT EXISTS final_clickstream
AS SELECT prev, ROUND((total_links / 30), 0) AS daily_clickstream
FROM clickstream_final;

CREATE TABLE IF NOT EXISTS join_clickstream
AS SELECT * FROM final_clickstream WHERE daily_clickstream > 199 ORDER BY daily_clickstream DESC;

SELECT c.prev, c.daily_clickstream, v.total_views, ROUND((c.daily_clickstream / v.total_views), 4)
AS fraction FROM join_clickstream c INNER JOIN q2_views v 
ON (c.prev = v.page);

-- Largest fraction of readers from Hotel California

CREATE TABLE IF NOT EXISTS hc_clickstream
AS SELECT prev, ROUND((occ / 30), 4) 
AS daily_cickstream FROM internal_links 
WHERE prev = 'Hotel_California';

SELECT v.page, c.curr, c.occ, v.total_views, ROUND((c.occ / v.total_views), 4)
AS fraction FROM total_a_pageviews v INNER JOIN internal_links c
ON (v.page = c.prev) WHERE c.prev = 'Hotel_California'
AND c.curr != 'Hotel_California_(Eagles_album)' AND c.curr != 'Eagles_(band)';

SELECT v.page, c.curr, c.occ, v.total_views, ROUND((c.occ / v.total_views), 4)
AS fraction FROM total_a_pageviews v INNER JOIN internal_links c 
ON (v.page = c.prev) WHERE c.prev = 'Don_Felder';

SELECT v.page, c.curr, c.occ, v.total_views, ROUND((c.occ / v.total_views), 4)
AS fraction FROM total_a_pageviews v INNER JOIN internal_links c 
ON (v.page = c.prev) WHERE c.prev = 'On_the_Border'
AND curr != 'One_of_These_Nights' AND curr != 'Desperado_(Eagles_album)';

-- RELATIVELY MORE POPULAR PAGE IN AMERICA THAN GERMANY

CREATE EXTERNAL TABLE IF NOT EXISTS american_views (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/american-views';

LOAD DATA INPATH '/user/skyler/american/' INTO TABLE american_views;

CREATE TABLE IF NOT EXISTS en_am_views (
	page STRING,
	views INT)
	PARTITIONED BY (lang STRING)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t';

INSERT INTO TABLE en_am_views PARTITION (lang = 'en')
SELECT page, views FROM american_views WHERE lang = 'en';

CREATE TABLE IF NOT EXISTS total_am_views
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS total_views_am FROM en_am_views WHERE page != 'Main_Page' 
AND page != 'Special:Search' AND page != '-';

SELECT * FROM total_am_views
ORDER BY total_views_am DESC;

CREATE EXTERNAL TABLE IF NOT EXISTS gm_views (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/german-views';

LOAD DATA INPATH '/user/skyler/german/' INTO TABLE gm_views;

CREATE TABLE IF NOT EXISTS gm_de_views (
	page STRING,
	views INT)
	PARTITIONED BY (lang STRING)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t';

INSERT INTO TABLE gm_de_views PARTITION (lang = 'de')
SELECT page, views FROM gm_views WHERE lang = 'de';

CREATE TABLE IF NOT EXISTS total_gm_views
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS total_views_gm FROM gm_de_views WHERE page != 'Main_Page'
AND page != 'Special:Search' AND page != '-';

SELECT * FROM total_gm_views
ORDER BY total_views_gm DESC;

CREATE TABLE IF NOT EXISTS am_gm_views
AS SELECT a.page, a.total_views_am, g.total_views_gm FROM total_am_views a
INNER JOIN total_gm_views g ON (a.page = g.page);

SELECT * FROM am_gm_views;

-- AVERAGE VIEWS OF VANDALIZED PAGES

CREATE EXTERNAL TABLE IF NOT EXISTS  nov_views (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/nov-data/';
	
LOAD DATA INPATH '/user/skyler/nov-views/' INTO TABLE nov_views;

CREATE TABLE IF NOT EXISTS total_nov_views
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS total_views FROM nov_views;

CREATE TABLE IF NOT EXISTS avg_view_min
AS SELECT (AVG(total_views) / 1440) AS minute_views FROM total_nov_views
WHERE total_views > 50;

CREATE EXTERNAL TABLE IF NOT EXISTS vandalism (
	wiki_db STRING,
	event_entity STRING,
	event_type STRING,
	event_timestamp STRING,
	event_comment STRING,
	event_user_id INT,
	event_user_text_historical STRING,
	event_user_text STRING,
	event_user_blocks_historical STRING,
	event_user_blocks STRING,
	event_user_groups_historical STRING,
	event_user_groups STRING,
	event_user_is_bot_by_historical STRING,
	event_user_is_bot_by STRING,
	event_user_is_created_by_self BOOLEAN,
	event_user_is_created_by_system BOOLEAN,
	event_user_is_created_by_peer BOOLEAN,
	event_user_is_anonymous BOOLEAN, 
	event_user_registration_timestamp STRING,
	event_user_creation_timestamp STRING,
	event_user_first_edit_timestamp STRING,
	event_user_revision_count INT,
	event_user_seconds_since_previous_revision INT,
	page_id INT,
	page_title_historical  STRING,
	page_title  STRING,
	page_namespace_historical INT,
	page_namespace_is_content_historical BOOLEAN,
	page_namespace INT,
	page_namespace_is_content BOOLEAN,
	page_is_redirect BOOLEAN,
	page_is_deleted BOOLEAN,
	page_creation_timestamp STRING,
	page_first_edit_timestamp STRING,
	page_revision_count INT,
	page_seconds_since_previous_revision INT,
	user_id INT,
	user_text_historical STRING,	
	user_text	STRING,
	user_blocks_historical STRING,
	user_blocks	STRING,	
	user_groups_historical	STRING,	
	user_groups	STRING,
	user_is_bot_by_historical STRING,	
	user_is_bot_by	STRING,	
	user_is_created_by_self BOOLEAN,	
	user_is_created_by_system BOOLEAN,
	user_is_created_by_peer BOOLEAN,
	user_is_anonymous BOOLEAN,
	user_registration_timestamp	STRING,
	user_creation_timestamp	STRING,
	user_first_edit_timestamp	STRING,
	revision_id INT,
	revision_parent_id INT, 
	revision_minor_edit BOOLEAN, 
	revision_deleted_parts	STRING,
	revision_deleted_parts_are_suppressed BOOLEAN,
	revision_text_bytes INT, 
	revision_text_bytes_diff INT, 
	revision_text_sha1	STRING,
	revision_content_model	STRING, 
	revision_content_format	STRING, 
	revision_is_deleted_by_page_deletion BOOLEAN,	
	revision_deleted_by_page_deletion_timestamp	STRING, 
	revision_is_identity_reverted BOOLEAN,
	revision_first_identity_reverting_revision_id INT,
	revision_seconds_to_identity_revert INT,
	revision_is_identity_revert BOOLEAN,
	revision_is_from_before_page_creation BOOLEAN,
	revision_tags	STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/skyler/vandalism';

LOAD DATA INPATH '/user/skyler/vandal-data/' INTO TABLE vandalism;

CREATE TABLE IF NOT EXISTS revision_seconds
AS SELECT revision_seconds_to_identity_revert FROM vandalism 
WHERE user_id IS NULL AND revision_is_identity_reverted = true
AND revision_seconds_to_identity_revert > 0 AND revision_seconds_to_identity_revert < 3000;

CREATE TABLE IF NOT EXISTS avg_min_revision
AS SELECT (AVG(revision_seconds_to_identity_revert) / 60) AS average_revision_minutes
FROM revision_seconds;

CREATE TABLE IF NOT EXISTS vand_views
AS SELECT * FROM avg_min_revision, avg_view_min;

SELECT average_revision_minutes, minute_views, ROUND((average_revision_minutes / minute_views), 2)
AS avg_vand_views FROM vand_views;

-- BITCOIN GROWTH ANALYSIS

CREATE EXTERNAL TABLE IF NOT EXISTS bitcoin_2015 (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/b2015/';

LOAD DATA INPATH '/user/skyler/2015/' INTO TABLE bitcoin_2015;

CREATE TABLE IF NOT EXISTS total_2015
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS v_2015 FROM bitcoin_2015 WHERE page = 'Bitcoin';

CREATE EXTERNAL TABLE IF NOT EXISTS bitcoin_2016 (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/b2016/';

LOAD DATA INPATH '/user/skyler/2016/' INTO TABLE bitcoin_2016;

CREATE TABLE IF NOT EXISTS total_2016
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS v_2016 FROM bitcoin_2016 WHERE page = 'Bitcoin';

CREATE EXTERNAL TABLE IF NOT EXISTS bitcoin_2017 (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/b2017/';

LOAD DATA INPATH '/user/skyler/2017/' INTO TABLE bitcoin_2017;

CREATE TABLE IF NOT EXISTS total_2017
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS v_2017 FROM bitcoin_2017 WHERE page = 'Bitcoin';

CREATE EXTERNAL TABLE IF NOT EXISTS bitcoin_2018 (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/b2018/';

LOAD DATA INPATH '/user/skyler/2018/' INTO TABLE bitcoin_2018;

CREATE TABLE IF NOT EXISTS total_2018
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS v_2018 FROM bitcoin_2018 WHERE page = 'Bitcoin';

CREATE EXTERNAL TABLE IF NOT EXISTS bitcoin_2019 (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/b2019/';

LOAD DATA INPATH '/user/skyler/2019/' INTO TABLE bitcoin_2019;

CREATE TABLE IF NOT EXISTS total_2019
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS v_2019 FROM bitcoin_2019 WHERE page = 'Bitcoin';

CREATE EXTERNAL TABLE IF NOT EXISTS bitcoin_2020 (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/b2020/';

LOAD DATA INPATH '/user/skyler/2020/' INTO TABLE bitcoin_2020;

CREATE TABLE IF NOT EXISTS total_2020
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS v_2020 FROM bitcoin_2020 WHERE page = 'Bitcoin';

CREATE EXTERNAL TABLE IF NOT EXISTS bitcoin_2021 (
	lang STRING,
	page STRING,
	views INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' '
	LOCATION '/user/skyler/b2021/';

LOAD DATA INPATH '/user/skyler/2021/' INTO TABLE bitcoin_2021;

CREATE TABLE IF NOT EXISTS total_2021
AS SELECT DISTINCT(page), SUM(views) OVER (PARTITION BY page ORDER BY page)
AS v_2021 FROM bitcoin_2021 WHERE page = 'Bitcoin';

SELECT a.v_2015, b.v_2016, c.v_2017, d.v_2018, e.v_2019, f.v_2020, g.v_2021
FROM total_2015 a INNER JOIN total_2016 b ON (b.page = a.page)
INNER JOIN total_2017 c ON (c.page = b.page)
INNER JOIN total_2018 d ON (d.page = c.page)
INNER JOIN total_2019 e ON (e.page = c.page)
INNER JOIN total_2020 f ON (f.page = e.page)
INNER JOIN total_2021 g ON (g.page = f.page);

