﻿CREATE TABLE
	weather_report
(
	station_id VARCHAR(50),
    station_name VARCHAR(255),
    latitude DECIMAL(20, 14),
    longitude DECIMAL(20, 14),
    elevation DECIMAL(16, 1),
    date_recorded DATETIME,
    awnd DECIMAL(10, 2),
    dapr DECIMAL(10, 2),
    mdpr DECIMAL(10, 2),
    pgmt DECIMAL(10, 2),
    prcp DECIMAL(10, 2),
    snow DECIMAL(10, 2),
    snwd DECIMAL(10, 2),
    tavg DECIMAL(10, 2),
    tmax DECIMAL(10, 2),
    tmin DECIMAL(10, 2),
    tobs DECIMAL(10, 2),
    tsun DECIMAL(10, 2),
    wdf2 DECIMAL(10, 2),
    wdf5 DECIMAL(10, 2),
    wesd DECIMAL(10, 2),
    wesf DECIMAL(10, 2),
    wsf2 DECIMAL(10, 2),
    wsf5 DECIMAL(10, 2),
    wt01 DECIMAL(10, 2),
    wt02 DECIMAL(10, 2),
    wt03 DECIMAL(10, 2),
    wt05 DECIMAL(10, 2),
    wt08 DECIMAL(10, 2),
    wt11 DECIMAL(10, 2)
);

GRANT LOAD FROM S3 ON *.* TO 'root'@'aurora-stack-ny-bikes-auroracluster-ph2r14jv6v79.cluster-chijjzlxyras.us-east-1.rds.amazonaws.com';

LOAD DATA FROM S3 's3://ny-bikes-raw/2799304.csv' 
INTO TABLE weather_report 
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS (@station_id, 
	@station_name, 
	@latitude, 
    @longitude, 
    @elevation, 
    @date_recorded, 
    @awnd, 
    @dapr, 
    @mdpr, 
    @pgmt, 
    @prcp, 
    @snow, 
    @snwd, 
    @tavg, 
    @tmax,
    @tmin,
    @tobs,
    @tsun,
    @wdf2,
    @wdf5,
    @wesd,
    @wesf,
    @wsf2,
    @wsf5,
    @wt01,
    @wt02,
    @wt03,
    @wt05,
    @wt08,
    @wt11)
SET station_id = TRIM(NULLIF(TRIM(@station_id), '')),
station_name = TRIM(NULLIF(TRIM(@station_name), '')),
latitude  = NULLIF(TRIM(@latitude), ''),
longitude  = NULLIF(TRIM(@longitude), ''), 
elevation = NULLIF(TRIM(@elevation), ''),
date_recorded = 
	CASE
		WHEN STR_TO_DATE(TRIM(@date_recorded), '%Y-%m-%d %H:%i:%s') IS NOT NULL THEN STR_TO_DATE(TRIM(@date_recorded), '%Y-%m-%d %H:%i:%s')
		WHEN STR_TO_DATE(TRIM(@date_recorded), '%Y/%m/%d %H:%i:%s') IS NOT NULL THEN STR_TO_DATE(TRIM(@date_recorded), '%Y/%m/%d %H:%i:%s')
        ELSE NULL
	END,
awnd = NULLIF(TRIM(@awnd), ''),
dapr = NULLIF(TRIM(@dapr), ''),
mdpr = NULLIF(TRIM(@mdpr), ''),
pgmt = NULLIF(TRIM(@pgmt), ''),
prcp = NULLIF(TRIM(@prcp), ''),
snow = NULLIF(TRIM(@snow), ''),
snwd = NULLIF(TRIM(@snwd), ''),
tavg = NULLIF(TRIM(@tavg), ''),
tmax = NULLIF(TRIM(@tmax), ''),
tmin = NULLIF(TRIM(@tmin), ''),
tobs = NULLIF(TRIM(@tobs), ''),
tsun = NULLIF(TRIM(@tsun), ''),
wdf2 = NULLIF(TRIM(@wdf2), ''),
wdf5 = NULLIF(TRIM(@wdf5), ''),
wesd = NULLIF(TRIM(@wesd), ''),
wesf = NULLIF(TRIM(@wesf), ''),
wsf2 = NULLIF(TRIM(@wsf2), ''),
wsf5 = NULLIF(TRIM(@wsf5), ''),
wt01 = NULLIF(TRIM(@wt01), ''),
wt02 = NULLIF(TRIM(@wt02), ''),
wt03 = NULLIF(TRIM(@wt03), ''),
wt05 = NULLIF(TRIM(@wt05), ''),
wt08 = NULLIF(TRIM(@wt08), ''),
wt11 = NULLIF(TRIM(@wt11), '');