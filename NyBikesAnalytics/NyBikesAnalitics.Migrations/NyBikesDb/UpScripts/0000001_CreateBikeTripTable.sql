CREATE TABLE 
	bike_trip
(
	ride_id VARCHAR(50),
    rideable_type VARCHAR(50),
    started_at DATETIME,
    ended_at DATETIME,
    start_station_name VARCHAR(255),
    start_station_id DECIMAL(6,2),
    end_station_name VARCHAR(255),
    end_station_id DECIMAL(6,2),
    start_lat DECIMAL(16, 14),
    start_lng DECIMAL(16, 14),
    end_lat DECIMAL(16, 14),
    end_lng DECIMAL(16, 14),
    member_casual VARCHAR(50)
);

GRANT LOAD FROM S3 ON *.* TO 'root'@'aurora-stack-ny-bikes-auroracluster-ph2r14jv6v79.cluster-chijjzlxyras.us-east-1.rds.amazonaws.com';

LOAD DATA FROM S3 's3://ny-bikes-raw/202110-citibike-tripdata-small.csv' 
INTO TABLE bike_trip 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS (@ride_id, @rideable_type, @started_at, @ended_at, @start_station_name, @start_station_id, @end_station_name, @end_station_id, @start_lat, @start_lng, @end_lat, @end_lng, @member_casual)
SET ride_id = TRIM(NULLIF(TRIM(@ride_id), '')),
rideable_type = TRIM(NULLIF(TRIM(@rideable_type), '')),
started_at = 
	CASE
		WHEN STR_TO_DATE(TRIM(@started_at), '%Y-%m-%d %H:%i:%s') IS NOT NULL THEN STR_TO_DATE(TRIM(@started_at), '%Y-%m-%d %H:%i:%s')
		WHEN STR_TO_DATE(TRIM(@started_at), '%Y/%m/%d %H:%i:%s') IS NOT NULL THEN STR_TO_DATE(TRIM(@started_at), '%Y/%m/%d %H:%i:%s')
        ELSE NULL
	END,
ended_at = 
	CASE
		WHEN STR_TO_DATE(TRIM(@ended_at), '%Y-%m-%d %H:%i:%s') IS NOT NULL THEN STR_TO_DATE(TRIM(@ended_at), '%Y-%m-%d %H:%i:%s')
		WHEN STR_TO_DATE(TRIM(@ended_at), '%Y/%m/%d %H:%i:%s') IS NOT NULL THEN STR_TO_DATE(TRIM(@ended_at), '%Y/%m/%d %H:%i:%s')
        ELSE NULL
	END,
start_station_name = TRIM(NULLIF(TRIM(@start_station_name), '')),
start_station_id = TRIM(NULLIF(TRIM(@start_station_id), '')),
end_station_name = TRIM(NULLIF(TRIM(@end_station_name), '')),
end_station_id = TRIM(NULLIF(TRIM(@end_station_id), '')),
start_lat = TRIM(NULLIF(TRIM(@start_lat), '')),
start_lng = TRIM(NULLIF(TRIM(@start_lng), '')),
end_lat = TRIM(NULLIF(TRIM(@end_lat), '')),
end_lng = TRIM(NULLIF(TRIM(@end_lng), '')),
member_casual = TRIM(NULLIF(TRIM(@member_casual), ''));