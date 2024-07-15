SELECT * FROM "spotify-db"."datawarehouse" limit 10;


SELECT name,track_id from datawarehouse limit 10;

#Query to Find Top 10 Most Popular Tracks
SELECT track_name, track_popularity
FROM datawarehouse
ORDER BY track_popularity DESC
LIMIT 10;


#Query to Count Tracks by Each Artist 
SELECT artist_id, COUNT(track_id) AS track_count
FROM datawarehouse
GROUP BY artist_id
ORDER BY track_count DESC;


#Query to Find Tracks Released After a Specific Date
SELECT track_name, release_date
FROM datawarehouse
WHERE release_date > '2020-01-01'
ORDER BY release_date DESC;


#Query to List Artists with their Most Popular Track
SELECT artist_id, track_name, MAX(track_popularity) AS max_popularity
FROM datawarehouse
GROUP BY artist_id, track_name
ORDER BY max_popularity DESC;



#Query to Calculate the Total Duration of All Tracks in Seconds
SELECT SUM(duration_sec) AS total_duration_sec
FROM datawarehouse;



#Query to Find the Most Popular Album
SELECT album_name, album_popularity
FROM your_database.your_table
ORDER BY album_popularity DESC
LIMIT 1;






