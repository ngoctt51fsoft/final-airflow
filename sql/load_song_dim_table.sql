insert into songs select distinct song_id, title, artist_id, year, duration
FROM staging_songs
