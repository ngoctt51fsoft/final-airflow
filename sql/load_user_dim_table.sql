insert into users select distinct userid, firstname, lastname, gender
FROM staging_events
WHERE page='NextSong' and userid IS NOT NULL
