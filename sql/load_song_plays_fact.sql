insert into public.songplays select
        md5(cast(events.sessionid as TEXT) || cast(events.start_time as TEXT)) songplay_id,
        events.start_time, 
        events.userid, 
        events.level, 
        songs.song_id, 
        songs.artist_id, 
        events.sessionid, 
        events.location, 
        events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
    FROM staging_events
    WHERE page='NextSong') events
    LEFT JOIN staging_songs songs
    ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration
