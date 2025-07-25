insert into time SELECT distinct start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
        extract(month from start_time), extract(year from start_time), extract(dow from start_time)
FROM songplays
