SELECT 	flight_date,
		dep_hour,
		origin,
		a.name AS airport,
		a.city,
		dep_delay_interval AS dep_delay,
		airline,
		tail_number,
		cancelled,
		temp_c,
		dewpoint_c,
		precipitation_mm,
		snow_mm,
		wind_direction,
		wind_speed_kmh,
		wc.weather_condition 
FROM {{ref('prep_flights')}} pf
JOIN {{ref('prep_airports')}} a 
ON pf.origin = a.faa 
JOIN {{ref('prep_weather_hourly')}} pwh 
ON pf.flight_date = pwh.date AND pf.origin = pwh.airport_code AND pf.dep_hour = pwh.HOUR
JOIN {{ref('weather_condition')}} wc 
ON condition_code = wc.code
ORDER BY flight_date, dep_hour