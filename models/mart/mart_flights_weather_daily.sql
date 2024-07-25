WITH flights_daily AS (
			SELECT 	flight_date,
					origin,
					a.name AS airport,
					a.city,
					avg(dep_delay_interval) AS avg_dep_delay,
					count(*) AS n_total_flights,
					sum(cancelled) AS n_cancelled_flights,
					(sum(cancelled)*100.0 / count(*))::NUMERIC(4,2) AS cancel_rate
			FROM {{ref('prep_flights')}} pf
			JOIN {{ref('prep_airports')}} a 
			ON pf.origin = a.faa 
			GROUP BY pf.flight_date, pf.origin, a.name, a.city 
)
SELECT  fd.*,
		pwd.avg_temp_c,
		pwd.precipitation_mm,
		pwd.max_snow_mm,
		pwd.avg_wind_direction,
		pwd.avg_wind_speed_kmh
FROM flights_daily fd
JOIN {{ref('prep_weather_daily')}} pwd 
ON fd.flight_date = pwd.date AND fd.origin = pwd.airport_code 