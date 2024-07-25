WITH daily_data AS (
    SELECT * 
    FROM {{ref('staging_weather_daily')}}
),
add_features AS (
    SELECT *
        , DATE_PART('day', date)::INTEGER AS date_day
        , DATE_PART('month', date)::INTEGER AS date_month
        , DATE_PART('year', date)::INTEGER AS date_year
        , DATE_PART('week', date)::INTEGER AS cw
        , TO_CHAR(date, 'FMmonth') AS month_name
        , TO_CHAR(date, 'FMday') AS weekday
    FROM daily_data 
)
SELECT *
FROM add_features
ORDER BY date