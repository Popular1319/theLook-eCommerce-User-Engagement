WITH
  main AS (
  SELECT
    *
  FROM
    bigquery-public-data.thelook_ecommerce.events ),
  type AS (-- This CTE is to create date functions to find the session interval and the required window functions like sessions_per_source and sessions_per_browser 
  SELECT
    user_id,
    session_id,
    DATE_DIFF(MAX(created_at),MIN(created_at),second) AS user_duration,
    COUNT(session_id) AS total_sessions,
    traffic_source,
    COUNT(main.session_id) OVER (PARTITION BY traffic_source) AS sessions_per_source,
    browser,
    COUNT(main.session_id) OVER (PARTITION BY browser) AS sessions_per_browser
  FROM
    main
  WHERE   -- Where statement is to add an extra level of granularity, to get zoomed in view of data on an user level events and sessions
    user_id = 62513
  GROUP BY
    user_id,
    traffic_source,
    browser,
    session_id )
SELECT      -- This CTE is to format the modeled data according to required table
  user_id,
  user_duration,
  --COUNT(main.event_type)/total_sessions AS avg_event_per_session,
  total_sessions,
  traffic_source,
  sessions_per_source,
  browser,
  sessions_per_browser
FROM
  type
GROUP BY
  user_id,
  user_duration,
  total_sessions,
  traffic_source,
  sessions_per_source,
  browser,
  sessions_per_browser