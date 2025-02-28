SELECT
    phrase,
    arrayMap((x, y) -> (x, y), groupArray(hour), groupArray(hour_views)) AS views_by_hour
FROM (
    SELECT
        phrase,
        toHour(dt) as hour,
        max(views) - min(views) as hour_views
    FROM
        phrases_views
    WHERE
        campaign_id = 1111111
        AND toDate(dt) = today()
    GROUP BY
        phrase,
        hour
    HAVING
        hour_views > 0
    ORDER BY
        phrase, hour DESC)
GROUP BY
    phrase
