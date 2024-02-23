SELECT
    counts.pagename,
    counts.hour,
    counts.average_pageviews
FROM (SELECT
            pagename,
            date_part('hour', insertion_date) AS hour,
            AVG(value) AS average_pageviews,
            ROW_NUMBER() OVER (PARTITION BY pagename ORDER BY AVG(value) DESC) AS row_num
        FROM pageviews_count
        GROUP BY
            page,
            hour) AS counts
WHERE counts.row_num = 1;
