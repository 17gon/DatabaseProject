--počet vysielaní pre každú spoločnosť--
SELECT
    dcom.company_name,
    COUNT(fb.cm_log_id) AS broadcast_count
FROM
    fact_broadcast fb
    JOIN dim_commercial dcom ON fb.dim_commersial_key = dcom.cm_base_id
GROUP BY
    dcom.company_name
ORDER BY
    broadcast_count DESC;


--Priemerná trvanie reklám podľa kategórie--
SELECT
    c.category AS category,
    ROUND(AVG(DATEDIFF('second','00:00:00'::TIME, fb.duration)), 2) AS avg_duration_seconds
FROM fact_broadcast fb
JOIN dim_category c ON fb.category_key = c.category
GROUP BY c.category
ORDER BY avg_duration_seconds DESC;



--počet vysielaní pre značky, top 5--
SELECT
    dcom.brand_name,
    COUNT(fb.cm_log_id) AS broadcast_count
FROM fact_broadcast fb
JOIN dim_commercial dcom ON fb.dim_commersial_key = dcom.cm_base_id
GROUP BY dcom.brand_name
ORDER BY broadcast_count DESC
LIMIT 5;


--počet vysielaní pre každý produkt--
SELECT
    dcom.product_name,
    COUNT(fb.cm_log_id) AS broadcast_count
FROM fact_broadcast fb
JOIN dim_commercial dcom ON fb.dim_commersial_key = dcom.cm_base_id
GROUP BY dcom.product_name
ORDER BY broadcast_count DESC
LIMIT 10;


--Počet reklám a unikátnych značiek podľa stanice--
SELECT
    ds.broadcast_station_name,
    COUNT(fb.cm_log_id) AS total_broadcasts,
    COUNT(DISTINCT dc.brand_name) AS unique_brands
FROM fact_broadcast fb
JOIN dim_station ds ON fb.station_id = ds.broadcast_station_id
JOIN dim_commercial dc ON fb.dim_commersial_key = dc.cm_base_id
GROUP BY ds.broadcast_station_name
ORDER BY total_broadcasts DESC;