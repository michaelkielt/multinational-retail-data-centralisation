-- 1) How many stores does the business have and in which countries? 
SELECT country_code, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- 2) Which locations currently have the most stores? 
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 8;

-- 3) Which months produce the highest cost of sales typically? 
WITH MonthlySales AS (
    SELECT
        ddt.month,
        ROUND(SUM(CAST(ot.product_quantity * dp.product_price AS NUMERIC)), 2) AS total_sales
    FROM
        orders_table ot
        JOIN dim_products dp ON ot.product_code = dp.product_code
        JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
    GROUP BY
        ddt.month
)
SELECT
    ROUND(AVG(total_sales), 2) AS average_sales,
    month
FROM
    MonthlySales
GROUP BY
    month
ORDER BY
    average_sales DESC
LIMIT
	6;

-- 4) How many sales are coming from online? 
SELECT
    COUNT(*) AS number_of_sales,
    SUM(product_quantity) AS total_product_quantity,
    CASE
        WHEN store_code = 'WEB-1388012W' THEN 'Web'
        ELSE 'Offline'
    END AS location
FROM
    orders_table
GROUP BY
    CASE
        WHEN store_code = 'WEB-1388012W' THEN 'Web'
        ELSE 'Offline'
    END
ORDER BY
	number_of_sales ASC;

-- 5) What percentage of sales come through each type of store?
WITH StoreSales AS (
    SELECT
        dsd.store_type,
        SUM(ot.product_quantity * dp.product_price) AS total_sales
    FROM
        orders_table ot
        JOIN dim_products dp ON ot.product_code = dp.product_code
        JOIN dim_store_details dsd ON ot.store_code = dsd.store_code
    GROUP BY
        dsd.store_type
)
SELECT
    store_type,
    ROUND(SUM(total_sales)::numeric, 2) AS total_sales,
    ROUND((SUM(total_sales)::numeric * 100.0) / SUM(SUM(total_sales)::numeric) OVER (), 2) AS percentage_total
FROM
    StoreSales
GROUP BY
    store_type
ORDER BY
    total_sales DESC;

-- 6) Which month in each year produced the highest cost of sales? 
WITH MonthlySales AS (
    SELECT
        ddt.year AS year,
        ddt.month AS month,
        ROUND(SUM(ot.product_quantity * dp.product_price)::numeric, 2) AS total_sales
    FROM
        orders_table ot
        JOIN dim_products dp ON ot.product_code = dp.product_code
        JOIN dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
    GROUP BY
        year, month
)
SELECT
    total_sales,
    year,
    month
FROM
    MonthlySales
ORDER BY
    total_sales DESC
LIMIT 
    10;

-- 7) What is our staff headcount?
SELECT
	SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
	total_staff_numbers DESC;

-- 8) What German store type is selling the most?
 WITH StoreSales AS (
    	SELECT
            ROUND(SUM(ot.product_quantity * dp.product_price)::numeric, 2) AS total_sales,
        	dsd.store_type,
        	dsd.country_code
    	FROM
       		orders_table ot
        JOIN dim_products dp ON ot.product_code = dp.product_code
    	JOIN dim_store_details dsd ON ot.store_code = dsd.store_code
    	WHERE
        	dsd.country_code = 'DE'
    	GROUP BY
        	dsd.store_type, dsd.country_code
)
SELECT
    total_sales,
    store_type,
    Country_code
FROM
    StoreSales
ORDER BY
    total_sales DESC;

-- 9) How quickly is the company making sales?
WITH SalesCTE AS (
    SELECT
        ddt.year,
        (ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp)::timestamp with time zone AS sale_time,
        LEAD((ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp)::timestamp with time zone) 
            OVER (PARTITION BY ddt.year ORDER BY (ddt.year || '-' || ddt.month || '-' || ddt.day || ' ' || ddt.timestamp)::timestamp with time zone) AS next_sale_time
    FROM
        orders_table ot
    JOIN
        dim_date_times ddt ON ot.date_uuid = ddt.date_uuid
)

SELECT
    year,
    AVG(next_sale_time - sale_time) AS actual_time_taken
FROM
    SalesCTE
WHERE
    next_sale_time IS NOT NULL
GROUP BY
    year
ORDER BY
    actual_time_taken DESC
LIMIT 
	5;

