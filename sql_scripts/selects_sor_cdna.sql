USE cdnasor;
SELECT
	(SELECT COUNT(*) FROM dim_channels WHERE surr_id is not null) as CHANN_DIM_COUNT,
	(SELECT COUNT(*) FROM dim_countries WHERE surr_id is not null) as COUN_DIM_COUNT,
	(SELECT COUNT(*) FROM dim_customers WHERE surr_id is not null)as CUS_DIM_COUNT,
    (SELECT COUNT(*) FROM dim_products WHERE surr_id is not null) AS PROD_DIM_COUNT,
    (SELECT COUNT(*) FROM dim_sales WHERE surr_id is not null) AS SALES_DIM_COUNT,
    (SELECT COUNT(*) FROM dim_promotions WHERE surr_id is not null) AS PROM_DIM_COUNT,
     (SELECT COUNT(*) FROM dim_times WHERE surr_id is not null) AS TIMES_DIM_COUNT
