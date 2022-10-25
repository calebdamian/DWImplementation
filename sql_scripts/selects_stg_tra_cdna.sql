USE cdnastg;
SELECT
	(SELECT COUNT(*) FROM channels_tra WHERE surr_id is not null) as CHANN_TRA_COUNT,
	(SELECT COUNT(*) FROM countries_tra WHERE surr_id is not null) as COUN_TRA_COUNT,
	(SELECT COUNT(*) FROM customers_tra WHERE surr_id is not null)as CUS_TRA_COUNT,
    (SELECT COUNT(*) FROM promotions_tra WHERE surr_id is not null) AS PROM_TRA_COUNT,
    (SELECT COUNT(*) FROM sales_tra WHERE surr_id is not null) AS SALES_TRA_COUNT,
    (SELECT COUNT(*) FROM products_tra WHERE surr_id is not null) AS PROD_TRA_COUNT,
    (SELECT COUNT(*) FROM times_tra WHERE surr_id is not null) AS TIMES_TRA_COUNT
