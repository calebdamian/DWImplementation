USE cdnastg;
SELECT
	(SELECT COUNT(*) FROM channels_ext WHERE CHANNEL_ID is not null) as CHANN_ext_COUNT,
	(SELECT COUNT(*) FROM countries_ext WHERE COUNTRY_ID is not null) as COUN_ext_COUNT,
	(SELECT COUNT(*) FROM customers_ext WHERE CUST_ID is not null)as CUS_ext_COUNT,
    (SELECT COUNT(*) FROM promotions_ext WHERE PROMO_ID is not null) AS PROM_ext_COUNT,
    (SELECT COUNT(*) FROM products_ext WHERE PROD_ID is not null) AS PROD_ext_COUNT,
    (SELECT COUNT(*) FROM sales_ext WHERE PROD_ID is not null) AS SALES_ext_COUNT,
    (SELECT COUNT(*) FROM times_ext WHERE TIME_ID is not null) AS TIMES_ext_COUNT