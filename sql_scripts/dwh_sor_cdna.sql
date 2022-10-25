DROP DATABASE if exists cdnasor;
CREATE DATABASE cdnasor;
USE cdnasor;
DROP TABLE IF EXISTS dim_channels;
CREATE TABLE dim_channels 
    ( 
    -- TABLAS DE DIMENSION TODAS CON ID AUTO INCREMENTABLE Y NUMERICO
	 surr_id INTEGER auto_increment primary KEY,
     channel_id INTEGER NOT NULL, 
     channel_desc VARCHAR(20) NOT NULL , 
     channel_class VARCHAR(20)  NOT NULL , 
     channel_class_id INTEGER  NOT NULL, 
     cod_etl bigint not null
    )
;
DROP TABLE IF EXISTS dim_countries;
CREATE TABLE dim_countries 
    ( 
     surr_id INTEGER auto_increment primary KEY,
     country_id INTEGER NOT NULL, 
     country_name VARCHAR(40)  NOT NULL , 
     country_region VARCHAR(20)  NOT NULL , 
     country_region_id INTEGER  NOT NULL,
     cod_etl bigint not null
    ) 
;

DROP TABLE IF EXISTS dim_customers;
CREATE TABLE dim_customers 
    ( 
	 surr_id INTEGER auto_increment primary KEY,
     cust_id INTEGER NOT NULL , 
     cust_name VARCHAR(60) NOT NULL, 
     cust_gender CHAR(1)  NOT NULL , 
     cust_year_of_birth INTEGER NOT NULL , 
     cust_marital_status VARCHAR(20) , 
     cust_street_address VARCHAR(40)  NOT NULL , 
     cust_postal_code VARCHAR(10)  NOT NULL , 
     cust_city VARCHAR(30)  NOT NULL , 
     cust_state_province VARCHAR(40)  NOT NULL , 
     country_id INTEGER  NOT NULL , 
     cust_main_phone_number VARCHAR(25)  NOT NULL , 
     cust_income_level VARCHAR(30) , 
     cust_credit_limit INTEGER , 
     cust_email VARCHAR(30),
     cod_etl bigint not null
    )
;

DROP TABLE IF EXISTS dim_products;
CREATE TABLE dim_products 
    ( 
	surr_id INTEGER auto_increment primary KEY,
     prod_id INTEGER NOT NULL , 
     prod_name VARCHAR(50)  NOT NULL , 
     prod_desc VARCHAR(4000)  NOT NULL , 
     prod_category VARCHAR(50)  NOT NULL , 
     prod_category_id INTEGER  NOT NULL , 
     prod_category_desc VARCHAR(2000)  NOT NULL , 
     prod_weight_class INTEGER  NOT NULL , 
     supplier_id INTEGER NOT NULL , 
     prod_status VARCHAR(20)  NOT NULL , 
     prod_list_price DECIMAL (8,2)  NOT NULL , 
     prod_min_price DECIMAL (8,2)  NOT NULL, 
     cod_etl bigint not null 
    )
;
DROP TABLE IF EXISTS dim_promotions;
CREATE TABLE dim_promotions 
    ( 
    surr_id INTEGER auto_increment primary KEY,
	promo_id INTEGER NOT NULL , 
    promo_name VARCHAR(30)  NOT NULL , 
    promo_cost DECIMAL (10,2)  NOT NULL , 
    promo_begin_date DATE  NOT NULL , 
    promo_end_date DATE  NOT NULL , 
    cod_etl bigint not null
    )
;
DROP TABLE IF EXISTS dim_times;
CREATE TABLE dim_times 
    ( 
     surr_id INTEGER auto_increment primary KEY,
     time_id DATE NOT NULL , 
     day_name VARCHAR(9)  NOT NULL , 
     day_number_in_week INTEGER  NOT NULL , 
     day_number_in_month INTEGER  NOT NULL , 
     calendar_week_number INTEGER  NOT NULL , 
     calendar_month_number INTEGER  NOT NULL , 
     calendar_month_desc VARCHAR(8)  NOT NULL , 
     end_of_cal_month DATE  NOT NULL , 
     calendar_month_name VARCHAR(9)  NOT NULL , 
     calendar_quarter_desc CHAR (7)  NOT NULL , 
     calendar_year INTEGER  NOT NULL , 
     cod_etl bigint not null
    ) 
;

DROP TABLE IF EXISTS dim_sales;
CREATE TABLE dim_sales 
    ( 
    surr_id INTEGER auto_increment primary KEY,
	prod_id INTEGER NOT NULL , 
    cust_id INTEGER  NOT NULL , 
    -- CAMBIAR DATE A INTEGER PARA PASARLE SURR
    time_id INTEGER  NOT NULL , 
    channel_id INTEGER  NOT NULL , 
    promo_id INTEGER  NOT NULL , 
    quantity_sold DECIMAL (10,2)  NOT NULL , 
    amount_sold DECIMAL (10,2)  NOT NULL , 
    cod_etl bigint not null
    ) 
;


/**
---------------------------------------------- CONSTRAINTS ----------------------------------------------
**/
-- OJO QUITAR EL COMMENT

alter table dim_customers
add
	constraint cus_count_fk foreign key(country_id) references dim_countries(surr_id);
alter table dim_sales
add
	constraint sl_ch_fk foreign key(channel_id) references dim_channels(surr_id);
alter table dim_sales
add
	constraint sl_cust_fk foreign key (cust_id) references dim_customers(surr_id);
alter table dim_sales
add
	constraint sl_prod_fk foreign key (prod_id) references dim_products(surr_id);
alter table dim_sales
add
	constraint sl_prom_fk foreign key (promo_id) references dim_promotions(surr_id);
alter table dim_sales
add
	constraint sl_time_fk foreign key (time_id) references dim_times(surr_id);