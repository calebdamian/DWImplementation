drop database if exists cdnastg;
create database cdnastg;
USE cdnastg;

DROP TABLE IF EXISTS CHANNELS_ext;
CREATE TABLE channels_ext
    ( 
     CHANNEL_ID CHAR(255), 
     CHANNEL_DESC CHAR(255), 
     CHANNEL_CLASS CHAR(255) , 
     CHANNEL_CLASS_ID CHAR(255) 
    )
;


ALTER TABLE CHANNELS_ext 
    ADD CONSTRAINT CHANNELS_PK PRIMARY KEY ( CHANNEL_ID );

DROP TABLE IF EXISTS COUNTRIES_ext;
CREATE TABLE COUNTRIES_ext 
    ( 
     COUNTRY_ID CHAR(255)   , 
     COUNTRY_NAME CHAR(255)  , 
     COUNTRY_REGION CHAR(255) , 
     COUNTRY_REGION_ID CHAR(255) 
    ) 
;

ALTER TABLE COUNTRIES_ext 
    ADD CONSTRAINT COUNTRIES_PK PRIMARY KEY ( COUNTRY_ID ) ;

DROP TABLE IF EXISTS CUSTOMERS_ext;
CREATE TABLE CUSTOMERS_ext 
    ( 
     CUST_ID CHAR(255)   , 
     CUST_FIRST_NAME CHAR(255)  , 
     CUST_LAST_NAME CHAR(255) , 
     CUST_GENDER CHAR(255)  , 
     CUST_YEAR_OF_BIRTH CHAR(255) , 
     CUST_MARITAL_STATUS CHAR(255) , 
     CUST_STREET_ADDRESS CHAR(255)  , 
     CUST_POSTAL_CODE CHAR(255) , 
     CUST_CITY CHAR(255)   , 
     CUST_STATE_PROVINCE CHAR(255)   , 
     COUNTRY_ID CHAR(255)   , 
     CUST_MAIN_PHONE_NUMBER CHAR(255)   , 
     CUST_INCOME_LEVEL CHAR(255) , 
     CUST_CREDIT_LIMIT CHAR(255) , 
     CUST_EMAIL CHAR(255)
    )
;

ALTER TABLE CUSTOMERS_ext 
    ADD CONSTRAINT CUSTOMERS_PK PRIMARY KEY ( CUST_ID );

DROP TABLE IF EXISTS PRODUCTS_ext;
CREATE TABLE PRODUCTS_ext 
    ( 
     PROD_ID CHAR(255)  , 
     PROD_NAME CHAR(255)   , 
     PROD_DESC VARCHAR(4000)   , 
     PROD_CATEGORY CHAR(255)   , 
     PROD_CATEGORY_ID CHAR(255)   , 
     PROD_CATEGORY_DESC VARCHAR(2000)   , 
     PROD_WEIGHT_CLASS CHAR(255)   , 
     SUPPLIER_ID CHAR(255)  , 
     PROD_STATUS CHAR(255)   , 
     PROD_LIST_PRICE CHAR(255)   , 
     PROD_MIN_PRICE CHAR(255)   
    )
;

ALTER TABLE PRODUCTS_ext 
    ADD CONSTRAINT PRODUCTS_PK PRIMARY KEY ( PROD_ID ) ;

DROP TABLE IF EXISTS PROMOTIONS_ext;
CREATE TABLE PROMOTIONS_ext 
    ( 
     PROMO_ID CHAR(255)   , 
     PROMO_NAME CHAR(255)   , 
     PROMO_COST CHAR(255)   , 
     PROMO_BEGIN_DATE CHAR(255)   , 
     PROMO_END_DATE CHAR(255)   
    )
;

ALTER TABLE PROMOTIONS_ext 
    ADD CONSTRAINT PROMO_PK PRIMARY KEY ( PROMO_ID );

DROP TABLE IF EXISTS SALES_ext;
CREATE TABLE SALES_ext 
    ( 
     PROD_ID CHAR(255)  , 
     CUST_ID CHAR(255)   , 
     TIME_ID CHAR(255)   , 
     CHANNEL_ID CHAR(255)   , 
     PROMO_ID CHAR(255)   , 
     QUANTITY_SOLD CHAR(255)  , 
     AMOUNT_SOLD CHAR(255)   
    ) 
;

DROP TABLE IF EXISTS TIMES_ext;
CREATE TABLE TIMES_ext 
    ( 
     TIME_ID CHAR(255)   , 
     DAY_NAME CHAR(255)   , 
     DAY_NUMBER_IN_WEEK CHAR(255)  , 
     DAY_NUMBER_IN_MONTH CHAR(255)   , 
     CALENDAR_WEEK_NUMBER CHAR(255)   , 
     CALENDAR_MONTH_NUMBER CHAR(255)  , 
     CALENDAR_MONTH_DESC CHAR(255)   , 
     END_OF_CAL_MONTH CHAR(255)   , 
     CALENDAR_MONTH_NAME CHAR(255)   , 
     CALENDAR_QUARTER_DESC CHAR(255)   , 
     CALENDAR_YEAR CHAR(255)   
    ) 
;
USE cdnastg;
-- TRANSFORMATION TABLES
/**
-----------------------------------------------------------------------------
**/
DROP TABLE IF EXISTS channels_tra;
DROP TABLE IF EXISTS countries_tra;
DROP TABLE IF EXISTS customers_tra;
DROP TABLE IF EXISTS products_tra;
DROP TABLE IF EXISTS promotions_tra;
DROP TABLE IF EXISTS sales_tra;
DROP TABLE IF EXISTS times_tra;
/**
-----------------------------------------------------------------------------
**/
-- PROC TABLE
/**
-----------------------------------------------------------------------------
**/
DROP TABLE IF EXISTS etl_proc;
CREATE TABLE etl_proc(
	cod_etl bigint auto_increment primary key,
    created_at date not null
    -- cod_etl_tra bigint,
    -- cod_etl_load bigint
);

CREATE TABLE channels_tra
    ( 
    -- recordar que el id entra solo, por lo que solo se toma en cuenta a partir de este
	 surr_id INTEGER auto_increment primary KEY,
     channel_id INTEGER NOT NULL, 
     channel_desc VARCHAR(20) NOT NULL , 
     channel_class VARCHAR(20)  NOT NULL , 
     channel_class_id INTEGER NOT NULL,
     cod_etl bigint 
    )
;

/**

------------------ COUNTRIES ------------------

**/

CREATE TABLE countries_tra
    ( 
     surr_id INTEGER auto_increment primary KEY,
     country_id INTEGER NOT NULL, 
     country_name VARCHAR(40)  NOT NULL , 
     country_region VARCHAR(20)  NOT NULL , 
     country_region_id INTEGER  NOT NULL,
     cod_etl bigint 
    ) 
;

/**

------------------ CUSTOMERS ------------------

**/

CREATE TABLE customers_tra
    ( 
	 surr_id INTEGER auto_increment primary KEY,
     cust_id INTEGER NOT NULL , 
     cust_name VARCHAR(60)  NOT NULL , 
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
     cod_etl bigint
    )
;

/**

------------------ PRODUCTS ------------------

**/

CREATE TABLE products_tra
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
    cod_etl bigint
    )
;

/**

------------------ PROMOTIONS ------------------

**/

CREATE TABLE promotions_tra 
    ( 
     surr_id INTEGER auto_increment primary KEY,
     promo_id INTEGER NOT NULL , 
     promo_name VARCHAR(30)  NOT NULL , 
     promo_cost DECIMAL (10,2)  NOT NULL , 
     promo_begin_date DATE  NOT NULL , 
     promo_end_date DATE  NOT NULL , 
     cod_etl bigint 
    )
;

/**

------------------ SALES ------------------

**/

CREATE TABLE sales_tra
    ( 
    surr_id INTEGER auto_increment primary KEY,
	prod_id INTEGER NOT NULL , 
	cust_id INTEGER  NOT NULL , 
    time_id DATE  NOT NULL , 
    channel_id INTEGER  NOT NULL , 
    promo_id INTEGER  NOT NULL , 
    quantity_sold DECIMAL (10,2)  NOT NULL , 
    amount_sold DECIMAL (10,2)  NOT NULL , 
	cod_etl bigint
    ) 
;

/**

------------------ TIMES ------------------

**/

CREATE TABLE times_tra
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
     calendar_month_name VARCHAR(9) NOT NULL,
     calendar_quarter_desc CHAR (7)  NOT NULL , 
     calendar_year INTEGER  NOT NULL , 
     cod_etl bigint
    ) 
;
-- SELECT CHANNEL_ID,CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tra WHERE cod_etl = 1 ;
/**ALTER TABLE `cdnastg`.`channels_tra` 
ADD INDEX `ch_tra_cod_etl_idx` (`cod_etl` ASC) VISIBLE;
;
ALTER TABLE `cdnastg`.`channels_tra` 
ADD CONSTRAINT `ch_tra_cod_etl`
  FOREIGN KEY (`cod_etl`)
  REFERENCES `cdnastg`.`proc_etl` (`cod_etl`);
ALTER TABLE `cdnastg`.`times_tra` 
ADD INDEX `tim_tra_cod_etl_idx` (`cod_etl` ASC) VISIBLE;
;
ALTER TABLE `cdnastg`.`times_tra` 
ADD CONSTRAINT `tim_tra_cod_etl`
  FOREIGN KEY (`cod_etl`)
  REFERENCES `cdnastg`.`proc_etl` (`cod_etl`);
  ALTER TABLE `cdnastg`.`sales_tra` 
ADD INDEX `sal_tra_cod_etl_idx` (`cod_etl` ASC) VISIBLE;
;
ALTER TABLE `cdnastg`.`sales_tra` 
ADD CONSTRAINT `sal_tra_cod_etl`
  FOREIGN KEY (`cod_etl`)
  REFERENCES `cdnastg`.`proc_etl` (`cod_etl`);
ALTER TABLE `cdnastg`.`promotions_tra` 
ADD INDEX `prom_tra_cod_etl_idx` (`cod_etl` ASC) VISIBLE;
;
ALTER TABLE `cdnastg`.`promotions_tra` 
ADD CONSTRAINT `prom_tra_cod_etl`
  FOREIGN KEY (`cod_etl`)
  REFERENCES `cdnastg`.`proc_etl` (`cod_etl`);
  ALTER TABLE `cdnastg`.`products_tra` 
ADD INDEX `prod_tra_cod_etl_idx` (`cod_etl` ASC) VISIBLE;
;
ALTER TABLE `cdnastg`.`products_tra` 
ADD CONSTRAINT `prod_tra_cod_etl`
  FOREIGN KEY (`cod_etl`)
  REFERENCES `cdnastg`.`proc_etl` (`cod_etl`);
  ALTER TABLE `cdnastg`.`customers_tra` 
ADD INDEX `cus_tra_cod_etl_idx` (`cod_etl` ASC) VISIBLE;
;
ALTER TABLE `cdnastg`.`customers_tra` 
ADD CONSTRAINT `cus_tra_cod_etl`
  FOREIGN KEY (`cod_etl`)
  REFERENCES `cdnastg`.`proc_etl` (`cod_etl`);
  ALTER TABLE `cdnastg`.`countries_tra` 
ADD INDEX `co_tra_cod_etl_idx` (`cod_etl` ASC) VISIBLE;
;
ALTER TABLE `cdnastg`.`countries_tra` 
ADD CONSTRAINT `co_tra_cod_etl`
  FOREIGN KEY (`cod_etl`)
  REFERENCES `cdnastg`.`proc_etl` (`cod_etl`);