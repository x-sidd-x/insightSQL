CREATE OR REPLACE PROCEDURE load_bronze()
LANGUAGE PLPGSQL
AS
$$
BEGIN
	RAISE NOTICE 'TRUNCATING CRM_CUST_INFO';
    RAISE NOTICE '-----------------------------------------------';

	TRUNCATE TABLE CRM_CUST_INFO;

    RAISE NOTICE 'LOADING DATA TO CRM_CUST_INFO';
    RAISE NOTICE '-----------------------------------------------';

	COPY CRM_CUST_INFO
	FROM 'C:\Program Files\PostgreSQL\17\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FORMAT CSV,
		HEADER TRUE,
		DELIMITER ','
	);

	RAISE NOTICE 'DATA LOADED TO CRM_CUST_INFO';
    RAISE NOTICE '-----------------------------------------------';

	RAISE NOTICE 'TRUNCATING PRD_INFO';
    RAISE NOTICE '-----------------------------------------------';
	
	TRUNCATE TABLE CRM_PRD_INFO;

	RAISE NOTICE 'LOADING DATA TO CRM_PRD_INFO';
    RAISE NOTICE '-----------------------------------------------';

	COPY CRM_PRD_INFO
	FROM 'C:\Program Files\PostgreSQL\17\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FORMAT CSV,
		HEADER TRUE,
		DELIMITER ','
	);

	RAISE NOTICE 'DATA LOADED TO CRM_PRD_INFO';
    RAISE NOTICE '-----------------------------------------------';

	RAISE NOTICE 'TRUNCATING CRM_SALES_DETAILS';
    RAISE NOTICE '-----------------------------------------------';
	
	TRUNCATE TABLE CRM_SALES_DETAILS;

	RAISE NOTICE 'LOADING DATA TO CRM_SALES_DETAILS';
    RAISE NOTICE '-----------------------------------------------';

	COPY CRM_SALES_DETAILS
	FROM 'C:\Program Files\PostgreSQL\17\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FORMAT CSV,
		HEADER TRUE,
		DELIMITER ','
	);

	RAISE NOTICE 'DATA LOADED TO CRM_SALES_DETAILS';
    RAISE NOTICE '-----------------------------------------------';


    RAISE NOTICE '------------------------------------------------------------------';
    RAISE NOTICE '------------------------------------------------------------------';

    RAISE NOTICE 'TRUNCATING ERP_CUST_AZ12';
    RAISE NOTICE '-----------------------------------------------';
	
	TRUNCATE TABLE ERP_CUST_AZ12;

	RAISE NOTICE 'LOADING DATA TO ERP_CUST_AZ12';
    RAISE NOTICE '-----------------------------------------------';
	
	COPY ERP_CUST_AZ12
	FROM 'C:\Program Files\PostgreSQL\17\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FORMAT CSV,
		HEADER TRUE,
		DELIMITER ','
	);

	RAISE NOTICE 'DATA LOADED TO ERP_CUST_AZ12';
    RAISE NOTICE '-----------------------------------------------';

    RAISE NOTICE 'TRUNCATING ERP_LOC_A101';
    RAISE NOTICE '-----------------------------------------------';
	
	TRUNCATE TABLE ERP_LOC_A101;

    RAISE NOTICE 'LOADING DATA TO ERP_LOC_A101';
    RAISE NOTICE '-----------------------------------------------';
	
	COPY ERP_LOC_A101
	FROM 'C:\Program Files\PostgreSQL\17\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FORMAT CSV,
		HEADER TRUE,
		DELIMITER ','
	);

	RAISE NOTICE 'DATA LOADED TO ERP_LOC_A101';
    RAISE NOTICE '-----------------------------------------------';
	
 	RAISE NOTICE 'TRUNCATING ERP_PX_CAT_G1V2';
    RAISE NOTICE '-----------------------------------------------';
	
	TRUNCATE TABLE ERP_PX_CAT_G1V2;

	RAISE NOTICE 'LOADING DATA TO ERP_PX_CAT_G1V2';
    RAISE NOTICE '-----------------------------------------------';
	
	COPY ERP_PX_CAT_G1V2
	FROM 'C:\Program Files\PostgreSQL\17\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FORMAT CSV,
		HEADER TRUE,
		DELIMITER ','
	);

	RAISE NOTICE 'DATA LOADED TO ERP_PX_CAT_G1V2';
    RAISE NOTICE '-----------------------------------------------';
END;
$$;

CALL load_bronze()
