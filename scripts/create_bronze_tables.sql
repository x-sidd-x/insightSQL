SET SEARCH_PATH TO BRONZE;

CREATE TABLE crm_cust_info IF NOT EXISTS(
cst_id int,
cst_key varchar(30),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(10),
cst_gndr varchar(10),
cst_create_date date
);

CREATE TABLE crm_prd_info IF NOT EXISTS(
prd_id int,
prd_key varchar(50),
prd_name varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_date timestamp,
prd_end_date timestamp
);

CREATE TABLE crm_sales_details IF NOT EXISTS(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);

CREATE TABLE erp_cust_az12 IF NOT EXISTS(
cid varchar(50),
bdate date,
gen varchar(50)
);

CREATE TABLE erp_loc_a101 IF NOT EXISTS(
cid varchar(50),
cntry varchar(50)
);

CREATE TABLE erp_px_cat_g1v2 IF NOT EXISTS(
id varchar(50),
cat varchar(50),
subcat varchar(50),
maintenance varchar(50)
);
