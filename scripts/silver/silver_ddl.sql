set search_path to silver
  
CREATE TABLE IF NOT EXISTS crm_cust_info(
cst_id int,
cst_key varchar(30),
cst_firstname varchar(50),
cst_lastname varchar(50),
cst_marital_status varchar(10),
cst_gndr varchar(10),
cst_create_date date,
dwh_create_date timestamp default now()
);

CREATE TABLE IF NOT EXISTS crm_prd_info(
prd_id int,
cat_id varchar(20),
prd_key varchar(50),
prd_name varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_date date,
prd_end_date date,
dwh_create_date timestamp default now()
);

CREATE TABLE  IF NOT EXISTS crm_sales_details(
sls_ord_num varchar(50),
sls_prd_key varchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date timestamp default now()
);

CREATE TABLE IF NOT EXISTS erp_cust_az12(
cid varchar(50),
bdate date,
gen varchar(50),
dwh_create_date timestamp default now()
);

CREATE TABLE  IF NOT EXISTS erp_loc_a101(
cid varchar(50),
cntry varchar(50),
dwh_create_date timestamp default now()
);

CREATE TABLE IF NOT EXISTS erp_px_cat_g1v2(
id varchar(50),
cat varchar(50),
subcat varchar(50),
maintenance varchar(50),
dwh_create_date timestamp default now()
);
