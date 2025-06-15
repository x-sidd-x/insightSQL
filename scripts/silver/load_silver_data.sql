create or replace procedure silver.load_silver()
language plpgsql
as
$$
declare 
	start_time timestamp;
	end_time timestamp;
	silver_load_start timestamp;
	silver_load_end timestamp;
begin

silver_load_start:= Now();

raise notice 'Loading data to silver.cust_info';
start_time:= Now();
--------------------------------------------------
truncate table silver.crm_cust_info;
insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
select
	cst_id,
	cst_key,
	trim(cst_firstname) as cst_firstname,
	trim(cst_lastname) as cst_lastname,
	case upper(trim(cst_marital_status))
		when 'S' then 'Single'
		when 'M' then 'Married'
		else 'n/a'
	end as cst_marital_status,
	case upper(trim(cst_gndr))
		when 'M' then 'Male'
		when 'F' then 'Female'
		else 'n/a'
	end as cst_gndr,
	cst_create_date
from
(select 
	*, 
	row_number() over(partition by cst_id order by cst_create_date desc) as latest
from bronze.crm_cust_info
where cst_id is not null)
where latest=1;
-----------------------------------------------------------
end_time:= Now();
raise notice 'silver.cust_info loaded in % seconds', extract(epoch from end_time-start_time);

raise notice '-----------------------------------------------------------';
raise notice '------------------------------------------------------------';

raise notice 'Loading data to silver.crm_prd_info';
start_time:= Now();
-------------------------------------------
truncate table silver.crm_prd_info;
insert into silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_name,
prd_cost,
prd_line,
prd_start_date,
prd_end_date
)
select 
	prd_id,
	replace(substring(prd_key, 1, 5),'-','_') as cat_id,
	substring(prd_key, 7, length(prd_key)) as prd_key,
	prd_name,
	coalesce(prd_cost,0) as prd_cost,
	case upper(trim(prd_line))
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'n/a'
	end as prd_line,
	cast(prd_start_date as date) as prd_start_date,
	cast(lead(prd_start_date) over(partition by prd_key order by prd_start_date) - interval '1 day' as date) as prd_end_date
from bronze.crm_prd_info;
-----------------------------------------------------------------------
end_time:= Now();
raise notice 'silver.crm_prd_info loaded in % seconds', extract(epoch from end_time-start_time);

raise notice '-----------------------------------------------------------';
raise notice '------------------------------------------------------------';

raise notice 'Loading data to silver.crm_sales_details';
start_time:= Now();
------------------------------------------------------------------------
truncate table silver.crm_sales_details;
insert into silver.crm_sales_details(
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
)
select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case 
		when length(cast(sls_order_dt as text))<8 then null
		else cast(cast(nullif(sls_order_dt,0) as text) as date)
	end sls_order_dt,
	case 
		when length(cast(sls_ship_dt as text))<8 then null
		else cast(cast(nullif(sls_ship_dt,0) as text) as date)
	end sls_ship_dt,
	case 
		when length(cast(sls_due_dt as text))<8 then null
		else cast(cast(nullif(sls_due_dt,0) as text) as date)
	end sls_due_dt,
	CASE
		WHEN sls_sales IS NULL OR sls_sales = 0 OR sls_sales != sls_quantity * sls_price
		  AND sls_quantity IS NOT NULL AND sls_quantity != 0
		  AND sls_price IS NOT NULL AND sls_price != 0
		THEN sls_price * abs(sls_quantity)
		ELSE sls_sales
	END AS sls_sales,
	CASE
		WHEN (sls_quantity IS NULL OR sls_quantity = 0)
		  AND sls_price IS NOT NULL AND sls_price != 0
		  AND sls_sales IS NOT NULL AND sls_sales != 0
		THEN abs(sls_sales) / abs(sls_price)
		ELSE sls_quantity
	END AS sls_quantity,
	CASE
		WHEN (sls_price IS NULL OR sls_price <= 0)
		  AND sls_quantity IS NOT NULL AND sls_quantity != 0
		  AND sls_sales IS NOT NULL AND sls_sales != 0
		THEN abs(sls_sales) / abs(sls_quantity)
		ELSE abs(sls_price)
	END AS sls_price
from bronze.crm_sales_details
where 
  sls_sales IS NOT NULL AND sls_sales >= 0 AND sls_sales = sls_quantity*sls_price AND
  sls_quantity IS NOT NULL AND sls_quantity >= 0 AND
  sls_price IS NOT NULL AND sls_price >= 0;
------------------------------------------------------------------------
end_time:= Now();
raise notice 'silver.crm_sales_details loaded in % seconds', extract(epoch from end_time-start_time);

raise notice '-----------------------------------------------------------';
raise notice '------------------------------------------------------------';

raise notice 'Loading data to silver.erp_cust_az12';
start_time:= Now();
------------------------------------------------------------------------
truncate table silver.erp_cust_az12;
insert into silver.erp_cust_az12(
cid,
bdate,
gen
)
select
		case
			when cid like 'NAS%' then substring(cid, 4, length(cid))
			else cid
		end as cid,
		case 
			when bdate > current_date then NULL
			else bdate
		end as bdate,
		case
			when nullif(trim(gen),'') is null then 'n/a'
			when trim(upper(gen)) = 'M' then 'Male'
			when trim(upper(gen)) = 'F' then 'Female'
			else gen
		end as gen
from bronze.erp_cust_az12;
--------------------------------------------------------------------------------------
end_time:= Now();
raise notice 'silver.erp_cust_az12 loaded in % seconds', extract(epoch from end_time-start_time);

raise notice '-----------------------------------------------------------';
raise notice '------------------------------------------------------------';

raise notice 'Loading data to silver.erp_loc_a101';
start_time:= Now();
------------------------------------------------------------------------
truncate table silver.erp_loc_a101;
insert into silver.erp_loc_a101(
cid,
cntry
)
select
    replace(cid, '-', '') as cid,
	case
		when nullif(trim(cntry),'') is null then 'n/a'
		when lower(trim(cntry)) in ('usa', 'us', 'united states', 'america') then 'United States'
		when lower(trim(cntry)) = 'de' then 'Germany'
		else trim(cntry)
	end as cntry
from bronze.erp_loc_a101;
--------------------------------------------------------------------------
end_time:= Now();
raise notice 'silver.erp_loc_a101 loaded in % seconds', extract(epoch from end_time-start_time);

raise notice '-----------------------------------------------------------';
raise notice '------------------------------------------------------------';

raise notice 'Loading data to silver.erp_px_cat_g1v2';
start_time:= Now();
------------------------------------------------------------------------
--Data is already 100% clean so directly loading it to the silver layer

truncate table silver.erp_px_cat_g1v2;
insert into silver.erp_px_cat_g1v2(
id,
cat,
subcat,
maintenance
)
select * from bronze.erp_px_cat_g1v2;
-------------------------------------------------------------------------
end_time:= Now();
raise notice 'silver.erp_px_cat_g1v2 loaded in % seconds', extract(epoch from end_time-start_time);

silver_load_end:= Now();

raise notice '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\|//////////////////////////////////';
raise notice '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\|//////////////////////////////////';
raise notice 'Silver Layer loading done in % seconds', extract(epoch from silver_load_end - silver_load_start);
raise notice '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\|//////////////////////////////////';
raise notice '\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\|//////////////////////////////////';
end;
$$;

call silver.load_silver()
