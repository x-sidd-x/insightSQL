create or replace view gold.dim_customer as
(
	select 
		row_number() over(order by ci.cst_id) as customer_key,
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as firstname,
		ci.cst_lastname as lastname,
		ci.cst_marital_status as marital_status,
		case
			when ci.cst_gndr != 'n/a' then ci.cst_gndr
			when ca.gen is null then ci.cst_gndr
			else ca.gen
		end as gender,	
		ci.dwh_create_date as create_date,
		ca.bdate as bithdate,
		la.cntry as country
	from silver.crm_cust_info as ci
	left join silver.erp_cust_az12 as ca on ci.cst_key = ca.cid
	left join silver.erp_loc_a101 as la on ci.cst_key = la.cid
)

-----------------------------------------------------------------------------------

create or replace view gold.dim_product as
(
	select
		row_number() over(order by start_date, product_number) as product_key,
		product_id,
		product_number,
		name,
		cost,
		line,
		category_id,
		category,
		subcategory,
		start_date,
		end_date,
		maintenance
	from
	(
		select
			row_number() over(partition by pi.cat_id, pi.prd_key order by pi.prd_start_date desc) as ranking,
			pi.prd_id as product_id,
			pi.cat_id as category_id,
			pi.prd_key as product_number,
			pi.prd_name as name,
			pi.prd_cost as cost,
			pi.prd_line as line,
			pi.prd_start_date as start_date,
			pi.prd_end_date as end_date,
			pcg.cat as category,
			pcg.subcat as subcategory,
			pcg.maintenance as maintenance
		from silver.crm_prd_info as pi
		left join silver.erp_px_cat_g1v2 as pcg on pi.cat_id = pcg.id
	)
	where ranking = 1
)

-----------------------------------------------------------------------------------

create or replace view gold.fact_sales as
(
	select 
		sd.sls_ord_num as order_number,
		dp.product_key,
		dc.customer_key,
		sd.sls_order_dt as order_date,
		sd.sls_ship_dt as ship_date,
		sd.sls_due_dt as due_date,
		sd.sls_sales as sales,
		sd.sls_quantity as qunatity,
		sd.sls_price as price
	from silver.crm_sales_details as sd
	left join gold.dim_product as dp
	on sd.sls_prd_key = dp.product_number
	left join gold.dim_customer as dc
	on sd.sls_cust_id = dc.customer_id
)

