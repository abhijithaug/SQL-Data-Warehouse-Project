--==============================================
-- Building  [crm_cust_info] in the gold layer
--==============================================

SELECT
		ci.cst_id,
		ci.cst_key ,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry
	FROM  [DataWarehouse27022025].[Silver].[crm_cust_info] ci
	LEFT JOIN [DataWarehouse27022025].[Silver].[erp_cust_az12] ca
		ON ci.cst_key = ca.cid
	LEFT JOIN [DataWarehouse27022025].[Silver].[erp_loc_a101] la
		ON ci.cst_key = la.cid
-- have joined all the three tables and we have picked all the columns that we want in this object 


-- WE HAVE TWO COLUMNS FOR GENDER
-- Data Integration is required here
-- Check for duplicates and multiple values
SELECT DISTINCT
		ci.cst_gndr,
		ca.gen,
		CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr -- Assuming CRM table is the master for gender
			ELSE COALESCE(ca.gen, 'n/a')
		END AS new_gen
	FROM  [DataWarehouse27022025].[Silver].[crm_cust_info] ci
	LEFT JOIN [DataWarehouse27022025].[Silver].[erp_cust_az12] ca
		ON ci.cst_key = ca.cid
	LEFT JOIN [DataWarehouse27022025].[Silver].[erp_loc_a101] la
		ON ci.cst_key = la.cid
	ORDER BY 1,2
-- here we are integrating two different Source system in one. This is called data integration. 
--Considering the master data as crm table, doing appropriate adjustments


-- Final SQL Querry
-- Followig Proper namings to columns
-- Rearranging the column positions to improve readability
-- This is a dimension table
-- Create a primary Key
-- A surrogate key (pseudokey) in a database is a unique identifier for either an entity in the modeled world or an object in the database

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER () OVER (ORDER BY cst_id) AS Customer_Key,
		ci.cst_id AS Customer_Id,
		ci.cst_key AS Customer_Number,
		ci.cst_firstname AS First_Name,
		ci.cst_lastname AS Last_Name,
		la.cntry AS Country,
		ci.cst_marital_status AS Marital_Status,
		CASE WHEN ci.cst_gndr!='n/a' THEN ci.cst_gndr -- Assuming CRM table is the master for gender
			ELSE COALESCE(ca.gen, 'n/a')
		END AS Gender,
		ci.cst_create_date AS Create_Date,
		ca.bdate AS Birth_Date
		FROM  [DataWarehouse27022025].[Silver].[crm_cust_info] ci
	LEFT JOIN [DataWarehouse27022025].[Silver].[erp_cust_az12] ca
		ON ci.cst_key = ca.cid
	LEFT JOIN [DataWarehouse27022025].[Silver].[erp_loc_a101] la
		ON ci.cst_key = la.cid
-- We took three three tables and put it in one object




SELECT * FROM gold.dim_customers


--==============================================
-- Building  [crm_prd_info] in the gold layer
--==============================================
-- Building  [crm_prd_info] in the gold layer
-- Create Dimension Products


SELECT	  pn.prd_id,
		  pn.cat_id,
		  pn.prd_key,
		  pn.prd_nm,
		  pn.prd_cost,
		  pn.prd_line,
		  pn.prd_start_dt,
		  pc.cat,
		  pc.subcat,
		  pc.maintenance
		  --pn.prd_end_dt
  FROM [DataWarehouse27022025].[Silver].[crm_prd_info] pn
	LEFT JOIN [DataWarehouse27022025].[Silver].[erp_px_cat_g1v2] pc
	ON pn.cat_id = pc.id
  WHERE prd_end_dt IS NULL --  Filter out allhistorical data
-- Considering the product end date is NULL meaning it "it is not cosed yet"


-- Step 1 
-- Check the data quality
-- check the uniqueness if prd_key
SELECT prd_key, COUNT(*) FROM(
SELECT	  pn.prd_id,
		  pn.cat_id,
		  pn.prd_key,
		  pn.prd_nm,
		  pn.prd_cost,
		  pn.prd_line,
		  pn.prd_start_dt,
		  pc.cat,
		  pc.subcat,
		  pc.maintenance
  FROM [DataWarehouse27022025].[Silver].[crm_prd_info] pn
	LEFT JOIN [DataWarehouse27022025].[Silver].[erp_px_cat_g1v2] pc
	ON pn.cat_id = pc.id
  WHERE prd_end_dt IS NULL
  )t
  GROUP BY prd_key
  HAVING COUNT(*) >1
  -- Result: we dont have any duplicates in prd_key
  -- Check for other columns also.


  -- Step2: Filter all historical data and rearranging column positions
  -- Reane Columns with understandable names.
  -- Here each row is exactly describing one object/ products. So This is a Dimension Table
  -- Create a Primary Key (ie. product_key)

  CREATE VIEW gold.dim_productsA AS
  SELECT
			ROW_NUMBER () OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS Product_key,
			pn.prd_id AS Product_Id,
			pn.prd_key AS Product_Number,
			pn.prd_nm AS Product_Name,
			pc.cat AS Category,
			pn.cat_id AS Category_Id,
			pc.subcat AS Sub_Category,
			pc.maintenance AS Maintenance,
			pn.prd_cost AS Product_Cost,
			pn.prd_line AS Product_Line,
			pn.prd_start_dt AS Start_Date
			FROM [DataWarehouse27022025].[Silver].[crm_prd_info] pn
	LEFT JOIN [DataWarehouse27022025].[Silver].[erp_px_cat_g1v2] pc
	ON pn.cat_id = pc.id
  WHERE prd_end_dt IS NULL
  


  SELECT * FROM gold.dim_productsA


--==============================================
-- Building  [crm_sales_details] in the gold layer
--==============================================
 -- Building [crm_sales_details]  in the gold layer(Fact Sales)
 -- This is a fact table
 -- Sort colums to improve readability
 SELECT 
		  sd.sls_ord_num AS Order_Number,
		  pr.product_key AS Product_Key,
		  cu.customer_key AS Customer_Key,
		  -- sd.sls_prd_key, --the product key and the customer ID comes from the dim table
		  -- sd.sls_cust_id, -- eplace those two informations from dim tables using JOIN 
		  sd.sls_order_dt AS Order_Date,
		  sd.sls_ship_dt AS Shipping_Date,
		  sd.sls_due_dt AS Due_Date,
		  sd.sls_sales AS Sales_Amount,
		  sd.sls_quantity AS Quantity,
		  sd.sls_price AS Price
  FROM [DataWarehouse27022025].[Silver].[crm_sales_details] sd
	LEFT JOIN gold.dim_productsA pr
	ON sd.sls_prd_key = pr.Product_Number
	LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id


-- Step 1
 -- Sort colums to improve readability
 --Final SQL for this table
 CREATE VIEW gold.fact_sales AS
 SELECT 
		  sd.sls_ord_num AS Order_Number,
		  pr.product_key AS Product_Key,
		  cu.customer_key AS Customer_Key,
		  sd.sls_order_dt AS Order_Date,
		  sd.sls_ship_dt AS Shipping_Date,
		  sd.sls_due_dt AS Due_Date,
		  sd.sls_sales AS Sales_Amount,
		  sd.sls_quantity AS Quantity,
		  sd.sls_price AS Price
  FROM [DataWarehouse27022025].[Silver].[crm_sales_details] sd
	LEFT JOIN gold.dim_productsA pr
	ON sd.sls_prd_key = pr.Product_Number
	LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id




SELECT * FROM gold.fact_sales



-- FACT CHECK
-- Foreign Key Integrity(Dim Table)
SELECT * 
	FROM gold.fact_sales f
	LEFT JOIN gold.dim_customers c
	ON c.Customer_Key = f.Customer_Key
	WHERE c.Customer_Key IS NULL
