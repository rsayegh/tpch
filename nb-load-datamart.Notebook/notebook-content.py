# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7da7e727-e309-41d2-b48a-819835ce9a5d",
# META       "default_lakehouse_name": "tpchlake",
# META       "default_lakehouse_workspace_id": "16e021f2-180b-4bb2-9e48-32981250ce0a",
# META       "known_lakehouses": [
# META         {
# META           "id": "7da7e727-e309-41d2-b48a-819835ce9a5d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# v-oder optimization
spark.conf.set("sprk.sql.parquet.vorder.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.binSize", "1073741824")

# Dynamic Partition Overwrite to avoid deleting existing partitions
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")

# arrow enablement
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")

# MARKDOWN ********************

# %%sql 
# 
# /* dbo.DimCustomer */
# DROP TABLE IF EXISTS tpchlake.DimCustomer;
# 
# -- CREATE TABLE tpchlake.DimCustomer(
# -- 	 CustomerKey int NOT NULL
# -- 	,CustomerCode int NOT NULL
# -- 	,CustomerName varchar(100) NOT NULL
# -- 	,CustomerAddress varchar(100) NOT NULL
# -- 	,CustomerPhone varchar(20) NOT NULL
# -- 	,CustomerBalance decimal(15, 2) NOT NULL
# -- 	,CustomerSegment varchar(50) NOT NULL
# -- 	,CustomerComment varchar(200) NOT NULL
# -- 	,CustomerNation varchar(100) NOT NULL
# -- 	,CustomerRegion varchar(100) NOT NULL
# -- 	-- ,CreateDatetime datetime NOT NULL
# -- 	-- ,ChangeDatetime datetime NOT NULL
# -- 	,LoadId int NOT NULL
# -- ) 
# -- USING DELTA;
# 
# 
# /* dbo.DimSupplier */
# DROP TABLE IF EXISTS tpchlake.DimSupplier;
# 
# -- CREATE TABLE tpchlake.DimSupplier(
# -- 	 SupplierKey int NOT NULL
# -- 	,SupplierCode int NOT NULL
# -- 	,SupplierName varchar(100) NOT NULL
# -- 	,SupplierAddress varchar(100) NOT NULL
# -- 	,SupplierPhone varchar(20) NOT NULL
# -- 	,SupplierBalance decimal(15, 2) NOT NULL
# -- 	,SupplierComment varchar(200) NOT NULL
# -- 	,SupplierNation varchar(100) NOT NULL
# -- 	,SupplierRegion varchar(100) NOT NULL
# -- 	-- ,CreateDatetime datetime NOT NULL
# -- 	-- ,ChangeDatetime datetime NOT NULL
# -- 	,LoadId int NOT NULL
# -- ) 
# -- USING DELTA;
# 
# /* dbo.DimDate */
# DROP TABLE IF EXISTS tpchlake.DimDate;
# 
# -- CREATE TABLE tpchlake.DimDate(
# -- 	 DateKey INT NOT NULL
# -- 	,Date DATE NOT NULL
# -- 	,DayName VARCHAR(10) NOT NULL
# -- 	,DayOfMonth SMALLINT NOT NULL
# -- 	,DayOfYear SMALLINT NOT NULL
# -- 	,Month  SMALLINT NOT NULL
# -- 	,MonthName VARCHAR(10) NOT NULL
# -- 	,MonthOfQuarter SMALLINT NOT NULL
# -- 	,MonthYear INT NOT NULL
# -- 	,Quarter SMALLINT NOT NULL
# -- 	,QuarterName VARCHAR(2) NOT NULL
# -- 	,Year SMALLINT NOT NULL
# -- 	,YearName VARCHAR(10) NOT NULL
# -- 	-- ,CreateDatetime datetime NOT NULL
# -- 	-- ,ChangeDatetime datetime NOT NULL
# -- 	,LoadId INT NOT NULL
# -- )
# -- USING DELTA;
# 
# 
# /* dbo.DimSuppliedPart */
# DROP TABLE IF EXISTS tpchlake.DimSuppliedPart;
# 
# -- CREATE TABLE tpchlake.DimSuppliedPart(
# -- 	 SuppliedPartKey INT NOT NULL
# -- 	,PartCode INT NOT NULL
# -- 	,SupplierCode INT NOT NULL
# -- 	,SuppliedPartQuantity INT NOT NULL
# -- 	,SuppliedPartCost decimal(15, 2) NOT NULL
# -- 	,SuppliedPartComment varchar(200) NOT NULL
# -- 	,PartName varchar(100) NOT NULL
# -- 	,PartManufacturer varchar(100) NOT NULL
# -- 	,PartBrand varchar(100) NOT NULL
# -- 	,PartType varchar(100) NOT NULL
# -- 	,PartSize INT NOT NULL
# -- 	,PartContainer varchar(100) NOT NULL
# -- 	,PartRetailPrice decimal(15, 2) NOT NULL
# -- 	,PartComment varchar(200) NOT NULL
# -- 	-- ,CreateDatetime datetime NOT NULL
# -- 	-- ,ChangeDatetime datetime NOT NULL
# -- 	,LoadId INT NOT NULL
# -- )
# -- USING DELTA;
# 
# /* dbo.FactOrderLines */
# DROP TABLE IF EXISTS tpchlake.FactOrderLines;
# 
# -- CREATE TABLE tpchlake.FactOrderLines(
# -- 	CommitDateKey int NOT NULL,
# -- 	CustomerKey int NOT NULL,
# -- 	OrderDateKey int NOT NULL,
# -- 	ReceiptDateKey int NOT NULL,
# -- 	ShipDateKey int NOT NULL,
# -- 	SuppliedPartKey int NOT NULL,
# -- 	SupplierKey int NOT NULL,
# -- 	OrderCode int NOT NULL,
# -- 	LineNumber int NOT NULL,
# -- 	Quantity decimal(15, 2) NOT NULL,
# -- 	ExtendedPrice decimal(15, 2) NOT NULL,
# -- 	Discount decimal(15, 2) NOT NULL,
# -- 	Tax decimal(15, 2) NOT NULL,
# -- 	ReturnFlag varchar(50) NOT NULL,
# -- 	LineStatus varchar(50) NOT NULL,
# -- 	ShipInstruct varchar(50) NOT NULL,
# -- 	ShipMode varchar(50) NOT NULL,
# -- 	LineItemComment varchar(200) NOT NULL,
# -- 	OrderStatus varchar(50) NOT NULL,
# -- 	OrderTotalPrice decimal(15, 2) NOT NULL,
# -- 	OrderPriority varchar(50) NOT NULL,
# -- 	OrderClerk varchar(50) NOT NULL,
# -- 	OrderShipPriority varchar(50) NOT NULL,
# -- 	OrderComment varchar(200) NOT NULL,
# -- 	LoadId int NOT NULL
# -- ) 
# -- USING DELTA;
# 


# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC CREATE TABLE tpchlake.dim_customer
# MAGIC AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER(ORDER BY c.c_custkey) AS CustomerKey
# MAGIC     ,c.c_custkey AS CustomerCode
# MAGIC     ,c.c_name AS CustomerName
# MAGIC     ,c.c_address AS CustomerAddress
# MAGIC     ,c.c_phone AS CustomerPhone
# MAGIC     ,c.c_acctbal AS CustomerBalance
# MAGIC     ,c.c_mktsegment AS CustomerSegment
# MAGIC     ,c.c_comment AS CustomerComment
# MAGIC     ,ifnull(n.n_name, 'N/A') AS CustomerNation
# MAGIC     ,ifnull(r.r_name, 'N/A') AS CustomerRegion
# MAGIC     ,1 as LoadId
# MAGIC FROM 
# MAGIC     tpchlake.customer c
# MAGIC     LEFT JOIN tpchlake.nation n 
# MAGIC         ON c.c_nationkey = n.n_nationkey
# MAGIC     LEFT JOIN tpchlake.region r
# MAGIC         ON n.n_regionkey = r.r_regionkey;

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS tpchlake.dim_supplied_part
# MAGIC CREATE TABLE tpchlake.dim_supplied_part
# MAGIC AS
# MAGIC SELECT 
# MAGIC      ROW_NUMBER() OVER(ORDER BY partsupp.ps_partkey, partsupp.ps_suppkey) AS SuppliedPartKey
# MAGIC     ,partsupp.ps_partkey AS PartCode
# MAGIC     ,partsupp.ps_suppkey AS SupplierCode
# MAGIC     ,partsupp.ps_availqty AS SuppliedPartQuantity
# MAGIC     ,partsupp.ps_supplycost AS SuppliedPartCost
# MAGIC     ,partsupp.ps_comment AS SuppliedPartComment
# MAGIC     ,part.p_name AS PartName
# MAGIC     ,part.p_mfgr AS PartManufacturer
# MAGIC     ,part.p_brand AS PartBrand 
# MAGIC     ,part.p_type AS PartType
# MAGIC     ,part.p_size AS PartSize
# MAGIC     ,part.p_container AS PartContainer
# MAGIC     ,part.p_retailprice AS PartRetailPrice
# MAGIC     ,part.p_comment AS PartComment
# MAGIC     ,1 as LoadId
# MAGIC FROM 
# MAGIC     tpchlake.partsupp partsupp
# MAGIC     INNER JOIN tpchlake.part part
# MAGIC         ON partsupp.ps_partkey = part.p_partkey

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS tpchlake.dim_supplier
# MAGIC CREATE TABLE tpchlake.dim_supplier
# MAGIC AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER(ORDER BY s.s_suppkey) AS SupplierKey
# MAGIC     ,s.s_suppkey AS SupplierCode
# MAGIC     ,s.s_name AS SupplierName
# MAGIC     ,s.s_address AS SupplierAddress
# MAGIC     ,s.s_phone AS SupplierPhone
# MAGIC     ,s.s_acctbal AS SupplierBalance
# MAGIC     ,s.s_comment AS SupplierComment
# MAGIC     ,ifnull(n.n_name, 'N/A') AS SupplierNation
# MAGIC     ,ifnull(r.r_name, 'N/A') AS SupplierRegion
# MAGIC     ,1 as LoadId
# MAGIC FROM 
# MAGIC     tpchlake.supplier s
# MAGIC     LEFT JOIN tpchlake.nation n 
# MAGIC         ON s.s_nationkey = n.n_nationkey
# MAGIC     LEFT JOIN tpchlake.region r
# MAGIC         ON n.n_regionkey = r.r_regionkey

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC DROP TABLE IF EXISTS tpchlake.fact_order_lines;
# MAGIC CREATE TABLE tpchlake.fact_order_lines
# MAGIC AS
# MAGIC SELECT
# MAGIC     /* surrogate keys */
# MAGIC      DATE_FORMAT(li.l_commitdate, 'yyyyMMdd') AS CommitDateKey
# MAGIC     ,ifnull(c.CustomerKey, -1) AS CustomerKey
# MAGIC     ,DATE_FORMAT(o.o_orderdate, 'yyyyMMdd') AS OrderDateKey
# MAGIC     ,DATE_FORMAT(li.l_receiptdate, 'yyyyMMdd') AS ReceiptDateKey
# MAGIC     ,DATE_FORMAT(li.l_shipdate, 'yyyyMMdd') AS ShipDateKey
# MAGIC     ,ifnull(sp.SuppliedPartKey, -1) AS SuppliedPartKey
# MAGIC     ,ifnull(s.SupplierKey, -1) AS SupplierKey
# MAGIC     /* metrics and properties */
# MAGIC     ,ifnull(o.o_orderkey, -1) AS OrderCode
# MAGIC     ,li.l_linenumber AS LineNumber
# MAGIC     ,li.l_quantity AS Quantity
# MAGIC     ,li.l_extendedprice AS ExtendedPrice
# MAGIC     ,li.l_discount AS Discount
# MAGIC     ,li.l_tax AS Tax
# MAGIC     ,CAST(li.l_returnflag AS VARCHAR(50)) AS ReturnFlag
# MAGIC     ,CAST(li.l_linestatus AS VARCHAR(50)) AS LineStatus
# MAGIC     ,CAST(li.l_shipinstruct AS VARCHAR(50)) AS ShipInstruct
# MAGIC     ,CAST(li.l_shipmode AS VARCHAR(50)) AS ShipMode
# MAGIC     ,CAST(li.l_comment AS VARCHAR(200)) AS LineItemComment
# MAGIC     ,CAST(o.o_orderstatus AS VARCHAR(50)) AS OrderStatus
# MAGIC     ,o.o_totalprice AS OrderTotalPrice
# MAGIC     ,CAST(o.o_orderpriority AS VARCHAR(50)) AS OrderPriority
# MAGIC     ,CAST(o.o_clerk AS VARCHAR(50)) AS OrderClerk
# MAGIC     ,CAST(o_shippriority AS VARCHAR(50)) AS OrderShipPriority
# MAGIC     ,CAST(o.o_comment AS VARCHAR(200)) AS OrderComment
# MAGIC     ,1 AS LoadId
# MAGIC FROM 
# MAGIC     tpchlake.lineitem li
# MAGIC     LEFT JOIN tpchlake.orders o
# MAGIC         ON o.o_orderkey = li.l_orderkey
# MAGIC     LEFT JOIN tpchlake.dim_customer c
# MAGIC         ON c.CustomerCode = o.o_custkey
# MAGIC     LEFT JOIN tpchlake.dim_supplied_part sp
# MAGIC         ON sp.PartCode = li.l_partkey AND sp.SupplierCode = li.l_suppkey 
# MAGIC     LEFT JOIN tpchlake.dim_supplier s
# MAGIC         ON s.SupplierCode = li.l_suppkey
# MAGIC ;

# METADATA ********************

# META {
# META   "language": "sparksql"
# META }

# CELL ********************

