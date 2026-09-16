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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql 
# MAGIC 
# MAGIC /* dbo.DimCustomer */
# MAGIC DROP TABLE IF EXISTS tpchlake.DimCustomer;
# MAGIC 
# MAGIC -- CREATE TABLE tpchlake.DimCustomer(
# MAGIC -- 	 CustomerKey int NOT NULL
# MAGIC -- 	,CustomerCode int NOT NULL
# MAGIC -- 	,CustomerName varchar(100) NOT NULL
# MAGIC -- 	,CustomerAddress varchar(100) NOT NULL
# MAGIC -- 	,CustomerPhone varchar(20) NOT NULL
# MAGIC -- 	,CustomerBalance decimal(15, 2) NOT NULL
# MAGIC -- 	,CustomerSegment varchar(50) NOT NULL
# MAGIC -- 	,CustomerComment varchar(200) NOT NULL
# MAGIC -- 	,CustomerNation varchar(100) NOT NULL
# MAGIC -- 	,CustomerRegion varchar(100) NOT NULL
# MAGIC -- 	-- ,CreateDatetime datetime NOT NULL
# MAGIC -- 	-- ,ChangeDatetime datetime NOT NULL
# MAGIC -- 	,LoadId int NOT NULL
# MAGIC -- ) 
# MAGIC -- USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC /* dbo.DimSupplier */
# MAGIC DROP TABLE IF EXISTS tpchlake.DimSupplier;
# MAGIC 
# MAGIC -- CREATE TABLE tpchlake.DimSupplier(
# MAGIC -- 	 SupplierKey int NOT NULL
# MAGIC -- 	,SupplierCode int NOT NULL
# MAGIC -- 	,SupplierName varchar(100) NOT NULL
# MAGIC -- 	,SupplierAddress varchar(100) NOT NULL
# MAGIC -- 	,SupplierPhone varchar(20) NOT NULL
# MAGIC -- 	,SupplierBalance decimal(15, 2) NOT NULL
# MAGIC -- 	,SupplierComment varchar(200) NOT NULL
# MAGIC -- 	,SupplierNation varchar(100) NOT NULL
# MAGIC -- 	,SupplierRegion varchar(100) NOT NULL
# MAGIC -- 	-- ,CreateDatetime datetime NOT NULL
# MAGIC -- 	-- ,ChangeDatetime datetime NOT NULL
# MAGIC -- 	,LoadId int NOT NULL
# MAGIC -- ) 
# MAGIC -- USING DELTA;
# MAGIC 
# MAGIC /* dbo.DimDate */
# MAGIC DROP TABLE IF EXISTS tpchlake.DimDate;
# MAGIC 
# MAGIC -- CREATE TABLE tpchlake.DimDate(
# MAGIC -- 	 DateKey INT NOT NULL
# MAGIC -- 	,Date DATE NOT NULL
# MAGIC -- 	,DayName VARCHAR(10) NOT NULL
# MAGIC -- 	,DayOfMonth SMALLINT NOT NULL
# MAGIC -- 	,DayOfYear SMALLINT NOT NULL
# MAGIC -- 	,Month  SMALLINT NOT NULL
# MAGIC -- 	,MonthName VARCHAR(10) NOT NULL
# MAGIC -- 	,MonthOfQuarter SMALLINT NOT NULL
# MAGIC -- 	,MonthYear INT NOT NULL
# MAGIC -- 	,Quarter SMALLINT NOT NULL
# MAGIC -- 	,QuarterName VARCHAR(2) NOT NULL
# MAGIC -- 	,Year SMALLINT NOT NULL
# MAGIC -- 	,YearName VARCHAR(10) NOT NULL
# MAGIC -- 	-- ,CreateDatetime datetime NOT NULL
# MAGIC -- 	-- ,ChangeDatetime datetime NOT NULL
# MAGIC -- 	,LoadId INT NOT NULL
# MAGIC -- )
# MAGIC -- USING DELTA;
# MAGIC 
# MAGIC 
# MAGIC /* dbo.DimSuppliedPart */
# MAGIC DROP TABLE IF EXISTS tpchlake.DimSuppliedPart;
# MAGIC 
# MAGIC -- CREATE TABLE tpchlake.DimSuppliedPart(
# MAGIC -- 	 SuppliedPartKey INT NOT NULL
# MAGIC -- 	,PartCode INT NOT NULL
# MAGIC -- 	,SupplierCode INT NOT NULL
# MAGIC -- 	,SuppliedPartQuantity INT NOT NULL
# MAGIC -- 	,SuppliedPartCost decimal(15, 2) NOT NULL
# MAGIC -- 	,SuppliedPartComment varchar(200) NOT NULL
# MAGIC -- 	,PartName varchar(100) NOT NULL
# MAGIC -- 	,PartManufacturer varchar(100) NOT NULL
# MAGIC -- 	,PartBrand varchar(100) NOT NULL
# MAGIC -- 	,PartType varchar(100) NOT NULL
# MAGIC -- 	,PartSize INT NOT NULL
# MAGIC -- 	,PartContainer varchar(100) NOT NULL
# MAGIC -- 	,PartRetailPrice decimal(15, 2) NOT NULL
# MAGIC -- 	,PartComment varchar(200) NOT NULL
# MAGIC -- 	-- ,CreateDatetime datetime NOT NULL
# MAGIC -- 	-- ,ChangeDatetime datetime NOT NULL
# MAGIC -- 	,LoadId INT NOT NULL
# MAGIC -- )
# MAGIC -- USING DELTA;
# MAGIC 
# MAGIC /* dbo.FactOrderLines */
# MAGIC DROP TABLE IF EXISTS tpchlake.FactOrderLines;
# MAGIC 
# MAGIC -- CREATE TABLE tpchlake.FactOrderLines(
# MAGIC -- 	CommitDateKey int NOT NULL,
# MAGIC -- 	CustomerKey int NOT NULL,
# MAGIC -- 	OrderDateKey int NOT NULL,
# MAGIC -- 	ReceiptDateKey int NOT NULL,
# MAGIC -- 	ShipDateKey int NOT NULL,
# MAGIC -- 	SuppliedPartKey int NOT NULL,
# MAGIC -- 	SupplierKey int NOT NULL,
# MAGIC -- 	OrderCode int NOT NULL,
# MAGIC -- 	LineNumber int NOT NULL,
# MAGIC -- 	Quantity decimal(15, 2) NOT NULL,
# MAGIC -- 	ExtendedPrice decimal(15, 2) NOT NULL,
# MAGIC -- 	Discount decimal(15, 2) NOT NULL,
# MAGIC -- 	Tax decimal(15, 2) NOT NULL,
# MAGIC -- 	ReturnFlag varchar(50) NOT NULL,
# MAGIC -- 	LineStatus varchar(50) NOT NULL,
# MAGIC -- 	ShipInstruct varchar(50) NOT NULL,
# MAGIC -- 	ShipMode varchar(50) NOT NULL,
# MAGIC -- 	LineItemComment varchar(200) NOT NULL,
# MAGIC -- 	OrderStatus varchar(50) NOT NULL,
# MAGIC -- 	OrderTotalPrice decimal(15, 2) NOT NULL,
# MAGIC -- 	OrderPriority varchar(50) NOT NULL,
# MAGIC -- 	OrderClerk varchar(50) NOT NULL,
# MAGIC -- 	OrderShipPriority varchar(50) NOT NULL,
# MAGIC -- 	OrderComment varchar(200) NOT NULL,
# MAGIC -- 	LoadId int NOT NULL
# MAGIC -- ) 
# MAGIC -- USING DELTA;
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

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
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
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
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
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
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
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
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
