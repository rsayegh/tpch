CREATE PROCEDURE dbo.uspLoadFactOrderLines(
	@LoadId INT
)
AS 
BEGIN
	/*
		Sample Call

		EXECUTE dbo.uspLoadFactOrderLines 
			@LoadId = 1

		DECLARE @LoadId INT = 1
		
	*/

	/* staging input */
	DROP TABLE IF EXISTS dbo.StagingOrderLines
	SELECT
		 /* surrogate keys */
		 CAST(CONVERT(CHAR(8), li.l_commitdate, 112) AS INT) AS CommitDateKey
		,ISNULL(c.[CustomerKey], -1) AS CustomerKey
		,CAST(CONVERT(CHAR(8), o.o_orderdate, 112) AS INT) AS OrderDateKey
		,CAST(CONVERT(CHAR(8), li.l_receiptdate, 112) AS INT) AS ReceiptDateKey
		,CAST(CONVERT(CHAR(8), li.l_shipdate, 112) AS INT) AS ShipDateKey
		,ISNULL(sp.[SuppliedPartKey], -1) AS [SuppliedPartKey]
		,ISNULL(s.[SupplierKey], -1) AS [SupplierKey]
		 /* metrics and properties */
		,ISNULL(o.o_orderkey, -1) AS OrderCode
		,li.l_linenumber AS LineNumber
		,li.l_quantity AS Quantity
		,li.l_extendedprice AS ExtendedPrice
		,li.l_discount AS Discount
		,li.l_tax AS Tax
		,CAST(li.l_returnflag AS VARCHAR(50)) AS ReturnFlag
		,CAST(li.l_linestatus AS VARCHAR(50)) AS LineStatus
		,CAST(li.l_shipinstruct AS VARCHAR(50)) AS ShipInstruct
		,CAST(li.l_shipmode AS VARCHAR(50)) AS ShipMode
		,CAST(li.l_comment AS VARCHAR(200)) AS LineItemComment
		,CAST(o.o_orderstatus AS VARCHAR(50)) AS OrderStatus
		,o.o_totalprice AS OrderTotalPrice
		,CAST(o.o_orderpriority AS VARCHAR(50)) AS OrderPriority
		,CAST(o.o_clerk AS VARCHAR(50)) AS OrderClerk
		,CAST(o_shippriority AS VARCHAR(50)) AS OrderShipPriority
		,CAST(o.o_comment AS VARCHAR(200)) AS OrderComment
		,@LoadId AS LoadId
	INTO dbo.StagingOrderLines
	FROM 
		tpchlake.dbo.lineitem li
		LEFT JOIN tpchlake.dbo.orders o
			ON o.o_orderkey = li.l_orderkey
		LEFT JOIN [dbo].[DimCustomer] c
			ON c.CustomerCode = o.o_custkey
		LEFT JOIN [dbo].[DimSuppliedPart] sp
			ON sp.PartCode = li.l_partkey AND sp.SupplierCode = li.l_suppkey 
		LEFT JOIN [dbo].[DimSupplier] s
			ON s.SupplierCode = li.[l_suppkey]


	/* partitioning and truncate statements are not available as of 26.01.2024 */
	DROP TABLE IF EXISTS dbo.FactOrderLines
	EXEC sp_rename 'StagingOrderLines', 'FactOrderLines';


END

--08:10
--