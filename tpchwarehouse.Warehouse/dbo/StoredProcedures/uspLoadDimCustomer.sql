CREATE PROCEDURE dbo.uspLoadDimCustomer(
	@LoadId INT
)
AS 
BEGIN
	/*
		Sample Call

		EXECUTE dbo.uspLoadDimCustomer 
			@LoadId = 1

		DECLARE @LoadId INT = 1
		
	*/

	/* MaxKey */
	DECLARE @MaxKey INT 
	SELECT @MaxKey = ISNULL(MAX(CustomerKey),0) FROM dbo.DimCustomer
	SELECT @MaxKey

	/* staging input */
	DROP TABLE IF EXISTS Staging.Customers
	SELECT
		 @MaxKey + ROW_NUMBER() OVER(ORDER BY c.c_custkey) AS CustomerKey
		,c.c_custkey AS CustomerCode
		,c.c_name AS CustomerName
		,c.c_address AS CustomerAddress
		,c.c_phone AS CustomerPhone
		,c.c_acctbal AS CustomerBalance
		,c.c_mktsegment AS CustomerSegment
		,c.c_comment AS CustomerComment
		,ISNULL(n.n_name, 'N/A') AS CustomerNation
		,ISNULL(r.r_name, 'N/A') AS CustomerRegion
	INTO Staging.Customers
	FROM 
		tpchlake.dbo.customer c
		LEFT JOIN tpchlake.dbo.nation n 
			ON c.c_nationkey = n.n_nationkey
		LEFT JOIN tpchlake.dbo.region r
			ON n.n_regionkey = r.r_regionkey
	;


	/* Initialization */
	IF @MaxKey = 0
	BEGIN
		INSERT INTO dbo.DimCustomer(
			CustomerKey
			,CustomerCode
			,CustomerName
			,CustomerAddress
			,CustomerPhone
			,CustomerBalance
			,CustomerSegment
			,CustomerComment
			,CustomerNation
			,CustomerRegion
			,CreateDatetime
			,ChangeDatetime
			,LoadId
		)
		SELECT 
			 CustomerKey
			,CustomerCode
			,CustomerName
			,CustomerAddress
			,CustomerPhone
			,CustomerBalance
			,CustomerSegment
			,CustomerComment
			,CustomerNation
			,CustomerRegion
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,@LoadId AS LoadId 
		FROM 
			Staging.Customers
		
	END
	ELSE /* incremental */
	BEGIN
		
		/* MERGE statement not available as of 2024.01.19 */
		
		/* update existing records */
		UPDATE tgt
		SET 
			 tgt.CustomerName = src.CustomerName
			,tgt.CustomerAddress = src.CustomerAddress
			,tgt.CustomerPhone = src.CustomerPhone
			,tgt.CustomerBalance = src.CustomerBalance
			,tgt.CustomerSegment = src.CustomerSegment
			,tgt.CustomerComment = src.CustomerComment
			,tgt.CustomerNation = src.CustomerNation
			,tgt.CustomerRegion = src.CustomerRegion
			,tgt.ChangeDatetime = CAST(GETDATE() AS DATETIME2(0))  
			,tgt.LoadId = @LoadId
		FROM 
			dbo.DimCustomer tgt
			INNER JOIN Staging.Customers src 
				ON src.CustomerCode = tgt.CustomerCode   
		WHERE 
			tgt.CustomerName <> src.CustomerName
			OR tgt.CustomerAddress <> src.CustomerAddress
			OR tgt.CustomerPhone <> src.CustomerPhone
			OR tgt.CustomerBalance <> src.CustomerBalance
			OR tgt.CustomerSegment <> src.CustomerSegment
			OR tgt.CustomerComment <> src.CustomerComment
			OR tgt.CustomerNation <> src.CustomerNation
			OR tgt.CustomerRegion <> src.CustomerRegion


		/* insert new records */
		INSERT INTO dbo.DimCustomer(
			CustomerKey
			,CustomerCode
			,CustomerName
			,CustomerAddress
			,CustomerPhone
			,CustomerBalance
			,CustomerSegment
			,CustomerComment
			,CustomerNation
			,CustomerRegion
			,CreateDatetime
			,ChangeDatetime
			,LoadId
		)
		SELECT 
			 src.CustomerKey
			,src.CustomerCode
			,src.CustomerName
			,src.CustomerAddress
			,src.CustomerPhone
			,src.CustomerBalance
			,src.CustomerSegment
			,src.CustomerComment
			,src.CustomerNation
			,src.CustomerRegion
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,@LoadId AS LoadId 
		FROM 
			Staging.Customers src
			LEFT JOIN dbo.DimCustomer tgt
				ON tgt.CustomerCode = src.CustomerCode   
		WHERE 
			tgt.CustomerKey IS NULL

	END

	/* 	NA member */
	IF NOT EXISTS(
		SELECT 1 FROM dbo.DimCustomer
		WHERE CustomerKey = -1
	)
	BEGIN
		INSERT INTO dbo.DimCustomer(
			 CustomerKey
			,CustomerCode
			,CustomerName
			,CustomerAddress
			,CustomerPhone
			,CustomerBalance
			,CustomerSegment
			,CustomerComment
			,CustomerNation
			,CustomerRegion
			,CreateDatetime
			,ChangeDatetime
			,LoadId
		)
		SELECT
			 -1 AS CustomerKey
			,-1 AS CustomerCode
			,'NA' AS CustomerName
			,'NA' AS CustomerAddress
			,'NA' AS CustomerPhone
			,0 AS CustomerBalance
			,'NA' AS CustomerSegment
			,'NA' AS CustomerComment
			,'NA'AS CustomerNation
			,'NA' AS CustomerRegion
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,@LoadId AS LoadId

	END

	/* drop the staging table */
	DROP TABLE IF EXISTS Staging.Customers

END