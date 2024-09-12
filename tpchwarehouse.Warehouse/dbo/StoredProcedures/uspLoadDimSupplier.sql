CREATE PROCEDURE dbo.uspLoadDimSupplier(
	@LoadId INT
)
AS 
BEGIN
	/*
		Sample Call

		EXECUTE dbo.uspLoadDimSupplier 
			@LoadId = 1

		DECLARE @LoadId INT = 1
		
	*/

	/* MaxKey */
	DECLARE @MaxKey INT 
	SELECT @MaxKey = ISNULL(MAX(SupplierKey),0) FROM dbo.DimSupplier
	SELECT @MaxKey

	/* staging input */
	DROP TABLE IF EXISTS Staging.Suppliers
	SELECT
		 @MaxKey + ROW_NUMBER() OVER(ORDER BY s.s_suppkey) AS SupplierKey
		,s.s_suppkey AS SupplierCode
		,s.s_name AS SupplierName
		,s.s_address AS SupplierAddress
		,s.s_phone AS SupplierPhone
		,s.s_acctbal AS SupplierBalance
		,s.s_comment AS SupplierComment
		,ISNULL(n.n_name, 'N/A') AS SupplierNation
		,ISNULL(r.r_name, 'N/A') AS SupplierRegion
	INTO Staging.Suppliers
	FROM 
		tpchlake.dbo.supplier s
		LEFT JOIN tpchlake.dbo.nation n 
			ON s.s_nationkey = n.n_nationkey
		LEFT JOIN tpchlake.dbo.region r
			ON n.n_regionkey = r.r_regionkey
	;


	/* Initialization */
	IF @MaxKey = 0
	BEGIN
		INSERT INTO dbo.DimSupplier(
			 SupplierKey
			,SupplierCode
			,SupplierName
			,SupplierAddress
			,SupplierPhone
			,SupplierBalance
			,SupplierComment
			,SupplierNation
			,SupplierRegion
			,CreateDatetime
			,ChangeDatetime
			,LoadId
		)
		SELECT 
			 SupplierKey
			,SupplierCode
			,SupplierName
			,SupplierAddress
			,SupplierPhone
			,SupplierBalance
			,SupplierComment
			,SupplierNation
			,SupplierRegion
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,@LoadId AS LoadId 
		FROM 
			Staging.Suppliers
		
	END
	ELSE /* incremental */
	BEGIN
		
		/* MERGE statement not available as of 2024.01.19 */
		
		/* update existing records */
		UPDATE tgt
		SET 
			 tgt.SupplierName = src.SupplierName
			,tgt.SupplierAddress = src.SupplierAddress
			,tgt.SupplierPhone = src.SupplierPhone
			,tgt.SupplierBalance = src.SupplierBalance
			,tgt.SupplierComment = src.SupplierComment
			,tgt.SupplierNation = src.SupplierNation
			,tgt.SupplierRegion = src.SupplierRegion
			,tgt.ChangeDatetime = CAST(GETDATE() AS DATETIME2(0))  
			,tgt.LoadId = @LoadId
		FROM 
			dbo.DimSupplier tgt
			INNER JOIN Staging.Suppliers src 
				ON src.SupplierCode = tgt.SupplierCode   
		WHERE 
			tgt.SupplierName <> src.SupplierName
			OR tgt.SupplierAddress <> src.SupplierAddress
			OR tgt.SupplierPhone <> src.SupplierPhone
			OR tgt.SupplierBalance <> src.SupplierBalance
			OR tgt.SupplierComment <> src.SupplierComment
			OR tgt.SupplierNation <> src.SupplierNation
			OR tgt.SupplierRegion <> src.SupplierRegion


		/* insert new records */
		INSERT INTO dbo.DimSupplier(
			SupplierKey
			,SupplierCode
			,SupplierName
			,SupplierAddress
			,SupplierPhone
			,SupplierBalance
			,SupplierComment
			,SupplierNation
			,SupplierRegion
			,CreateDatetime
			,ChangeDatetime
			,LoadId
		)
		SELECT 
			 src.SupplierKey
			,src.SupplierCode
			,src.SupplierName
			,src.SupplierAddress
			,src.SupplierPhone
			,src.SupplierBalance
			,src.SupplierComment
			,src.SupplierNation
			,src.SupplierRegion
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,@LoadId AS LoadId 
		FROM 
			Staging.Suppliers src
			LEFT JOIN dbo.DimSupplier tgt
				ON tgt.SupplierCode = src.SupplierCode   
		WHERE 
			tgt.SupplierKey IS NULL

	END

	/* 	NA member */
	IF NOT EXISTS(
		SELECT 1 FROM dbo.DimSupplier
		WHERE SupplierKey = -1
	)
	BEGIN
		INSERT INTO dbo.DimSupplier(
			SupplierKey
			,SupplierCode
			,SupplierName
			,SupplierAddress
			,SupplierPhone
			,SupplierBalance
			,SupplierComment
			,SupplierNation
			,SupplierRegion
			,CreateDatetime
			,ChangeDatetime
			,LoadId
		)
		SELECT
			 -1 AS SupplierKey
			,-1 AS SupplierCode
			,'NA' AS SupplierName
			,'NA' AS SupplierAddress
			,'NA' AS SupplierPhone
			,0 AS SupplierBalance
			,'NA' AS SupplierComment
			,'NA'AS SupplierNation
			,'NA' AS SupplierRegion
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,1 AS LoadId

	END

	/* drop the staging table */
	DROP TABLE IF EXISTS Staging.Suppliers

END