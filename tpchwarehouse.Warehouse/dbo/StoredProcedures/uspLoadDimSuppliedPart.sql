CREATE PROCEDURE dbo.uspLoadDimSuppliedPart(
	@LoadId INT
)
AS 
BEGIN
	/*
		Sample Call

		EXECUTE dbo.uspLoadDimSuppliedPart 
			@LoadId = 1

		DECLARE @LoadId INT = 1
		
	*/

	/* MaxKey */
	DECLARE @MaxKey INT 
	SELECT @MaxKey = ISNULL(MAX(SuppliedPartKey),0) FROM dbo.DimSuppliedPart
	SELECT @MaxKey

	/* staging table */
	DROP TABLE IF EXISTS Staging.SuppliedPart
	SELECT 
		 @MaxKey + ROW_NUMBER() OVER(ORDER BY partsupp.ps_partkey, partsupp.ps_suppkey) AS SuppliedPartKey
		,partsupp.ps_partkey AS PartCode
		,partsupp.ps_suppkey AS SupplierCode
		,partsupp.ps_availqty AS SuppliedPartQuantity
		,partsupp.ps_supplycost AS SuppliedPartCost
		,partsupp.ps_comment AS SuppliedPartComment
		,part.p_name AS PartName
		,part.p_mfgr AS PartManufacturer
		,part.p_brand AS PartBrand 
		,part.p_type AS PartType
		,part.p_size AS PartSize
		,part.p_container AS PartContainer
		,part.p_retailprice AS PartRetailPrice
		,part.p_comment AS PartComment
	INTO Staging.SuppliedPart
	FROM 
		tpchlake.dbo.partsupp partsupp
		INNER JOIN tpchlake.dbo.part part
			ON partsupp.ps_partkey = part.p_partkey
	--WHERE 
	--	partsupp.ps_partkey = 17789322
	;


	/* Initialization */
	IF @MaxKey = 0
	BEGIN

		INSERT INTO dbo.DimSuppliedPart(
			 SuppliedPartKey
			,PartCode
			,SupplierCode
			,SuppliedPartQuantity
			,SuppliedPartCost
			,SuppliedPartComment
			,PartName
			,PartManufacturer
			,PartBrand
			,PartType
			,PartSize
			,PartContainer
			,PartRetailPrice
			,PartComment
			,CreateDatetime
			,ChangeDatetime
			,LoadId
		)
		SELECT 
			 SuppliedPartKey
			,PartCode
			,SupplierCode
			,SuppliedPartQuantity
			,SuppliedPartCost
			,SuppliedPartComment
			,PartName
			,PartManufacturer
			,PartBrand
			,PartType
			,PartSize
			,PartContainer
			,PartRetailPrice
			,PartComment
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,@LoadId AS LoadId 
		FROM 
			Staging.SuppliedPart
		
	END
	ELSE /* incremental */
	BEGIN
		
		/* MERGE statement not available as of 2024.01.19 */
		
		/* update existing records */
		UPDATE tgt
		SET 
			 tgt.SuppliedPartQuantity = src.SuppliedPartQuantity
			,tgt.SuppliedPartCost = src.SuppliedPartCost
			,tgt.SuppliedPartComment = src.SuppliedPartComment
			,tgt.PartName = src.PartName
			,tgt.PartManufacturer = src.PartManufacturer
			,tgt.PartBrand = src.PartBrand
			,tgt.PartType = src.PartType
			,tgt.PartSize = src.PartSize
			,tgt.PartContainer = src.PartContainer
			,tgt.PartRetailPrice = src.PartRetailPrice
			,tgt.PartComment = src.PartComment
			,tgt.ChangeDatetime = CAST(GETDATE() AS DATETIME2(0))  
			,tgt.LoadId = @LoadId
		FROM 
			dbo.DimSuppliedPart tgt
			INNER JOIN Staging.SuppliedPart src 
				ON tgt.PartCode = src.PartCode AND tgt.SupplierCode = src.SupplierCode
		WHERE 
			tgt.SuppliedPartQuantity <> src.SuppliedPartQuantity
			OR tgt.SuppliedPartCost <> src.SuppliedPartCost
			OR tgt.SuppliedPartComment <> src.SuppliedPartComment
			OR tgt.PartName <> src.PartName
			OR tgt.PartManufacturer <> src.PartManufacturer
			OR tgt.PartBrand <> src.PartBrand
			OR tgt.PartType <> src.PartType
			OR tgt.PartSize <> src.PartSize
			OR tgt.PartContainer <> src.PartContainer
			OR tgt.PartRetailPrice <> src.PartRetailPrice
			OR tgt.PartComment <> src.PartComment


		/* insert new records */
		INSERT INTO dbo.DimSuppliedPart(
			 SuppliedPartKey
			,PartCode
			,SupplierCode
			,SuppliedPartQuantity
			,SuppliedPartCost
			,SuppliedPartComment
			,PartName
			,PartManufacturer
			,PartBrand
			,PartType
			,PartSize
			,PartContainer
			,PartRetailPrice
			,PartComment
			,CreateDatetime
			,ChangeDatetime
			,LoadId
		)
		SELECT 
			 src.SuppliedPartKey
			,src.PartCode
			,src.SupplierCode
			,src.SuppliedPartQuantity
			,src.SuppliedPartCost
			,src.SuppliedPartComment
			,src.PartName
			,src.PartManufacturer
			,src.PartBrand
			,src.PartType
			,src.PartSize
			,src.PartContainer
			,src.PartRetailPrice
			,src.PartComment
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,@LoadId AS LoadId 
		FROM 
			Staging.SuppliedPart src
			LEFT JOIN dbo.DimSuppliedPart tgt
				ON tgt.PartCode = src.PartCode AND tgt.SupplierCode = src.SupplierCode
		WHERE 
			tgt.SuppliedPartKey IS NULL

	END




	


	/* drop the staging table */
	DROP TABLE IF EXISTS Staging.SuppliedPart

END