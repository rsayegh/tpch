CREATE PROCEDURE dbo.uspLoadFactAccounts(
	@LoadId INT
)
AS 
BEGIN
	/*
		Sample Call

		EXECUTE dbo.uspLoadFactAccounts 
			@LoadId = 1

		DECLARE @LoadId INT = 1
		
	*/

	/* staging input */
	DROP TABLE IF EXISTS dbo.StagingAccounts
	;WITH Temp AS (
		SELECT 
			 t.[DateKey]
			,a.[Account]
			--,CAST(t.RowNumber AS DECIMAL(10,2)) / 100
			--,CAST([Value] AS DECIMAL(12,2))
			,
			CASE 
				WHEN t.[DateKey] in (19970101, 19980101, 19980201) THEN CAST(a.[Value] AS DECIMAL(12,2)) 
				ELSE CAST(a.[Value] AS DECIMAL(12,2)) * (1 + CAST(t.RowNumber AS DECIMAL(10,2)) / 100)
			END AS [Value]
		FROM 
			[tpchlake].[dbo].[accounts] a
			CROSS APPLY (
				SELECT 
					DateKey, ROW_NUMBER() OVER(ORDER BY DateKey) AS RowNumber
				FROM 
					[dbo].[DimDate]
				WHERE 
					[DayOfMonth] = 1 AND [MonthYear] BETWEEN 199701 AND 199712
			) t

		WHERE 
			a.[DateKey] = 19970101
		UNION ALL
		SELECT 
			 a.[DateKey]
			,a.[Account]
			,CAST(a.[Value] AS DECIMAL(12,2)) AS [Value]
		FROM 
			[tpchlake].[dbo].[accounts] a
		WHERE 
			a.[DateKey] IN ( 19980101, 19980201)
	)
	SELECT 
		[DateKey]
		,PVT.[Product Revenue] AS [ProductRevenue]
		,PVT.[Service and other revenue] AS [ServiceAndOtherRevenue]
		,PVT.[Revenue] AS [Revenue]
		,PVT.[Product cost] AS [ProductCost]
		,PVT.[Service and other costs] AS [ServiceAndOtherCosts]
		,PVT.[Gross margin] AS [GrossMargin]
		,PVT.[Research and development] AS [ResearchAndDevelopment]
		,PVT.[Sales and marketing] AS [SalesAndMarketing]
		,PVT.[General and administrative] AS [GeneralAndAdministrative]
		,PVT.[Restructuring] AS [Restructuring]
		,PVT.[Operating income] AS [OperatingIncome]
		,PVT.[Other Income, net] AS [OtherIncomeNet]
		,PVT.[Income before income taxes] AS [IncomeBeforeIncomeTaxes]
		,PVT.[Provision for income taxes] AS [ProvisionForIncomeTaxes]
		,PVT.[Net income] AS [NetIncome]
	INTO dbo.StagingAccounts
	FROM   
	(
		SELECT 
			[DateKey],[Account],[Value]
		FROM 
			Temp
	
		) p  
		PIVOT  
		(  
		SUM([Value])
		FOR [Account] IN ( 
			 [Product Revenue]
			,[Service and other revenue]
			,[Revenue]
			,[Product cost]
			,[Service and other costs]
			,[Gross margin]
			,[Research and development]
			,[Sales and marketing]
			,[General and administrative]
			,[Restructuring]
			,[Operating income]
			,[Other Income, net]
			,[Income before income taxes]
			,[Provision for income taxes]
			,[Net income]
			)  
		) AS PVT ;

	--select * from  dbo.StagingAccounts
	--where [DateKey] in (19970101,19980101)

	/* partitioning and truncate statements are not available as of 26.01.2024 */
	DROP TABLE IF EXISTS dbo.FactAccounts 
	EXEC sp_rename 'StagingAccounts', 'FactAccounts';


END