CREATE PROCEDURE dbo.uspLoadDimDate(
	 @LoadId INT
	,@StartDate DATE
	,@EndDate DATE
)
AS 
BEGIN
	/*
		Sample Call

		EXECUTE dbo.uspLoadDimDate 
			@LoadId = 1
			,@StartDate = '1989-12-31'  --this is the min order data in the order table
			,@EndDate = '1999-12-31'

		DECLARE @LoadId INT = 1
			,@StartDate DATE = '1989-12-31' 
			,@EndDate DATE = '1999-12-31'
		
	*/

	/* MaxKey */
	DECLARE @MaxKey INT 
	SELECT @MaxKey = ISNULL(MAX(DateKey),0) FROM dbo.DimDate
	SELECT @MaxKey


	/* staging dates */
	DROP TABLE IF EXISTS Staging.Dates
	CREATE TABLE Staging.Dates(
		 DateKey INT NOT NULL
		,Date DATE NOT NULL
		,DayName VARCHAR(10) NOT NULL
		,DayOfMonth SMALLINT NOT NULL
		,DayOfYear SMALLINT NOT NULL
		,Month  SMALLINT NOT NULL
		,MonthName VARCHAR(10) NOT NULL
		,MonthOfQuarter SMALLINT NOT NULL
		,MonthYear INT NOT NULL
		,Quarter SMALLINT NOT NULL
		,QuarterName VARCHAR(2) NOT NULL
		,Year SMALLINT NOT NULL
		,YearName VARCHAR(10) NOT NULL
	)


	;WITH numbers AS (
		SELECT number FROM master..spt_values WHERE type = 'P' AND number >= 1
		UNION ALL
		SELECT number + 2047 FROM master..spt_values WHERE type = 'P' AND number >= 1
		UNION ALL
		SELECT number + (2047 * 2) FROM master..spt_values WHERE type = 'P' AND number >= 1

	)
	,Dates AS(
		SELECT 
			DATEADD(DAY, number, @StartDate) [Date]
		FROM 
			numbers
		WHERE 
			DATEADD(DAY, number, @StartDate) <= @EndDate
	)
	INSERT INTO Staging.Dates(
		 DateKey
		,Date
		,DayName
		,DayOfMonth
		,DayOfYear
		,Month
		,MonthName
		,MonthOfQuarter
		,MonthYear
		,Quarter
		,QuarterName
		,Year
		,YearName
	)
	SELECT
		 CONVERT(CHAR(8), [Date], 112) AS DateKey
		,[Date]
		,DATENAME(DW, [Date]) AS DayName
		,DATEPART(DD, [Date]) AS DayOfMonth
		,DATEPART(DY, [Date]) AS DayOfYear
		,DATEPART(MM, [Date]) AS Month
		,DATENAME(MM, [Date]) AS MonthName
		,
		CASE
			WHEN DATEPART(MM, [Date]) IN (1, 4, 7, 10) THEN 1
			WHEN DATEPART(MM, [Date]) IN (2, 5, 8, 11) THEN 2
			WHEN DATEPART(MM, [Date]) IN (3, 6, 9, 12) THEN 3
			END 
		AS MonthOfQuarter
		--,CAST(LEFT(CONVERT(CHAR(8), [Date], 112), 6) AS INT) AS MonthYear
		,YEAR([Date]) * 12 + MONTH([Date]) AS MonthYear
		,DATEPART(QQ, [Date]) AS Quarter
		,'Q' + CAST(DATEPART(QQ, [Date]) AS VARCHAR(1)) AS QuarterName
		,DATEPART(YEAR, [Date]) AS Year
		,'CY ' + CONVERT(VARCHAR, DATEPART(YEAR, [Date])) AS YearName
	FROM 
		Dates


	--/* Initialization */
	--IF @MaxKey = 0
	--BEGIN
	--	PRINT 'init'
	--	INSERT INTO dbo.DimDate(
	--		 DateKey
	--		,Date
	--		,DayName
	--		,DayOfMonth
	--		,DayOfYear
	--		,Month
	--		,MonthName
	--		,MonthOfQuarter
	--		,MonthYear
	--		,Quarter
	--		,QuarterName
	--		,Year
	--		,YearName
	--		,CreateDatetime 
	--		,ChangeDatetime 
	--		,LoadId 
	--	)
	--	SELECT 
	--		 DateKey
	--		,Date
	--		,DayName
	--		,DayOfMonth
	--		,DayOfYear
	--		,Month
	--		,MonthName
	--		,MonthOfQuarter
	--		,MonthYear
	--		,Quarter
	--		,QuarterName
	--		,Year
	--		,YearName
	--		,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
	--		,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
	--		,@LoadId AS LoadId 
	--	FROM 
	--		Staging.Dates

	--END
	--ELSE /* incremental */
	--BEGIN
	--	PRINT 'inc'
		/* MERGE statement not available as of 2024.01.19 */

		/* update existing records */
		UPDATE tgt
		SET 
			 tgt.DateKey = src.DateKey
			,tgt.Date = src.Date
			,tgt.DayName = src.DayName
			,tgt.DayOfMonth = src.DayOfMonth
			,tgt.DayOfYear = src.DayOfYear
			,tgt.Month = src.Month
			,tgt.MonthName = src.MonthName
			,tgt.MonthOfQuarter = src.MonthOfQuarter
			,tgt.MonthYear = src.MonthYear
			,tgt.Quarter = src.Quarter
			,tgt.QuarterName = src.QuarterName
			,tgt.Year = src.Year
			,tgt.YearName = src.YearName
			,tgt.ChangeDatetime = CAST(GETDATE() AS DATETIME2(0))  
			,tgt.LoadId = @LoadId
		FROM 
			dbo.DimDate tgt
			INNER JOIN Staging.Dates src 
				ON src.DateKey = tgt.DateKey   
		--WHERE 
		--	tgt.Date <> src.Date
		--	OR tgt.DayName <> src.DayName
		--	OR tgt.DayOfMonth <> src.DayOfMonth
		--	OR tgt.DayOfYear <> src.DayOfYear
		--	OR tgt.Month <> src.Month
		--	OR tgt.MonthName <> src.MonthName
		--	OR tgt.MonthOfQuarter <> src.MonthOfQuarter
		--	OR tgt.MonthYear <> src.MonthYear
		--	OR tgt.Quarter <> src.Quarter
		--	OR tgt.QuarterName <> src.QuarterName
		--	OR tgt.Year <> src.Year
		--	OR tgt.YearName <> src.YearName

	--	/* insert new records */
	--	INSERT INTO dbo.DimDate(
	--		 DateKey
	--		,Date
	--		,DayName
	--		,DayOfMonth
	--		,DayOfYear
	--		,Month
	--		,MonthName
	--		,MonthOfQuarter
	--		,MonthYear
	--		,Quarter
	--		,QuarterName
	--		,Year
	--		,YearName
	--		,CreateDatetime 
	--		,ChangeDatetime 
	--		,LoadId 
	--	)
	--	SELECT 
	--		 src.DateKey
	--		,src.Date
	--		,src.DayName
	--		,src.DayOfMonth
	--		,src.DayOfYear
	--		,src.Month
	--		,src.MonthName
	--		,src.MonthOfQuarter
	--		,src.MonthYear
	--		,src.Quarter
	--		,src.QuarterName
	--		,src.Year
	--		,src.YearName
	--		,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
	--		,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
	--		,@LoadId AS LoadId 
	--	FROM 
	--		Staging.Dates src
	--		LEFT JOIN dbo.DimDate tgt
	--			ON tgt.DateKey = src.DateKey   
	--	WHERE 
	--		tgt.DateKey IS NULL

	--END

	/* drop the staging table */
	DROP TABLE IF EXISTS Staging.Dates

	--SELECT min(Date), max(Date) FROM dbo.DimDate

END