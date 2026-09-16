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
			,@StartDate DATE = '1992-01-01' 
			,@EndDate DATE = '2024-12-31'
		
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



	/* while loop to iterate over the dates */
	WHILE @StartDate <= @EndDate
	BEGIN

		/* set the current date */
		DECLARE @CurrentDate AS DATE = @StartDate
	
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
			 CONVERT(CHAR(8), @CurrentDate, 112) AS DateKey
			,@CurrentDate AS Date
			,DATENAME(DW, @CurrentDate) AS DayName
			,DATEPART(DD, @CurrentDate) AS DayOfMonth
			,DATEPART(DY, @CurrentDate) AS DayOfYear
			,DATEPART(MM, @CurrentDate) AS Month
			,DATENAME(MM, @CurrentDate) AS MonthName
			,
			CASE
				WHEN DATEPART(MM, @CurrentDate) IN (1, 4, 7, 10) THEN 1
				WHEN DATEPART(MM, @CurrentDate) IN (2, 5, 8, 11) THEN 2
				WHEN DATEPART(MM, @CurrentDate) IN (3, 6, 9, 12) THEN 3
				END 
			AS MonthOfQuarter
			,CAST(LEFT(CONVERT(CHAR(8), @CurrentDate, 112), 6) AS INT) AS MonthYear
			,DATEPART(QQ, @CurrentDate) AS Quarter
			,'Q' + CAST(DATEPART(QQ, @CurrentDate) AS VARCHAR(1)) AS QuarterName
			,DATEPART(YEAR, @CurrentDate) AS Year
			,'CY ' + CONVERT(VARCHAR, DATEPART(YEAR, @CurrentDate)) AS YearName

		/* increment the start date */
		SET @StartDate = DATEADD(DD, 1, @StartDate)

	END


	/* Initialization */
	IF @MaxKey = 0
	BEGIN
		PRINT 'init'
		INSERT INTO dbo.DimDate(
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
			,CreateDatetime 
			,ChangeDatetime 
			,LoadId 
		)
		SELECT 
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
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,@LoadId AS LoadId 
		FROM 
			Staging.Dates

	END
	ELSE /* incremental */
	BEGIN
		PRINT 'inc'
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
		WHERE 
			tgt.Date <> src.Date
			OR tgt.DayName <> src.DayName
			OR tgt.DayOfMonth <> src.DayOfMonth
			OR tgt.DayOfYear <> src.DayOfYear
			OR tgt.Month <> src.Month
			OR tgt.MonthName <> src.MonthName
			OR tgt.MonthOfQuarter <> src.MonthOfQuarter
			OR tgt.MonthYear <> src.MonthYear
			OR tgt.Quarter <> src.Quarter
			OR tgt.QuarterName <> src.QuarterName
			OR tgt.Year <> src.Year
			OR tgt.YearName <> src.YearName

		/* insert new records */
		INSERT INTO dbo.DimDate(
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
			,CreateDatetime 
			,ChangeDatetime 
			,LoadId 
		)
		SELECT 
			 src.DateKey
			,src.Date
			,src.DayName
			,src.DayOfMonth
			,src.DayOfYear
			,src.Month
			,src.MonthName
			,src.MonthOfQuarter
			,src.MonthYear
			,src.Quarter
			,src.QuarterName
			,src.Year
			,src.YearName
			,CAST(GETDATE() AS DATETIME2(0)) AS CreateDatetime
			,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
			,@LoadId AS LoadId 
		FROM 
			Staging.Dates src
			LEFT JOIN dbo.DimDate tgt
				ON tgt.DateKey = src.DateKey   
		WHERE 
			tgt.DateKey IS NULL

	END

	/* drop the staging table */
	DROP TABLE IF EXISTS Staging.Dates

END

--SELECT * FROM dbo.DimDate

GO