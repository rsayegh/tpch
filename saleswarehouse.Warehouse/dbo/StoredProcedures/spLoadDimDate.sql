CREATE PROC dbo.spLoadDimDate
AS
BEGIN
    /*
        sample call
        EXECUTE dbo.spLoadDimDate
    */

    /* stage the data */
    DROP TABLE IF EXISTS dbo.Staging_DimDate

    CREATE TABLE dbo.Staging_DimDate
    AS
    SELECT 
        DateKey
        ,FullDateAlternateKey
        ,DayNumberOfWeek
        ,EnglishDayNameOfWeek
        ,DayNumberOfMonth
        ,DayNumberOfYear
        ,WeekNumberOfYear
        ,EnglishMonthName
        ,MonthNumberOfYear
        ,CalendarYear
        ,FiscalYear
        ,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
    FROM 
        raw.dbo.date

    ;

    /* update existing records */
    UPDATE tgt
    SET 
         tgt.DateKey = src.DateKey
        ,tgt.FullDateAlternateKey = src.FullDateAlternateKey
        ,tgt.DayNumberOfWeek = src.DayNumberOfWeek
        ,tgt.EnglishDayNameOfWeek = src.EnglishDayNameOfWeek
        ,tgt.DayNumberOfMonth = src.DayNumberOfMonth
        ,tgt.DayNumberOfYear = src.DayNumberOfYear
        ,tgt.WeekNumberOfYear = src.WeekNumberOfYear
        ,tgt.EnglishMonthName = src.EnglishMonthName
        ,tgt.MonthNumberOfYear = src.MonthNumberOfYear
        ,tgt.CalendarYear = src.CalendarYear
        ,tgt.FiscalYear = src.FiscalYear
        ,tgt.ChangeDatetime = CAST(GETDATE() AS DATETIME2(0))
    FROM 
        dbo.DimDate tgt
        INNER JOIN dbo.Staging_DimDate src 
            ON src.FullDateAlternateKey = tgt.FullDateAlternateKey    

    /* insert new records */
    INSERT INTO dbo.DimDate 
    SELECT 
         src.DateKey
        ,src.FullDateAlternateKey
        ,src.DayNumberOfWeek
        ,src.EnglishDayNameOfWeek
        ,src.DayNumberOfMonth
        ,src.DayNumberOfYear
        ,src.WeekNumberOfYear
        ,src.EnglishMonthName
        ,src.MonthNumberOfYear
        ,src.CalendarYear
        ,src.FiscalYear
        ,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
    FROM 
        dbo.Staging_DimDate src
        LEFT JOIN dbo.DimDate tgt
            ON src.FullDateAlternateKey = tgt.FullDateAlternateKey
    WHERE 
        tgt.FullDateAlternateKey IS NULL
    

    /* drop staging table */
    DROP TABLE IF EXISTS dbo.Staging_DimDate
  
END