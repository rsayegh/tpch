CREATE PROC dbo.spLoadDimCurrency
AS
BEGIN
    /*
        sample call
        EXECUTE dbo.spLoadDimCurrency
    */


    DROP TABLE IF EXISTS dbo.Staging_DimCurrency

    CREATE TABLE dbo.Staging_DimCurrency
    AS
    /* NA member */
    SELECT 
        -1 AS CurrencyKey
        ,'NA' AS CurrencyAlternateKey
        ,'NA' AS CurrencyName
        ,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
    UNION ALL
    /* filtered currencies */
    SELECT 
        CurrencyKey
        ,CurrencyAlternateKey
        ,CurrencyName
        ,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
    FROM 
        raw.dbo.currency
    WHERE 
        CurrencyAlternateKey IN ('CHF', 'EUR', 'USD')
    ;

    /* update existing records */
    UPDATE tgt
    SET 
         tgt.CurrencyKey = src.CurrencyKey
        ,tgt.CurrencyAlternateKey = src.CurrencyAlternateKey
        ,tgt.CurrencyName = src.CurrencyName
        ,tgt.ChangeDatetime = CAST(GETDATE() AS DATETIME2(0))
    FROM 
        dbo.DimCurrency tgt
        INNER JOIN dbo.Staging_DimCurrency src 
            ON src.CurrencyKey = tgt.CurrencyKey    

    /* insert new records */
    INSERT INTO dbo.DimCurrency 
    SELECT 
         src.CurrencyKey
        ,src.CurrencyAlternateKey
        ,src.CurrencyName
        ,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
    FROM 
        dbo.Staging_DimCurrency src
        LEFT JOIN dbo.DimCurrency tgt
            ON src.CurrencyKey = tgt.CurrencyKey
    WHERE 
        tgt.CurrencyKey IS NULL

    
    /* drop staging table */
    DROP TABLE IF EXISTS dbo.Staging_DimCurrency
    
  
END