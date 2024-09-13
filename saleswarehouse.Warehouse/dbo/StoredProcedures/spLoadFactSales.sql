CREATE PROC dbo.spLoadFactSales(
    @OrderDateKey INT
)
AS
BEGIN
    /*
        sample call
        EXECUTE dbo.spLoadFactSales
            @OrderDateKey = '20130101'
    */

    DROP TABLE IF EXISTS dbo.Staging_FactSales

    CREATE TABLE dbo.Staging_FactSales
    AS
    SELECT 
         PartitionKey
        ,ProductKey
        ,OrderDateKey
        ,CurrencyKey
        ,OrderQuantity 
        ,UnitPrice 
        ,ExtendedAmount 
        ,UnitPriceDiscountPct 
        ,DiscountAmount
        ,ProductStandardCost 
        ,TotalProductCost 
        ,SalesAmount 
        ,TaxAmt 
    FROM 
        raw.dbo.sales
    WHERE 
        OrderDateKey = @OrderDateKey
    ;

    DELETE FROM dbo.FactSales
    WHERE 
        OrderDateKey = @OrderDateKey

    INSERT INTO dbo.FactSales
    SELECT * 
    FROM dbo.Staging_FactSales

    DROP TABLE IF EXISTS dbo.Staging_FactSales

END