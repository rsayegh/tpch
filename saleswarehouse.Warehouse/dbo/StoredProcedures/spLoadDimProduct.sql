CREATE PROC dbo.spLoadDimProduct
AS
BEGIN
    /*
        sample call
        EXECUTE dbo.spLoadDimProduct
    */

    DROP TABLE IF EXISTS dbo.Staging_DimProduct

    CREATE TABLE dbo.Staging_DimProduct
    AS
    SELECT 
         product.ProductKey
        ,product.ProductAlternateKey
        ,product.EnglishProductName AS ProductName
        ,product.StandardCost
        ,product.FinishedGoodsFlag
        ,product.Color
        ,product.SafetyStockLevel
        ,product.ReorderPoint
        ,product.ListPrice
        ,product.SizeRange
        ,DaysToManufacture
        ,product.ProductLine
        ,product.DealerPrice
        ,product.Class
        ,product.ModelName
        ,product.EnglishDescription AS Description
        ,product.StartDate
        ,product.EndDate
        ,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
        ,subcategory.EnglishProductSubcategoryName AS ProductSubcategoryName
        ,category.EnglishProductCategoryName AS ProductCategoryName
    FROM 
        raw.dbo.product product
        INNER JOIN raw.dbo.productsubcategory subcategory
            ON subcategory.ProductSubcategoryKey = product.ProductSubcategoryKey
        INNER JOIN raw.dbo.productcategory category
            ON category.ProductCategoryKey = subcategory.ProductCategoryKey
    ;

    /* update existing records */
    UPDATE tgt
    SET 
         tgt.ProductKey = src.ProductKey
        ,tgt.ProductAlternateKey = src.ProductAlternateKey
        ,tgt.ProductName = src.ProductName
        ,tgt.StandardCost = src.StandardCost
        ,tgt.FinishedGoodsFlag = src.FinishedGoodsFlag
        ,tgt.Color = src.Color
        ,tgt.SafetyStockLevel = src.SafetyStockLevel
        ,tgt.ReorderPoint = src.ReorderPoint
        ,tgt.ListPrice = src.ListPrice
        ,tgt.SizeRange = src.SizeRange
        ,tgt.DaysToManufacture = src.DaysToManufacture
        ,tgt.ProductLine = src.ProductLine
        ,tgt.DealerPrice = src.DealerPrice
        ,tgt.Class = src.Class
        ,tgt.ModelName = src.ModelName
        ,tgt.Description = src.Description
        ,tgt.StartDate = src.StartDate
        ,tgt.EndDate = src.EndDate
        ,tgt.ChangeDatetime = CAST(GETDATE() AS DATETIME2(0))
        ,tgt.ProductSubcategoryName = src.ProductSubcategoryName
        ,tgt.ProductCategoryName = src.ProductCategoryName
    FROM 
        dbo.DimProduct tgt
        INNER JOIN dbo.Staging_DimProduct src 
            ON src.ProductKey = tgt.ProductKey    

    /* insert new records */
    INSERT INTO dbo.DimProduct 
    SELECT 
         src.ProductKey
        ,src.ProductAlternateKey
        ,src.ProductName
        ,src.StandardCost
        ,src.FinishedGoodsFlag
        ,src.Color
        ,src.SafetyStockLevel
        ,src.ReorderPoint
        ,src.ListPrice
        ,src.SizeRange
        ,src.DaysToManufacture
        ,src.ProductLine
        ,src.DealerPrice
        ,src.Class
        ,src.ModelName
        ,src.Description
        ,src.StartDate
        ,src.EndDate
        ,CAST(GETDATE() AS DATETIME2(0)) AS ChangeDatetime
        ,src.ProductSubcategoryName
        ,src.ProductCategoryName
    FROM 
        dbo.Staging_DimProduct src
        LEFT JOIN dbo.DimProduct tgt
            ON src.ProductKey = tgt.ProductKey
    WHERE 
        tgt.ProductKey IS NULL
    
    DROP TABLE IF EXISTS dbo.Staging_DimProduct

END