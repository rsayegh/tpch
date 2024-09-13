CREATE TABLE [dbo].[DimProduct] (

	[ProductKey] int NULL, 
	[ProductAlternateKey] varchar(8000) NULL, 
	[ProductName] varchar(8000) NULL, 
	[StandardCost] decimal(19,4) NULL, 
	[FinishedGoodsFlag] bit NULL, 
	[Color] varchar(8000) NULL, 
	[SafetyStockLevel] int NULL, 
	[ReorderPoint] int NULL, 
	[ListPrice] decimal(19,4) NULL, 
	[SizeRange] varchar(8000) NULL, 
	[DaysToManufacture] int NULL, 
	[ProductLine] varchar(8000) NULL, 
	[DealerPrice] decimal(19,4) NULL, 
	[Class] varchar(8000) NULL, 
	[ModelName] varchar(8000) NULL, 
	[Description] varchar(8000) NULL, 
	[StartDate] datetime2(6) NULL, 
	[EndDate] datetime2(6) NULL, 
	[ChangeDatetime] datetime2(0) NULL, 
	[ProductSubcategoryName] varchar(8000) NULL, 
	[ProductCategoryName] varchar(8000) NULL
);

