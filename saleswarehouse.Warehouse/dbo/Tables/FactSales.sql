CREATE TABLE [dbo].[FactSales] (

	[PartitionKey] int NULL, 
	[ProductKey] int NULL, 
	[OrderDateKey] int NULL, 
	[CurrencyKey] int NULL, 
	[OrderQuantity] int NULL, 
	[UnitPrice] float NULL, 
	[ExtendedAmount] float NULL, 
	[UnitPriceDiscountPct] float NULL, 
	[DiscountAmount] float NULL, 
	[ProductStandardCost] float NULL, 
	[TotalProductCost] float NULL, 
	[SalesAmount] float NULL, 
	[TaxAmt] float NULL
);

