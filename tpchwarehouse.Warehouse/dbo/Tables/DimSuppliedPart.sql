CREATE TABLE [dbo].[DimSuppliedPart] (

	[SuppliedPartKey] int NOT NULL, 
	[PartCode] int NOT NULL, 
	[SupplierCode] int NOT NULL, 
	[SuppliedPartQuantity] int NOT NULL, 
	[SuppliedPartCost] decimal(15,2) NULL, 
	[SuppliedPartComment] varchar(200) NULL, 
	[PartName] varchar(100) NULL, 
	[PartManufacturer] varchar(100) NULL, 
	[PartBrand] varchar(100) NULL, 
	[PartType] varchar(100) NULL, 
	[PartSize] int NOT NULL, 
	[PartContainer] varchar(100) NULL, 
	[PartRetailPrice] decimal(15,2) NULL, 
	[PartComment] varchar(200) NULL, 
	[CreateDatetime] datetime2(0) NOT NULL, 
	[ChangeDatetime] datetime2(0) NOT NULL, 
	[LoadId] int NOT NULL
);

