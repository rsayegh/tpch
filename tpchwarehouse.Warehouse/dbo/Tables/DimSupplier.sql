CREATE TABLE [dbo].[DimSupplier] (

	[SupplierKey] int NOT NULL, 
	[SupplierCode] int NOT NULL, 
	[SupplierName] varchar(100) NOT NULL, 
	[SupplierAddress] varchar(100) NOT NULL, 
	[SupplierPhone] varchar(20) NOT NULL, 
	[SupplierBalance] decimal(15,2) NOT NULL, 
	[SupplierComment] varchar(200) NULL, 
	[SupplierNation] varchar(100) NOT NULL, 
	[SupplierRegion] varchar(100) NOT NULL, 
	[CreateDatetime] datetime2(0) NOT NULL, 
	[ChangeDatetime] datetime2(0) NOT NULL, 
	[LoadId] int NOT NULL
);


GO
ALTER TABLE [dbo].[DimSupplier] ADD CONSTRAINT PKDimSupplier primary key NONCLUSTERED ([SupplierKey]);
GO
ALTER TABLE [dbo].[DimSupplier] ADD CONSTRAINT UQ_DimSupplier_SupplierCode unique NONCLUSTERED ([SupplierCode]);