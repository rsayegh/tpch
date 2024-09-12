CREATE TABLE [dbo].[DimCustomer] (

	[CustomerKey] int NOT NULL, 
	[CustomerCode] int NOT NULL, 
	[CustomerName] varchar(100) NOT NULL, 
	[CustomerAddress] varchar(100) NOT NULL, 
	[CustomerPhone] varchar(20) NOT NULL, 
	[CustomerBalance] decimal(15,2) NOT NULL, 
	[CustomerSegment] varchar(50) NOT NULL, 
	[CustomerComment] varchar(200) NULL, 
	[CustomerNation] varchar(100) NOT NULL, 
	[CustomerRegion] varchar(100) NOT NULL, 
	[CreateDatetime] datetime2(0) NOT NULL, 
	[ChangeDatetime] datetime2(0) NOT NULL, 
	[LoadId] int NOT NULL
);


GO
ALTER TABLE [dbo].[DimCustomer] ADD CONSTRAINT PK_DimCustomer primary key NONCLUSTERED ([CustomerKey]);
GO
ALTER TABLE [dbo].[DimCustomer] ADD CONSTRAINT UQ_DimCustomer_CustomerCode unique NONCLUSTERED ([CustomerCode]);