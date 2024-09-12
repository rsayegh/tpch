CREATE TABLE [dbo].[FactOrderLines] (

	[CommitDateKey] int NULL, 
	[CustomerKey] int NOT NULL, 
	[OrderDateKey] int NULL, 
	[ReceiptDateKey] int NULL, 
	[ShipDateKey] int NULL, 
	[SuppliedPartKey] int NOT NULL, 
	[SupplierKey] int NOT NULL, 
	[OrderCode] int NOT NULL, 
	[LineNumber] int NULL, 
	[Quantity] decimal(15,2) NULL, 
	[ExtendedPrice] decimal(15,2) NULL, 
	[Discount] decimal(15,2) NULL, 
	[Tax] decimal(15,2) NULL, 
	[ReturnFlag] varchar(50) NULL, 
	[LineStatus] varchar(50) NULL, 
	[ShipInstruct] varchar(50) NULL, 
	[ShipMode] varchar(50) NULL, 
	[LineItemComment] varchar(200) NULL, 
	[OrderStatus] varchar(50) NULL, 
	[OrderTotalPrice] decimal(15,2) NULL, 
	[OrderPriority] varchar(50) NULL, 
	[OrderClerk] varchar(50) NULL, 
	[OrderShipPriority] varchar(50) NULL, 
	[OrderComment] varchar(200) NULL, 
	[LoadId] int NULL
);

