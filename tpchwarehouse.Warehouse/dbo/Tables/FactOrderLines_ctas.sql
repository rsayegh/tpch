CREATE TABLE [dbo].[FactOrderLines_ctas] (
    [CommitDateKey]     INT             NULL,
    [CustomerKey]       INT             NOT NULL,
    [OrderDateKey]      INT             NULL,
    [ReceiptDateKey]    INT             NULL,
    [ShipDateKey]       INT             NULL,
    [SuppliedPartKey]   INT             NOT NULL,
    [SupplierKey]       INT             NOT NULL,
    [OrderCode]         BIGINT          NOT NULL,
    [LineNumber]        BIGINT          NULL,
    [Quantity]          DECIMAL (15, 2) NULL,
    [ExtendedPrice]     DECIMAL (15, 2) NULL,
    [Discount]          DECIMAL (15, 2) NULL,
    [Tax]               DECIMAL (15, 2) NULL,
    [ReturnFlag]        VARCHAR (50)    NULL,
    [LineStatus]        VARCHAR (50)    NULL,
    [ShipInstruct]      VARCHAR (50)    NULL,
    [ShipMode]          VARCHAR (50)    NULL,
    [LineItemComment]   VARCHAR (200)   NULL,
    [OrderStatus]       VARCHAR (50)    NULL,
    [OrderTotalPrice]   DECIMAL (15, 2) NULL,
    [OrderPriority]     VARCHAR (50)    NULL,
    [OrderClerk]        VARCHAR (50)    NULL,
    [OrderShipPriority] VARCHAR (50)    NULL,
    [OrderComment]      VARCHAR (200)   NULL,
    [LoadId]            INT             NULL
);


GO