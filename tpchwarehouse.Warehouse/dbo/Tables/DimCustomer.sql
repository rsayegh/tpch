CREATE TABLE [dbo].[DimCustomer] (
    [CustomerKey]     INT             NOT NULL,
    [CustomerCode]    INT             NOT NULL,
    [CustomerName]    VARCHAR (100)   NOT NULL,
    [CustomerAddress] VARCHAR (100)   NOT NULL,
    [CustomerPhone]   VARCHAR (20)    NOT NULL,
    [CustomerBalance] DECIMAL (15, 2) NOT NULL,
    [CustomerSegment] VARCHAR (50)    NOT NULL,
    [CustomerComment] VARCHAR (200)   NULL,
    [CustomerNation]  VARCHAR (100)   NOT NULL,
    [CustomerRegion]  VARCHAR (100)   NOT NULL,
    [CreateDatetime]  DATETIME2 (0)   NOT NULL,
    [ChangeDatetime]  DATETIME2 (0)   NOT NULL,
    [LoadId]          INT             NOT NULL
);


GO

ALTER TABLE [dbo].[DimCustomer]
    ADD CONSTRAINT [PK_DimCustomer] PRIMARY KEY NONCLUSTERED ([CustomerKey] ASC) NOT ENFORCED;


GO

ALTER TABLE [dbo].[DimCustomer]
    ADD CONSTRAINT [UQ_DimCustomer_CustomerCode] UNIQUE NONCLUSTERED ([CustomerCode] ASC) NOT ENFORCED;


GO