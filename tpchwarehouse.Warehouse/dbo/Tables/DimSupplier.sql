CREATE TABLE [dbo].[DimSupplier] (
    [SupplierKey]     INT             NOT NULL,
    [SupplierCode]    INT             NOT NULL,
    [SupplierName]    VARCHAR (100)   NOT NULL,
    [SupplierAddress] VARCHAR (100)   NOT NULL,
    [SupplierPhone]   VARCHAR (20)    NOT NULL,
    [SupplierBalance] DECIMAL (15, 2) NOT NULL,
    [SupplierComment] VARCHAR (200)   NULL,
    [SupplierNation]  VARCHAR (100)   NOT NULL,
    [SupplierRegion]  VARCHAR (100)   NOT NULL,
    [CreateDatetime]  DATETIME2 (0)   NOT NULL,
    [ChangeDatetime]  DATETIME2 (0)   NOT NULL,
    [LoadId]          INT             NOT NULL
);


GO

ALTER TABLE [dbo].[DimSupplier]
    ADD CONSTRAINT [PKDimSupplier] PRIMARY KEY NONCLUSTERED ([SupplierKey] ASC) NOT ENFORCED;


GO

ALTER TABLE [dbo].[DimSupplier]
    ADD CONSTRAINT [UQ_DimSupplier_SupplierCode] UNIQUE NONCLUSTERED ([SupplierCode] ASC) NOT ENFORCED;


GO