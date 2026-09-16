CREATE TABLE [dbo].[DimSuppliedPart] (
    [SuppliedPartKey]      INT             NOT NULL,
    [PartCode]             INT             NOT NULL,
    [SupplierCode]         INT             NOT NULL,
    [SuppliedPartQuantity] INT             NOT NULL,
    [SuppliedPartCost]     DECIMAL (15, 2) NULL,
    [SuppliedPartComment]  VARCHAR (200)   NULL,
    [PartName]             VARCHAR (100)   NULL,
    [PartManufacturer]     VARCHAR (100)   NULL,
    [PartBrand]            VARCHAR (100)   NULL,
    [PartType]             VARCHAR (100)   NULL,
    [PartSize]             INT             NOT NULL,
    [PartContainer]        VARCHAR (100)   NULL,
    [PartRetailPrice]      DECIMAL (15, 2) NULL,
    [PartComment]          VARCHAR (200)   NULL,
    [CreateDatetime]       DATETIME2 (0)   NOT NULL,
    [ChangeDatetime]       DATETIME2 (0)   NOT NULL,
    [LoadId]               INT             NOT NULL
);


GO