CREATE VIEW dbo.vwDimSupplier
AS
SELECT TOP (1000) [SupplierKey]
      ,[SupplierCode]
      ,[SupplierName]
      ,[SupplierAddress]
      ,[SupplierPhone]
      ,[SupplierBalance]
      ,[SupplierComment]
      ,[SupplierNation]
      ,[SupplierRegion]
      ,[CreateDatetime]
      ,[ChangeDatetime]
      ,[LoadId]
  FROM [dbo].[DimSupplier]
;