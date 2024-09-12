CREATE VIEW dbo.vwDimCustomer
AS
SELECT TOP (1000) [CustomerKey]
      ,[CustomerCode]
      ,[CustomerName]
      ,[CustomerAddress]
      ,[CustomerPhone]
      ,[CustomerBalance]
      ,[CustomerSegment]
      ,[CustomerComment]
      ,[CustomerNation]
      ,[CustomerRegion]
      ,[CreateDatetime]
      ,[ChangeDatetime]
      ,[LoadId]
  FROM [dbo].[DimCustomer]
;