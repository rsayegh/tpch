CREATE TABLE [dbo].[FactAccounts] (

	[DateKey] int NULL, 
	[ProductRevenue] decimal(38,8) NULL, 
	[ServiceAndOtherRevenue] decimal(38,8) NULL, 
	[Revenue] decimal(38,8) NULL, 
	[ProductCost] decimal(38,8) NULL, 
	[ServiceAndOtherCosts] decimal(38,8) NULL, 
	[GrossMargin] decimal(38,8) NULL, 
	[ResearchAndDevelopment] decimal(38,8) NULL, 
	[SalesAndMarketing] decimal(38,8) NULL, 
	[GeneralAndAdministrative] decimal(38,8) NULL, 
	[Restructuring] decimal(38,8) NULL, 
	[OperatingIncome] decimal(38,8) NULL, 
	[OtherIncomeNet] decimal(38,8) NULL, 
	[IncomeBeforeIncomeTaxes] decimal(38,8) NULL, 
	[ProvisionForIncomeTaxes] decimal(38,8) NULL, 
	[NetIncome] decimal(38,8) NULL
);

