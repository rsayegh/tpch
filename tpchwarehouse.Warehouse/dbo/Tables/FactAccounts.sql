CREATE TABLE [dbo].[FactAccounts] (
    [DateKey]                  INT             NULL,
    [ProductRevenue]           DECIMAL (38, 8) NULL,
    [ServiceAndOtherRevenue]   DECIMAL (38, 8) NULL,
    [Revenue]                  DECIMAL (38, 8) NULL,
    [ProductCost]              DECIMAL (38, 8) NULL,
    [ServiceAndOtherCosts]     DECIMAL (38, 8) NULL,
    [GrossMargin]              DECIMAL (38, 8) NULL,
    [ResearchAndDevelopment]   DECIMAL (38, 8) NULL,
    [SalesAndMarketing]        DECIMAL (38, 8) NULL,
    [GeneralAndAdministrative] DECIMAL (38, 8) NULL,
    [Restructuring]            DECIMAL (38, 8) NULL,
    [OperatingIncome]          DECIMAL (38, 8) NULL,
    [OtherIncomeNet]           DECIMAL (38, 8) NULL,
    [IncomeBeforeIncomeTaxes]  DECIMAL (38, 8) NULL,
    [ProvisionForIncomeTaxes]  DECIMAL (38, 8) NULL,
    [NetIncome]                DECIMAL (38, 8) NULL
);


GO