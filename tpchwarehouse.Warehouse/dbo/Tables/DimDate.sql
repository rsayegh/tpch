CREATE TABLE [dbo].[DimDate] (

	[DateKey] int NOT NULL, 
	[Date] date NOT NULL, 
	[DayName] varchar(10) NOT NULL, 
	[DayOfMonth] smallint NOT NULL, 
	[DayOfYear] smallint NOT NULL, 
	[Month] smallint NOT NULL, 
	[MonthName] varchar(10) NOT NULL, 
	[MonthOfQuarter] smallint NOT NULL, 
	[MonthYear] int NOT NULL, 
	[Quarter] smallint NOT NULL, 
	[QuarterName] varchar(2) NOT NULL, 
	[Year] smallint NOT NULL, 
	[YearName] varchar(10) NOT NULL, 
	[CreateDatetime] datetime2(0) NOT NULL, 
	[ChangeDatetime] datetime2(0) NOT NULL, 
	[LoadId] int NOT NULL
);

