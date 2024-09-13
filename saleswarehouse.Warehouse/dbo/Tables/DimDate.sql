CREATE TABLE [dbo].[DimDate] (

	[DateKey] int NULL, 
	[FullDateAlternateKey] date NULL, 
	[DayNumberOfWeek] int NULL, 
	[EnglishDayNameOfWeek] varchar(8000) NULL, 
	[DayNumberOfMonth] int NULL, 
	[DayNumberOfYear] int NULL, 
	[WeekNumberOfYear] int NULL, 
	[EnglishMonthName] varchar(8000) NULL, 
	[MonthNumberOfYear] int NULL, 
	[CalendarYear] int NULL, 
	[FiscalYear] int NULL, 
	[ChangeDatetime] datetime2(0) NULL
);

