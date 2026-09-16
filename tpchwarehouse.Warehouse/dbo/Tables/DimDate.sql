CREATE TABLE [dbo].[DimDate] (
    [DateKey]        INT           NOT NULL,
    [Date]           DATE          NOT NULL,
    [DayName]        VARCHAR (10)  NOT NULL,
    [DayOfMonth]     SMALLINT      NOT NULL,
    [DayOfYear]      SMALLINT      NOT NULL,
    [Month]          SMALLINT      NOT NULL,
    [MonthName]      VARCHAR (10)  NOT NULL,
    [MonthOfQuarter] SMALLINT      NOT NULL,
    [MonthYear]      INT           NOT NULL,
    [Quarter]        SMALLINT      NOT NULL,
    [QuarterName]    VARCHAR (2)   NOT NULL,
    [Year]           SMALLINT      NOT NULL,
    [YearName]       VARCHAR (10)  NOT NULL,
    [CreateDatetime] DATETIME2 (0) NOT NULL,
    [ChangeDatetime] DATETIME2 (0) NOT NULL,
    [LoadId]         INT           NOT NULL
);


GO