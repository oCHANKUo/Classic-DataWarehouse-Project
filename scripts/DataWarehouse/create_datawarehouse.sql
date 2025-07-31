USE DataWarehouseClassic
GO
-- ===Fact Table===============================================
-- Create FactSalesOrderDetail table
IF OBJECT_ID('FactSalesOrderDetail', 'U') IS NOT NULL
    DROP TABLE FactSalesOrderDetail;

CREATE TABLE FactSalesOrderDetail (
    SalesOrderDetailID INT PRIMARY KEY,
    SalesOrderID INT,
    ProductID INT,
    CustomerID INT,
    SalesPersonID NVARCHAR(5),
    TerritoryID INT,
    OrderDateKey INT,
    DueDateKey INT,
    ShipDateKey INT,
    CarrierTrackingNumber NVARCHAR(50),
    OrderQTY INT,
    UnitPrice MONEY,
    UnitPriceDiscount MONEY,
    LineTotal DECIMAL(18,2),
    Freight MONEY,
    TaxAmt MONEY,
    TotalDue MONEY
);

-- ==Dimension Tables============================================



-- Create DimCustomer Table
IF OBJECT_ID('DimCustomer', 'U') IS NOT NULL
    DROP TABLE DimCustomer;

CREATE TABLE DimCustomer (
    CustomerID INT PRIMARY KEY,
    FullName NVARCHAR(100),
    Title NVARCHAR(10),
    Gender NVARCHAR(10),
    PhoneNumber NVARCHAR(20),
    EmailAddress NVARCHAR(50),
    AddressType NVARCHAR(50),
    AddressLine NVARCHAR(50),
    --AddressLine2 NVARCHAR(100),
    City NVARCHAR(50),
    StateProvinceName NVARCHAR(50),
    PostalCode NVARCHAR(20),
    CountryRegionName NVARCHAR(50),
    EmailPromotion NVARCHAR(25)
);
-- ======================================


-- Create DimProduct table
IF OBJECT_ID('DimProduct', 'U') IS NOT NULL
    DROP TABLE DimProduct;

CREATE TABLE DimProduct (
    ProductID INT PRIMARY KEY,
    ProductName NVARCHAR(50),
    ProductNumber NVARCHAR(25),
    Color NVARCHAR(20),
    Size NVARCHAR(10),
	SizeUnitMeasureCode NVARCHAR(10),
    StandardCost MONEY,
    ListPrice MONEY,
    Weight NVARCHAR(10),
	WeightUnitMeasureCode NVARCHAR(10),
    CategoryName NVARCHAR(50),
    SubCategoryName NVARCHAR(50)
);



-- Create DimSalesPerson table
IF OBJECT_ID('DimSalesPerson', 'U') IS NOT NULL
    DROP TABLE DimSalesPerson;

CREATE TABLE DimSalesPerson (
    SalesPersonID INT PRIMARY KEY,
    TerritoryID NVARCHAR(15),
    SalesQuota MONEY,
    Bonus MONEY,
    CommissionPct MONEY,
    SalesYTD MONEY,
    SalesLastYear MONEY
);


-- Create DimTerritory Table
IF OBJECT_ID('DimTerritory', 'U') IS NOT NULL
    DROP TABLE DimTerritory;

CREATE TABLE DimTerritory (
    TerritoryID INT PRIMARY KEY,
    TerritoryName NVARCHAR(50),
    CountryRegionCode NVARCHAR(10),
    RegionGroup NVARCHAR(50)
);



-- Create DimDate table
BEGIN TRY
	DROP TABLE [DimDate]
END TRY

BEGIN CATCH
	/*No Action*/
END CATCH

/**********************************************************************************/

CREATE TABLE	[DimDate]
	(	[DateKey] INT primary key, 
		[Date] DATETIME,
		[FullDateUK] CHAR(10), -- Date in dd-MM-yyyy format
		[FullDateUSA] CHAR(10),-- Date in MM-dd-yyyy format
		[DayOfMonth] VARCHAR(2), -- Field will hold day number of Month
		[DaySuffix] VARCHAR(4), -- Apply suffix as 1st, 2nd ,3rd etc
		[DayName] VARCHAR(9), -- Contains name of the day, Sunday, Monday 
		[DayOfWeekUSA] CHAR(1),-- First Day Sunday=1 and Saturday=7
		[DayOfWeekUK] CHAR(1),-- First Day Monday=1 and Sunday=7
		[DayOfWeekInMonth] VARCHAR(2), --1st Monday or 2nd Monday in Month
		[DayOfWeekInYear] VARCHAR(2),
		[DayOfQuarter] VARCHAR(3),
		[DayOfYear] VARCHAR(3),
		[WeekOfMonth] VARCHAR(1),-- Week Number of Month 
		[WeekOfQuarter] VARCHAR(2), --Week Number of the Quarter
		[WeekOfYear] VARCHAR(2),--Week Number of the Year
		[Month] VARCHAR(2), --Number of the Month 1 to 12
		[MonthName] VARCHAR(9),--January, February etc
		[MonthOfQuarter] VARCHAR(2),-- Month Number belongs to Quarter
		[Quarter] CHAR(1),
		[QuarterName] VARCHAR(9),--First,Second..
		[Year] CHAR(4),-- Year value of Date stored in Row
		[YearName] CHAR(7), --CY 2012,CY 2013
		[MonthYear] CHAR(10), --Jan-2013,Feb-2013
		[MMYYYY] CHAR(6),
		[FirstDayOfMonth] DATE,
		[LastDayOfMonth] DATE,
		[FirstDayOfQuarter] DATE,
		[LastDayOfQuarter] DATE,
		[FirstDayOfYear] DATE,
		[LastDayOfYear] DATE,
		[IsHolidaySL] BIT,-- Flag 1=National Holiday, 0-No National Holiday
		[IsWeekday] BIT,-- 0=Week End ,1=Week Day
		[HolidaySL] VARCHAR(50),--Name of Holiday in US
		[isCurrentDay] int, -- Current day=1 else = 0
		[isDataAvailable] int, -- data available for the day = 1, no data available for the day = 0
		[isLatestDataAvailable] int
	)


/********************************************************************************************/
--Specify Start Date and End date here
--Value of Start Date Must be Less than Your End Date 

DECLARE @StartDate DATETIME = '01/01/1990' --Starting value of Date Range
DECLARE @EndDate DATETIME = '01/01/2099' --End Value of Date Range

--Temporary Variables To Hold the Values During Processing of Each Date of Year
DECLARE
	@DayOfWeekInMonth INT,
	@DayOfWeekInYear INT,
	@DayOfQuarter INT,
	@WeekOfMonth INT,
	@CurrentYear INT,
	@CurrentMonth INT,
	@CurrentQuarter INT

/*Table Data type to store the day of week count for the month and year*/
DECLARE @DayOfWeek TABLE (DOW INT, MonthCount INT, QuarterCount INT, YearCount INT)

INSERT INTO @DayOfWeek VALUES (1, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (2, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (3, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (4, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (5, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (6, 0, 0, 0)
INSERT INTO @DayOfWeek VALUES (7, 0, 0, 0)

--Extract and assign various parts of Values from Current Date to Variable

DECLARE @CurrentDate AS DATETIME = @StartDate
SET @CurrentMonth = DATEPART(MM, @CurrentDate)
SET @CurrentYear = DATEPART(YY, @CurrentDate)
SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)

/********************************************************************************************/
--Proceed only if Start Date(Current date ) is less than End date you specified above

WHILE @CurrentDate < @EndDate
BEGIN
 
/*Begin day of week logic*/

         /*Check for Change in Month of the Current date if Month changed then 
          Change variable value*/
	IF @CurrentMonth != DATEPART(MM, @CurrentDate) 
	BEGIN
		UPDATE @DayOfWeek
		SET MonthCount = 0
		SET @CurrentMonth = DATEPART(MM, @CurrentDate)
	END

        /* Check for Change in Quarter of the Current date if Quarter changed then change 
         Variable value*/

	IF @CurrentQuarter != DATEPART(QQ, @CurrentDate)
	BEGIN
		UPDATE @DayOfWeek
		SET QuarterCount = 0
		SET @CurrentQuarter = DATEPART(QQ, @CurrentDate)
	END
       
        /* Check for Change in Year of the Current date if Year changed then change 
         Variable value*/
	

	IF @CurrentYear != DATEPART(YY, @CurrentDate)
	BEGIN
		UPDATE @DayOfWeek
		SET YearCount = 0
		SET @CurrentYear = DATEPART(YY, @CurrentDate)
	END
	
        -- Set values in table data type created above from variables 

	UPDATE @DayOfWeek
	SET 
		MonthCount = MonthCount + 1,
		QuarterCount = QuarterCount + 1,
		YearCount = YearCount + 1
	WHERE DOW = DATEPART(DW, @CurrentDate)

	SELECT
		@DayOfWeekInMonth = MonthCount,
		@DayOfQuarter = QuarterCount,
		@DayOfWeekInYear = YearCount
	FROM @DayOfWeek
	WHERE DOW = DATEPART(DW, @CurrentDate)
	
/*End day of week logic*/


/* Populate Your Dimension Table with values*/
	
	INSERT INTO [DimDate]
	SELECT
		
		CONVERT (char(8),@CurrentDate,112) as DateKey,
		@CurrentDate AS Date,
		CONVERT (char(10),@CurrentDate,103) as FullDateUK,
		CONVERT (char(10),@CurrentDate,101) as FullDateUSA,
		DATEPART(DD, @CurrentDate) AS DayOfMonth,
		--Apply Suffix values like 1st, 2nd 3rd etc..
		CASE 
			WHEN DATEPART(DD,@CurrentDate) IN (11,12,13) 
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'th'
			WHEN RIGHT(DATEPART(DD,@CurrentDate),1) = 1 
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'st'
			WHEN RIGHT(DATEPART(DD,@CurrentDate),1) = 2 
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'nd'
			WHEN RIGHT(DATEPART(DD,@CurrentDate),1) = 3 
			THEN CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'rd'
			ELSE CAST(DATEPART(DD,@CurrentDate) AS VARCHAR) + 'th' 
			END AS DaySuffix,
		
		DATENAME(DW, @CurrentDate) AS DayName,
		DATEPART(DW, @CurrentDate) AS DayOfWeekUSA,

		-- check for day of week as Per US and change it as per UK format 
		CASE DATEPART(DW, @CurrentDate)
			WHEN 1 THEN 7
			WHEN 2 THEN 1
			WHEN 3 THEN 2
			WHEN 4 THEN 3
			WHEN 5 THEN 4
			WHEN 6 THEN 5
			WHEN 7 THEN 6
			END 
			AS DayOfWeekUK,
		
		@DayOfWeekInMonth AS DayOfWeekInMonth,
		@DayOfWeekInYear AS DayOfWeekInYear,
		@DayOfQuarter AS DayOfQuarter,
		DATEPART(DY, @CurrentDate) AS DayOfYear,
		DATEPART(WW, @CurrentDate) + 1 - DATEPART(WW, CONVERT(VARCHAR, 
		DATEPART(MM, @CurrentDate)) + '/1/' + CONVERT(VARCHAR, 
		DATEPART(YY, @CurrentDate))) AS WeekOfMonth,
		(DATEDIFF(DD, DATEADD(QQ, DATEDIFF(QQ, 0, @CurrentDate), 0), 
		@CurrentDate) / 7) + 1 AS WeekOfQuarter,
		DATEPART(WW, @CurrentDate) AS WeekOfYear,
		DATEPART(MM, @CurrentDate) AS Month,
		DATENAME(MM, @CurrentDate) AS MonthName,
		CASE
			WHEN DATEPART(MM, @CurrentDate) IN (1, 4, 7, 10) THEN 1
			WHEN DATEPART(MM, @CurrentDate) IN (2, 5, 8, 11) THEN 2
			WHEN DATEPART(MM, @CurrentDate) IN (3, 6, 9, 12) THEN 3
			END AS MonthOfQuarter,
		DATEPART(QQ, @CurrentDate) AS Quarter,
		CASE DATEPART(QQ, @CurrentDate)
			WHEN 1 THEN 'First'
			WHEN 2 THEN 'Second'
			WHEN 3 THEN 'Third'
			WHEN 4 THEN 'Fourth'
			END AS QuarterName,
		DATEPART(YEAR, @CurrentDate) AS Year,
		'CY ' + CONVERT(VARCHAR, DATEPART(YEAR, @CurrentDate)) AS YearName,
		LEFT(DATENAME(MM, @CurrentDate), 3) + '-' + CONVERT(VARCHAR, 
		DATEPART(YY, @CurrentDate)) AS MonthYear,
		RIGHT('0' + CONVERT(VARCHAR, DATEPART(MM, @CurrentDate)),2) + 
		CONVERT(VARCHAR, DATEPART(YY, @CurrentDate)) AS MMYYYY,
		CONVERT(DATETIME, CONVERT(DATE, DATEADD(DD, - (DATEPART(DD, 
		@CurrentDate) - 1), @CurrentDate))) AS FirstDayOfMonth,
		CONVERT(DATETIME, CONVERT(DATE, DATEADD(DD, - (DATEPART(DD, 
		(DATEADD(MM, 1, @CurrentDate)))), DATEADD(MM, 1, 
		@CurrentDate)))) AS LastDayOfMonth,
		DATEADD(QQ, DATEDIFF(QQ, 0, @CurrentDate), 0) AS FirstDayOfQuarter,
		DATEADD(QQ, DATEDIFF(QQ, -1, @CurrentDate), -1) AS LastDayOfQuarter,
		CONVERT(DATETIME, '01/01/' + CONVERT(VARCHAR, DATEPART(YY, 
		@CurrentDate))) AS FirstDayOfYear,
		CONVERT(DATETIME, '12/31/' + CONVERT(VARCHAR, DATEPART(YY, 
		@CurrentDate))) AS LastDayOfYear,
		NULL AS IsHolidaySL,
		CASE DATEPART(DW, @CurrentDate)
			WHEN 1 THEN 0
			WHEN 2 THEN 1
			WHEN 3 THEN 1
			WHEN 4 THEN 1
			WHEN 5 THEN 1
			WHEN 6 THEN 1
			WHEN 7 THEN 0
			END AS IsWeekday,
		NULL AS HolidaySL, (case when @CurrentDate = convert(date, sysdatetime()) then 1 else 0 end), 0, 0

	SET @CurrentDate = DATEADD(DD, 1, @CurrentDate)
END

/********************************************************************************************/
 
/*****************************************************************************************/
