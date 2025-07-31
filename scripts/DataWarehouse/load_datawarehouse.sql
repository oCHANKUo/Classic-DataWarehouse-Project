USE DataWarehouseClassic
GO

IF OBJECT_ID('dbo.load_datawarehouse', 'P') IS NOT NULL
    DROP PROCEDURE dbo.load_datawarehouse;
GO

CREATE PROCEDURE dbo.load_datawarehouse
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY

        -- Insert Data to DimCustomer
        TRUNCATE TABLE DataWarehouseClassic.dbo.DimCustomer;
        INSERT INTO DataWarehouseClassic.dbo.DimCustomer (
            CustomerID,
            FullName,
            Title,
            Gender,
            PhoneNumber,
            EmailAddress,
            AddressType,
            AddressLine,
            City,
            StateProvinceName,
            PostalCode,
            CountryRegionName,
            EmailPromotion
        )
        SELECT
            ic.CustomerID,
            CONCAT(ISNULL(ic.FirstName, ''), ' ', ISNULL(ic.LastName, '')) AS FulltName,
            ISNULL(ic.Title, 'N/A') AS Title,
            CASE 
                WHEN ic.Gender = 'F' THEN 'Female'
                WHEN ic.Gender = 'M' THEN 'Male'
                ELSE 'N/A'
            END AS Gender,
            ic.PhoneNumber,
            ic.EmailAddress,
            ca.AddressType,
            ca.AddressLine1,
            ca.City,
            ca.StateProvinceName,
            ca.PostalCode,
            ca.CountryRegionName,
            CASE 
                WHEN ic.EmailPromotion = 0 THEN '(0) No Promotions'
                WHEN ic.EmailPromotion = 1 THEN '(1) Promotions'
                WHEN ic.EmailPromotion = 2 THEN '(2) Priority Promotions'
                ELSE 'Unknown'
            END AS EmailPromotion
        FROM Staging.dbo.IndividualCustomer ic
        LEFT JOIN Staging.dbo.CustomerAddress ca 
            ON ic.CustomerID = ca.CustomerID;

    
        -- Insert Data to DimProduct
        TRUNCATE TABLE DataWarehouseClassic.dbo.DimProduct;
        INSERT INTO DataWarehouseClassic.dbo.DimProduct (
            ProductID,
            ProductName,
            ProductNumber,
            Color,
            Size,
            SizeUnitMeasureCode,
            StandardCost,
            ListPrice,
            Weight,
            WeightUnitMeasureCode,
            CategoryName,
            SubCategoryName
        )
        SELECT
            p.ProductID,
            p.Name AS ProductName,
            p.ProductNumber,
            CASE 
                WHEN p.Color = 'Multi' THEN 'Multicolor'
                WHEN p.Color IS NULL THEN 'N/A'
                ELSE p.Color
            END AS Color,
            COALESCE(CAST(p.Size AS NVARCHAR(10)), 'N/A') AS Size,
            COALESCE(p.SizeUnitMeasureCode, 'N/A') AS SizeUnitMeasureCode,
            p.StandardCost,
            p.ListPrice,
            COALESCE(CAST(p.Weight AS NVARCHAR(10)), 'N/A') AS Weight,
            COALESCE(p.WeightUnitMeasureCode, 'N/A') AS WeightUnitMeasureCode,
            COALESCE(LTRIM(RTRIM(pc.Name)), 'Uncategorized') AS CategoryName,
            COALESCE(psc.Name, 'Uncategorized') AS SubCategoryName
        FROM Staging.dbo.Product p
        LEFT JOIN Staging.dbo.ProductSubCategory psc ON p.ProductSubCategoryID = psc.ProductSubCategoryID
        LEFT JOIN Staging.dbo.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID;


        -- Insert Data to DimSalesPerson
        TRUNCATE TABLE DataWarehouseClassic.dbo.DimSalesPerson;
        INSERT INTO DataWarehouseClassic.dbo.DimSalesPerson (
            SalesPersonID,
            TerritoryID,
            SalesQuota,
            Bonus,
            CommissionPct,
            SalesYTD,
            SalesLastYear
        )
        SELECT
            sp.BusinessEntityID,
            COALESCE(CAST(sp.TerritoryID AS NVARCHAR(20)), 'Unassigned') AS TerritoryID,
            COALESCE(sp.SalesQuota, 0.00) AS SalesQuota,
            sp.Bonus,
            sp.CommissionPct,
            sp.SalesYTD,
            sp.SalesLastYear
        FROM Staging.dbo.SalesPerson sp;


        -- Insert Data to DimTerritory
        TRUNCATE TABLE DataWarehouseClassic.dbo.DimTerritory;
        INSERT INTO DataWarehouseClassic.dbo.DimTerritory (
            TerritoryID,
            TerritoryName,
            CountryRegionCode,
            RegionGroup
        )
        SELECT
            TerritoryID,
            LTRIM(RTRIM(Name)) AS TerritoryName,
            LTRIM(RTRIM(CountryRegionCode)) AS CountryRegionCode,
            LTRIM(RTRIM(RegionGroup)) AS RegionGroup
        FROM Staging.dbo.SalesTerritory;


        -- Insert Data to FactSalesOrderDetail
        TRUNCATE TABLE DataWarehouseClassic.dbo.FactSalesOrderDetail;
        INSERT INTO DataWarehouseClassic.dbo.FactSalesOrderDetail (
            SalesOrderDetailID,
            SalesOrderID,
            ProductID,
            CustomerID,
            SalesPersonID,
            TerritoryID,
            OrderDateKey,
            DueDateKey,
            ShipDateKey,
            CarrierTrackingNumber,
            OrderQTY,
            UnitPrice,
            UnitPriceDiscount,
            LineTotal,
            Freight,
            TaxAmt,
            TotalDue
        )
        SELECT 
            sod.SalesOrderDetailID,
            sod.SalesOrderID,
            sod.ProductID,
            soh.CustomerID,
            COALESCE(CAST(soh.SalesPersonID AS NVARCHAR(5)), 'N/A') AS SalesPersonID,
            soh.TerritoryID,

            -- Date dimension foreign keys
            dd_order.DateKey AS OrderDateKey,
            dd_due.DateKey AS DueDateKey,
            dd_ship.DateKey AS ShipDateKey,

            COALESCE(TRIM(sod.CarrierTrackingNumber), 'N/A') AS CarrierTrackingNumber,

            sod.OrderQty,
            sod.UnitPrice,
            sod.UnitPriceDiscount,

            -- Computed LineTotal
            (sod.UnitPrice - sod.UnitPriceDiscount) * sod.OrderQty AS LineTotal,

            soh.Freight,
            soh.TaxAmt,
                
            -- Computed TotalDue
            soh.SubTotal + soh.TaxAmt + soh.Freight AS TotalDue

        FROM Staging.dbo.SalesOrderDetail sod
        INNER JOIN Staging.dbo.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID

        -- Date dimension joins
        LEFT JOIN DataWarehouseClassic.dbo.DimDate dd_order ON CAST(soh.OrderDate AS DATE) = dd_order.[Date]
        LEFT JOIN DataWarehouseClassic.dbo.DimDate dd_due ON CAST(soh.DueDate AS DATE) = dd_due.[Date]
        LEFT JOIN DataWarehouseClassic.dbo.DimDate dd_ship ON CAST(soh.ShipDate AS DATE) = dd_ship.[Date];

    END TRY
    BEGIN CATCH
        PRINT 'Error Code: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error Message: ' + ERROR_MESSAGE();
    END CATCH
END;
GO


--EXEC load_datawarehouse;
