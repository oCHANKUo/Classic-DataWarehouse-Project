/**********************************************************************************************
 Script Name   : load_DimSalesPerson
 Description   : UPSERTs records into the DimSalesPerson table within the DataWarehouseClassic 
                 database. This procedure loads and updates salesperson-related metrics such as 
                 sales quota, bonus, and performance figures from the staging environment.

                 - Handles NULL values for TerritoryID and SalesQuota with default values.
                 - Ensures latest YTD and last year sales are updated.
                 - Inserts new salesperson records or updates existing ones.
**********************************************************************************************/

-- UPSERT DimSalesPerson

CREATE OR ALTER PROCEDURE load_DimSalesPerson
AS
BEGIN
    SET NOCOUNT ON;

    MERGE DataWarehouseClassic.dbo.DimSalesPerson AS target
    USING (
        SELECT
            sp.BusinessEntityID AS SalesPersonID,
            COALESCE(sp.TerritoryID, -1) AS TerritoryID,
            COALESCE(sp.SalesQuota, 0.00) AS SalesQuota,
            sp.Bonus,
            sp.CommissionPct,
            sp.SalesYTD,
            sp.SalesLastYear
        FROM Staging.dbo.SalesPerson sp
    ) AS source
    ON target.SalesPersonID = source.SalesPersonID

    WHEN MATCHED THEN
        UPDATE SET
            target.TerritoryID = source.TerritoryID,
            target.SalesQuota = source.SalesQuota,
            target.Bonus = source.Bonus,
            target.CommissionPct = source.CommissionPct,
            target.SalesYTD = source.SalesYTD,
            target.SalesLastYear = source.SalesLastYear

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            SalesPersonID,
            TerritoryID,
            SalesQuota,
            Bonus,
            CommissionPct,
            SalesYTD,
            SalesLastYear
        )
        VALUES (
            source.SalesPersonID,
            source.TerritoryID,
            source.SalesQuota,
            source.Bonus,
            source.CommissionPct,
            source.SalesYTD,
            source.SalesLastYear
        );

END;

-- EXEC load_DimSalesPerson
