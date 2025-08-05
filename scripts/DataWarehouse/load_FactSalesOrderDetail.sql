/******************************************************************************************
  Procedure Name : load_FactSalesOrderDetail
  Description    : Performs an incremental load into the FactSalesOrderDetail fact table.
                   Inserts new sales order details from the staging area that do not yet
                   exist in the fact table by joining with dimension tables to get surrogate keys.
******************************************************************************************/

-- FactSalesOrderDetail incremental load

CREATE OR ALTER PROCEDURE dbo.load_FactSalesOrderDetail
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO DataWarehouseClassic.dbo.FactSalesOrderDetail (
        SalesOrderDetailID,
        SalesOrderID,
        ProductKey,
        CustomerKey,
        SalesPersonKey,
        TerritoryKey,
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
        dp.ProductKey,
        dc.CustomerKey,
        dsp.SalesPersonKey,
        dt.TerritoryKey,
        dd_order.DateKey,
        dd_due.DateKey,
        dd_ship.DateKey,
        COALESCE(TRIM(sod.CarrierTrackingNumber), 'N/A') AS CarrierTrackingNumber,
        sod.OrderQty,
        sod.UnitPrice,
        sod.UnitPriceDiscount,
        (sod.UnitPrice - sod.UnitPriceDiscount) * sod.OrderQty AS LineTotal,
        soh.Freight,
        soh.TaxAmt,
        soh.SubTotal + soh.TaxAmt + soh.Freight AS TotalDue
    FROM Staging.dbo.SalesOrderDetail sod
    INNER JOIN Staging.dbo.SalesOrderHeader soh ON sod.SalesOrderID = soh.SalesOrderID
    JOIN DataWarehouseClassic.dbo.DimProduct dp ON dp.ProductID = sod.ProductID
    JOIN DataWarehouseClassic.dbo.DimCustomer dc ON dc.CustomerID = soh.CustomerID
    LEFT JOIN DataWarehouseClassic.dbo.DimSalesPerson dsp ON dsp.SalesPersonID = soh.SalesPersonID
    LEFT JOIN DataWarehouseClassic.dbo.DimTerritory dt ON dt.TerritoryID = soh.TerritoryID
    LEFT JOIN DataWarehouseClassic.dbo.DimDate dd_order ON CAST(soh.OrderDate AS DATE) = dd_order.[Date]
    LEFT JOIN DataWarehouseClassic.dbo.DimDate dd_due ON CAST(soh.DueDate AS DATE) = dd_due.[Date]
    LEFT JOIN DataWarehouseClassic.dbo.DimDate dd_ship ON CAST(soh.ShipDate AS DATE) = dd_ship.[Date]
    WHERE NOT EXISTS (
        SELECT 1 FROM DataWarehouseClassic.dbo.FactSalesOrderDetail f
        WHERE f.SalesOrderDetailID = sod.SalesOrderDetailID
    );
END;


-- EXEC load_FactSalesOrderDetail;
