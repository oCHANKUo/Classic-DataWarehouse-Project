/*
===============================================================================================
 Title       : load_DataWarehouse
 Description : This stored procedure orchestrates the sequential loading of all dimension and
               fact tables in the DataWarehouseClassic database. It ensures the data is 
               loaded in the correct order while handling any errors gracefully.

 Procedure   : load_DataWarehouse

 Steps Performed:
   - Executes the following ETL load procedures:
       • load_DimCustomer
       • load_DimProduct
       • load_DimSalesPerson
       • load_DimTerritory
       • load_FactSalesOrderDetail
===============================================================================================
*/

USE DataWarehouseClassic
GO
CREATE OR ALTER PROCEDURE dbo.load_DataWarehouse
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        EXEC dbo.load_DimCustomer;
        PRINT 'load_DimCustomer executed successfully.';

        EXEC dbo.load_DimProduct;
        PRINT 'load_DimProduct executed successfully.';

        EXEC dbo.load_DimSalesPerson;
        PRINT 'load_DimSalesPerson executed successfully.';

        EXEC dbo.load_DimTerritory;
        PRINT 'load_DimTerritory executed successfully.';

        EXEC dbo.load_FactSalesOrderDetail;
        PRINT 'load_FactSalesOrderDetail executed successfully.';
    END TRY
    BEGIN CATCH
        PRINT ERROR_MESSAGE();
        PRINT 'State: ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT 'Line Number: ' + CAST(ERROR_LINE() AS NVARCHAR(10));
    END CATCH
END;

-- EXEC dbo.load_DataWarehouse;
