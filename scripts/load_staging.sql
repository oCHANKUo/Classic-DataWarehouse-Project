USE [Staging]
GO

-- StoredProcedure [dbo].[load_staging] 
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[load_staging]
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        -- Load IndividualCustomer
        INSERT INTO IndividualCustomer
        SELECT * FROM CustomerDatabase.dbo.IndividualCustomer;

        -- Load Product
        INSERT INTO Product
        SELECT * FROM CustomerDatabase.dbo.Product;

        -- Load ProductCategory
        INSERT INTO ProductCategory
        SELECT * FROM CustomerDatabase.dbo.ProductCategory;

        -- Load ProductSubCategory
        INSERT INTO ProductSubCategory
        SELECT * FROM CustomerDatabase.dbo.ProductSubCategory;

        -- Load SalesOrderDetail
        INSERT INTO SalesOrderDetail
        SELECT * FROM CustomerDatabase.dbo.SalesOrderDetail;

        -- Load SalesOrderHeader
        INSERT INTO SalesOrderHeader
        SELECT * FROM CustomerDatabase.dbo.SalesOrderHeader;

        -- Load SalesPerson
        INSERT INTO SalesPerson
        SELECT * FROM CustomerDatabase.dbo.SalesPerson;

        -- Load SalesTerritory
        INSERT INTO SalesTerritory
        SELECT * FROM CustomerDatabase.dbo.SalesTerritory;

        -- Load CustomerAddress
        INSERT INTO CustomerAddress
        SELECT * FROM CustomerDatabase.dbo.CustomerAddress;

        PRINT 'Staging load completed successfully.';
    END TRY
    BEGIN CATCH
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error Message: ' + ERROR_MESSAGE();
    END CATCH
END;
