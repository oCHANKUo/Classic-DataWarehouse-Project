/**********************************************************************************************
 Script Name   : load_DimCustomer
 Description   : Performs an UPSERT operation on the DimCustomer dimension table in the 
                 DataWarehouseClassic database. It merges customer data from the staging 
                 tables (IndividualCustomer and CustomerAddress) into DimCustomer.
                 
                 - Updates existing customer records if matched by CustomerID.
                 - Inserts new customer records if not matched.
                 - Handles null values and formats fields such as FullName, Gender, and 
                   EmailPromotion with user-friendly values.

 Source Tables : 
     - Staging.dbo.IndividualCustomer
     - Staging.dbo.CustomerAddress
**********************************************************************************************/

-- Stored Procedure to UPSERT DimCustomer
CREATE OR ALTER PROCEDURE dbo.load_DimCustomer
AS
BEGIN
    SET NOCOUNT ON;

    MERGE DataWarehouseClassic.dbo.DimCustomer AS target
    USING (
        SELECT
            ic.CustomerID,
            CONCAT(ISNULL(ic.FirstName, ''), ' ', ISNULL(ic.LastName, '')) AS FullName,
            ISNULL(ic.Title, 'N/A') AS Title,
            CASE 
                WHEN ic.Gender = 'F' THEN 'Female'
                WHEN ic.Gender = 'M' THEN 'Male'
                ELSE 'N/A'
            END AS Gender,
            ic.PhoneNumber,
            ic.EmailAddress,
            ca.AddressType,
            ca.AddressLine1 AS AddressLine,
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
            ON ic.CustomerID = ca.CustomerID
    ) AS source
    ON target.CustomerID = source.CustomerID

    WHEN MATCHED THEN
        UPDATE SET
            FullName = source.FullName,
            Title = source.Title,
            Gender = source.Gender,
            PhoneNumber = source.PhoneNumber,
            EmailAddress = source.EmailAddress,
            AddressType = source.AddressType,
            AddressLine = source.AddressLine,
            City = source.City,
            StateProvinceName = source.StateProvinceName,
            PostalCode = source.PostalCode,
            CountryRegionName = source.CountryRegionName,
            EmailPromotion = source.EmailPromotion

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
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
        VALUES (
            source.CustomerID,
            source.FullName,
            source.Title,
            source.Gender,
            source.PhoneNumber,
            source.EmailAddress,
            source.AddressType,
            source.AddressLine,
            source.City,
            source.StateProvinceName,
            source.PostalCode,
            source.CountryRegionName,
            source.EmailPromotion
        );
END;

--EXEC dbo.load_DimCustomer;
