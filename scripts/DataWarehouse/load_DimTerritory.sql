/******************************************************************************************
  Procedure Name : load_DimTerritory
  Description    : Performs an UPSERT operation on the DimTerritory table by merging data
                   from the Staging.dbo.SalesTerritory table into the DataWarehouseClassic.
******************************************************************************************/

-- UPSERT DimTerritory

CREATE OR ALTER PROCEDURE load_DimTerritory
AS
BEGIN
    SET NOCOUNT ON;

    MERGE DataWarehouseClassic.dbo.DimTerritory AS target
    USING (
        SELECT
            TerritoryID,
            LTRIM(RTRIM(Name)) AS TerritoryName,
            LTRIM(RTRIM(CountryRegionCode)) AS CountryRegionCode,
            LTRIM(RTRIM(RegionGroup)) AS RegionGroup
        FROM Staging.dbo.SalesTerritory
    ) AS source
    ON target.TerritoryID = source.TerritoryID

    WHEN MATCHED THEN
        UPDATE SET
            target.TerritoryName = source.TerritoryName,
            target.CountryRegionCode = source.CountryRegionCode,
            target.RegionGroup = source.RegionGroup

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            TerritoryID,
            TerritoryName,
            CountryRegionCode,
            RegionGroup
        )
        VALUES (
            source.TerritoryID,
            source.TerritoryName,
            source.CountryRegionCode,
            source.RegionGroup
        );
END;

--EXEC load_DimTerritory;
