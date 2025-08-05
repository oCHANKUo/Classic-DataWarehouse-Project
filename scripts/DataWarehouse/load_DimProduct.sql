/**********************************************************************************************
 Script Name   : load_DimProduct
 Description   : UPSERTs records into the DimProduct table within the DataWarehouseClassic 
                 database. This procedure enriches and transforms product data from the 
                 staging tables before loading.

                 - Handles missing or inconsistent Size and Weight units.
                 - Converts weight to grams where applicable.
                 - Normalizes fields like Color and Category names.
                 - Ensures both Product Category and SubCategory are linked.

 Source Tables : 
     - Staging.dbo.Product
     - Staging.dbo.ProductSubCategory
     - Staging.dbo.ProductCategory

 Target Table  : 
     - DataWarehouseClassic.dbo.DimProduct

 Author        : [Your Name]
 Created On    : [Date]
**********************************************************************************************/

-- Stored Procedure to UPSERT DimProduct
CREATE OR ALTER PROCEDURE load_DimProduct
AS
BEGIN
    SET NOCOUNT ON;

    MERGE DataWarehouseClassic.dbo.DimProduct AS Target
    USING (
        SELECT
            p.ProductID,
            p.Name AS ProductName,
            p.ProductNumber,
            CASE 
                WHEN p.Color = 'Multi' THEN 'Multicolor'
                WHEN p.Color IS NULL THEN 'N/A'
                ELSE p.Color
            END AS Color,

            CASE 
                WHEN SizeUnitMeasureCode = 'CM' AND ISNUMERIC(CAST(Size AS NVARCHAR)) = 1 THEN CAST(Size AS DECIMAL(10,2))
                ELSE NULL
            END AS SizeValue,

            CASE 
                WHEN SizeUnitMeasureCode IS NULL THEN Size
                WHEN SizeUnitMeasureCode <> 'CM' THEN Size 
                ELSE NULL
            END AS SizeLabel,

            COALESCE(CAST(p.Size AS NVARCHAR(10)), 'UNK') AS Size,
            COALESCE(p.SizeUnitMeasureCode, 'N/A') AS SizeUnitMeasureCode,

            p.StandardCost,
            p.ListPrice,

            p.Weight,
            COALESCE(p.WeightUnitMeasureCode, 'UNK') AS WeightUnitMeasureCode,

            CASE
                WHEN p.Weight IS NULL THEN NULL
                WHEN p.WeightUnitMeasureCode = 'LB' THEN p.Weight * 453.592
                WHEN p.WeightUnitMeasureCode = 'G' THEN p.Weight
                ELSE NULL 
            END AS WeightInGrams,

            COALESCE(LTRIM(RTRIM(pc.Name)), 'Uncategorized') AS CategoryName,
            COALESCE(psc.Name, 'Uncategorized') AS SubCategoryName

        FROM Staging.dbo.Product p
        LEFT JOIN Staging.dbo.ProductSubCategory psc ON p.ProductSubCategoryID = psc.ProductSubCategoryID
        LEFT JOIN Staging.dbo.ProductCategory pc ON psc.ProductCategoryID = pc.ProductCategoryID
    ) AS Source
    ON Target.ProductID = Source.ProductID
    WHEN MATCHED THEN
        UPDATE SET 
            Target.ProductName = Source.ProductName,
            Target.ProductNumber = Source.ProductNumber,
            Target.Color = Source.Color,
            Target.Size = Source.Size,
            Target.SizeUnitMeasureCode = Source.SizeUnitMeasureCode,
            Target.StandardCost = Source.StandardCost,
            Target.ListPrice = Source.ListPrice,
            Target.Weight = Source.Weight,
            Target.WeightUnitMeasureCode = Source.WeightUnitMeasureCode,
            Target.WeightInGrams = Source.WeightInGrams,
            Target.CategoryName = Source.CategoryName,
            Target.SubCategoryName = Source.SubCategoryName
    WHEN NOT MATCHED THEN
        INSERT (
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
            WeightInGrams,
            CategoryName,
            SubCategoryName
        )
        VALUES (
            Source.ProductID,
            Source.ProductName,
            Source.ProductNumber,
            Source.Color,
            Source.Size,
            Source.SizeUnitMeasureCode,
            Source.StandardCost,
            Source.ListPrice,
            Source.Weight,
            Source.WeightUnitMeasureCode,
            Source.WeightInGrams,
            Source.CategoryName,
            Source.SubCategoryName
        );
END;

-- EXEC load_DimProduct
