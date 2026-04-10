-- =============================================================================
-- FastFreight Solutions — Azure SQL Source Database
-- Case Study #1: Incremental Data Ingestion Pipeline
-- Microsoft Fabric / DP-600 Portfolio
-- =============================================================================

-- Step 1: Create the ShipmentLogs source table
-- =============================================================================
CREATE TABLE dbo.ShipmentLogs (
    ShipmentID          NVARCHAR(20)    NOT NULL,
    CustomerName        NVARCHAR(100)   NOT NULL,
    OriginPort          NVARCHAR(100)   NOT NULL,
    OriginCountry       NVARCHAR(60)    NOT NULL,
    OriginRegion        NVARCHAR(10)    NOT NULL,   -- APAC, EMEA, AMER
    OriginPortCode      NVARCHAR(10)    NOT NULL,
    DestinationPort     NVARCHAR(100)   NOT NULL,
    DestinationCountry  NVARCHAR(60)    NOT NULL,
    DestinationRegion   NVARCHAR(10)    NOT NULL,
    DestinationPortCode NVARCHAR(10)    NOT NULL,
    CargoType           NVARCHAR(50)    NOT NULL,
    CargoDescription    NVARCHAR(200)   NULL,
    WeightKG            DECIMAL(12,2)   NOT NULL,
    DeclaredValueUSD    DECIMAL(14,2)   NULL,
    ContainerCount      SMALLINT        NOT NULL DEFAULT 1,
    ContainerType       NVARCHAR(10)    NOT NULL,  -- 20GP, 40GP, 40HC, 40RH, 20TK
    CarrierName         NVARCHAR(100)   NULL,
    VesselName          NVARCHAR(100)   NULL,
    Status              NVARCHAR(40)    NOT NULL,
    DelayReason         NVARCHAR(200)   NULL,
    DepartureDate       DATETIME        NOT NULL,
    EstimatedArrival    DATETIME        NOT NULL,
    ActualArrival       DATETIME        NULL,
    LastModifiedDate    DATETIME        NOT NULL,  -- WATERMARK COLUMN
    CONSTRAINT PK_ShipmentLogs PRIMARY KEY (ShipmentID)
);

-- =============================================================================
-- Step 2: Create index on watermark column (CRITICAL for performance)
-- Without this index, every pipeline run = full table scan
-- =============================================================================
CREATE NONCLUSTERED INDEX IX_ShipmentLogs_LastModified
    ON dbo.ShipmentLogs (LastModifiedDate)
    INCLUDE (ShipmentID, Status, CargoType, OriginPortCode, DestinationPortCode);

-- Additional index for common dashboard filter patterns
CREATE NONCLUSTERED INDEX IX_ShipmentLogs_Status
    ON dbo.ShipmentLogs (Status, LastModifiedDate);

CREATE NONCLUSTERED INDEX IX_ShipmentLogs_Origin_Dest
    ON dbo.ShipmentLogs (OriginPortCode, DestinationPortCode, LastModifiedDate);

-- =============================================================================
-- Step 3: Create the watermark control table (lives in Azure SQL)
-- In the Fabric implementation this lives in the Lakehouse as a Delta table
-- but this SQL version is provided for reference / hybrid setups
-- =============================================================================
CREATE TABLE dbo.WatermarkControl (
    TableName           NVARCHAR(100)   NOT NULL,
    LastWatermarkDate   DATETIME        NOT NULL,
    CreatedDate         DATETIME        NOT NULL DEFAULT GETDATE(),
    Notes               NVARCHAR(500)   NULL,
    CONSTRAINT PK_WatermarkControl PRIMARY KEY (TableName)
);

-- Seed with epoch date — first pipeline run will load all history
INSERT INTO dbo.WatermarkControl (TableName, LastWatermarkDate, Notes)
VALUES ('ShipmentLogs', '1900-01-01 00:00:00', 'Initial seed — triggers full history load on first run');

-- =============================================================================
-- Step 4: Watermark pipeline queries (used by Data Factory Lookup activities)
-- =============================================================================

-- Activity 1: Get old watermark (Lookup 1 in ADF/Fabric pipeline)
SELECT LastWatermarkDate
FROM   dbo.WatermarkControl
WHERE  TableName = 'ShipmentLogs';

-- Activity 2: Get new watermark ceiling (Lookup 2 in ADF/Fabric pipeline)
SELECT MAX(LastModifiedDate) AS NewWatermark
FROM   dbo.ShipmentLogs;

-- Activity 3: Incremental extract query (Copy Activity source query)
-- @{activity('Lookup_OldWatermark').output.firstRow.LastWatermarkDate}
-- @{activity('Lookup_NewWatermark').output.firstRow.NewWatermark}
SELECT
    ShipmentID, CustomerName,
    OriginPort, OriginCountry, OriginRegion, OriginPortCode,
    DestinationPort, DestinationCountry, DestinationRegion, DestinationPortCode,
    CargoType, CargoDescription,
    WeightKG, DeclaredValueUSD, ContainerCount, ContainerType,
    CarrierName, VesselName,
    Status, DelayReason,
    DepartureDate, EstimatedArrival, ActualArrival,
    LastModifiedDate
FROM  dbo.ShipmentLogs
WHERE LastModifiedDate > '@old_watermark'
  AND LastModifiedDate <= '@new_watermark';

-- Activity 4: Update watermark after successful run (Stored Procedure activity)
UPDATE dbo.WatermarkControl
SET    LastWatermarkDate = '@new_watermark'
WHERE  TableName = 'ShipmentLogs';

-- =============================================================================
-- Step 5: Useful validation queries for monitoring pipeline health
-- =============================================================================

-- Check record count by status (run after each pipeline execution)
SELECT   Status, COUNT(*) AS RecordCount
FROM     dbo.ShipmentLogs
GROUP BY Status
ORDER BY RecordCount DESC;

-- Check watermark advancement over time
SELECT   TableName, LastWatermarkDate, Notes
FROM     dbo.WatermarkControl;

-- Identify the most recently modified records (sanity check)
SELECT TOP 10 ShipmentID, Status, LastModifiedDate
FROM   dbo.ShipmentLogs
ORDER BY LastModifiedDate DESC;

-- Count records that would be picked up in next incremental window
SELECT COUNT(*) AS RowsInNextDelta
FROM   dbo.ShipmentLogs
WHERE  LastModifiedDate > (SELECT LastWatermarkDate FROM dbo.WatermarkControl WHERE TableName = 'ShipmentLogs');
