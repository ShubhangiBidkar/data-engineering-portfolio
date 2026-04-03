-- =============================================================================
-- Case Study #2 — Healthcare Medallion Lakehouse
-- MediCore Health Network
-- FILE: cs2_01_azure_sql_patients.sql
--
-- PURPOSE:
-- Create the Patients source table in Azure SQL and load 1,000 records.
-- This is SOURCE 1 of 3. The Fabric pipeline reads from this table.
--
-- WHAT BELONGS HERE:
--   ✅ CREATE TABLE dbo.Patients
--   ✅ Performance indexes on watermark column
--   ✅ BULK INSERT from patients.csv
--   ✅ Verification queries
--
-- WHAT DOES NOT BELONG HERE:
--   ❌ Watermark control table  → lives in Fabric Lakehouse as Delta table
--   ❌ Row-level security       → applied in Fabric SQL Analytics Endpoint
--   ❌ Dynamic data masking     → applied in Fabric Data Warehouse
--   ❌ Stored procedures        → pipeline logic handled in Fabric notebooks
-- =============================================================================


-- =============================================================================
-- STEP 1 — Create the Patients table
-- =============================================================================
-- LastModifiedDate is the WATERMARK COLUMN.
-- Every INSERT and UPDATE in the source system must populate this column.
-- Without a reliable timestamp column, incremental ingestion is not possible.
-- =============================================================================

CREATE TABLE dbo.Patients (

    -- Identifiers
    PatientID               NVARCHAR(20)    NOT NULL,
    MRN                     NVARCHAR(20)    NOT NULL,

    -- Demographics
    FirstName               NVARCHAR(60)    NOT NULL,
    LastName                NVARCHAR(60)    NOT NULL,
    DateOfBirth             DATE            NOT NULL,
    Age                     SMALLINT        NULL,
    Gender                  NVARCHAR(10)    NULL,
    SSN                     NVARCHAR(15)    NULL,
    PhoneNumber             NVARCHAR(20)    NULL,
    Email                   NVARCHAR(100)   NULL,

    -- Address
    AddressLine1            NVARCHAR(200)   NULL,
    City                    NVARCHAR(60)    NULL,
    State                   NVARCHAR(5)     NULL,
    ZipCode                 NVARCHAR(10)    NULL,

    -- Clinical
    BloodType               NVARCHAR(5)     NULL,
    Ethnicity               NVARCHAR(60)    NULL,
    MaritalStatus           NVARCHAR(20)    NULL,
    EmploymentStatus        NVARCHAR(30)    NULL,

    -- Insurance
    InsurancePayer          NVARCHAR(60)    NULL,
    InsurancePayerCode      NVARCHAR(10)    NULL,
    InsuranceType           NVARCHAR(20)    NULL,
    InsuranceMemberID       NVARCHAR(30)    NULL,

    -- Primary Diagnosis (ICD-10)
    PrimaryDiagnosis        NVARCHAR(10)    NULL,
    PrimaryDiagnosisDesc    NVARCHAR(200)   NULL,
    Specialty               NVARCHAR(60)    NULL,
    ComorbidityCode         NVARCHAR(10)    NULL,
    ComorbidityDesc         NVARCHAR(200)   NULL,

    -- Facility
    PrimaryFacilityID       NVARCHAR(10)    NULL,
    PrimaryFacilityName     NVARCHAR(100)   NULL,

    -- Administrative
    RegistrationDate        DATETIME        NULL,
    IsActive                NVARCHAR(1)     NULL DEFAULT 'Y',

    -- Watermark column — MUST be populated on every INSERT and UPDATE
    LastModifiedDate        DATETIME        NOT NULL,

    CONSTRAINT PK_Patients PRIMARY KEY (PatientID)
);
GO


-- =============================================================================
-- STEP 2 — Create indexes
-- =============================================================================
-- The index on LastModifiedDate is not optional.
-- Without it the Copy Activity performs a full table scan on every run.
-- At 1M+ rows that becomes minutes of unnecessary work per pipeline run.
-- =============================================================================

-- Watermark index — used by every incremental pipeline run
CREATE NONCLUSTERED INDEX IX_Patients_LastModified
    ON dbo.Patients (LastModifiedDate)
    INCLUDE (PatientID, IsActive, State, InsurancePayer);

-- Active patients index — most queries filter on active patients only
CREATE NONCLUSTERED INDEX IX_Patients_Active
    ON dbo.Patients (IsActive, LastModifiedDate)
    INCLUDE (PatientID, FirstName, LastName, State);
GO


-- =============================================================================
-- STEP 3 — Create staging table and load patients.csv
-- =============================================================================
-- We load into a staging table first (no constraints, all strings).
-- This absorbs any formatting issues in the CSV without failing.
-- We then INSERT into the production table with type casting and validation.
-- =============================================================================

-- Create staging table (all NVARCHAR — no type failures during bulk load)
CREATE TABLE dbo.Patients_Staging (
    PatientID               NVARCHAR(20),
    MRN                     NVARCHAR(20),
    FirstName               NVARCHAR(60),
    LastName                NVARCHAR(60),
    DateOfBirth             NVARCHAR(20),
    Age                     NVARCHAR(10),
    Gender                  NVARCHAR(10),
    SSN                     NVARCHAR(15),
    PhoneNumber             NVARCHAR(20),
    Email                   NVARCHAR(100),
    AddressLine1            NVARCHAR(200),
    City                    NVARCHAR(60),
    State                   NVARCHAR(5),
    ZipCode                 NVARCHAR(10),
    BloodType               NVARCHAR(5),
    Ethnicity               NVARCHAR(60),
    MaritalStatus           NVARCHAR(20),
    EmploymentStatus        NVARCHAR(30),
    InsurancePayer          NVARCHAR(60),
    InsurancePayerCode      NVARCHAR(10),
    InsuranceType           NVARCHAR(20),
    InsuranceMemberID       NVARCHAR(30),
    PrimaryDiagnosis        NVARCHAR(10),
    PrimaryDiagnosisDesc    NVARCHAR(200),
    Specialty               NVARCHAR(60),
    ComorbidityCode         NVARCHAR(10),
    ComorbidityDesc         NVARCHAR(200),
    PrimaryFacilityID       NVARCHAR(10),
    PrimaryFacilityName     NVARCHAR(100),
    RegistrationDate        NVARCHAR(30),
    IsActive                NVARCHAR(1),
    LastModifiedDate        NVARCHAR(30)
);
GO


-- Bulk insert patients.csv from ADLS Gen2
-- Replace YOUR_ACCOUNT and YOUR_CONTAINER with actual values
-- Run credential and data source creation first (from CS1 setup if already done)
BULK INSERT dbo.Patients_Staging
FROM 'patients.csv'
WITH (
    DATA_SOURCE     = 'medicore_blob_source',
    FORMAT          = 'CSV',
    FIRSTROW        = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '0x0a',
    TABLOCK
);
GO


-- =============================================================================
-- STEP 4 — Validate staging before promoting to production
-- =============================================================================
-- Check for data issues before committing to the production table.
-- Fix any issues in the CSV and re-run the bulk insert if needed.
-- =============================================================================

SELECT
    COUNT(*)                                                        AS TotalRows,
    COUNT(DISTINCT PatientID)                                       AS UniquePatients,
    SUM(CASE WHEN PatientID IS NULL THEN 1 ELSE 0 END)              AS NullPatientIDs,
    SUM(CASE WHEN TRY_CAST(Age AS INT) IS NULL
              OR TRY_CAST(Age AS INT) < 0
              OR TRY_CAST(Age AS INT) > 120 THEN 1 ELSE 0 END)     AS InvalidAges,
    SUM(CASE WHEN LastModifiedDate IS NULL
              OR LastModifiedDate = '' THEN 1 ELSE 0 END)           AS NullWatermarks,
    SUM(CASE WHEN IsActive NOT IN ('Y', 'N') THEN 1 ELSE 0 END)    AS InvalidIsActive
FROM dbo.Patients_Staging;

-- Expected result:
--   TotalRows       = 1000
--   UniquePatients  = 1000
--   NullPatientIDs  = 0
--   InvalidAges     = 0
--   NullWatermarks  = 0
--   InvalidIsActive = 0
GO


-- =============================================================================
-- STEP 5 — Insert from staging to production with type casting
-- =============================================================================

INSERT INTO dbo.Patients (
    PatientID, MRN, FirstName, LastName,
    DateOfBirth, Age, Gender, SSN, PhoneNumber, Email,
    AddressLine1, City, State, ZipCode,
    BloodType, Ethnicity, MaritalStatus, EmploymentStatus,
    InsurancePayer, InsurancePayerCode, InsuranceType, InsuranceMemberID,
    PrimaryDiagnosis, PrimaryDiagnosisDesc, Specialty,
    ComorbidityCode, ComorbidityDesc,
    PrimaryFacilityID, PrimaryFacilityName,
    RegistrationDate, IsActive, LastModifiedDate
)
SELECT
    PatientID,
    MRN,
    FirstName,
    LastName,
    TRY_CAST(DateOfBirth        AS DATE),
    TRY_CAST(Age                AS SMALLINT),
    Gender,
    SSN,
    PhoneNumber,
    Email,
    AddressLine1,
    City,
    State,
    ZipCode,
    BloodType,
    Ethnicity,
    MaritalStatus,
    EmploymentStatus,
    InsurancePayer,
    InsurancePayerCode,
    InsuranceType,
    InsuranceMemberID,
    PrimaryDiagnosis,
    PrimaryDiagnosisDesc,
    Specialty,
    ComorbidityCode,
    ComorbidityDesc,
    PrimaryFacilityID,
    PrimaryFacilityName,
    TRY_CAST(RegistrationDate   AS DATETIME),
    ISNULL(IsActive, 'Y'),
    TRY_CAST(LastModifiedDate   AS DATETIME)
FROM dbo.Patients_Staging
WHERE PatientID          IS NOT NULL
  AND TRY_CAST(LastModifiedDate AS DATETIME) IS NOT NULL;
GO


-- Drop staging table — no longer needed
DROP TABLE dbo.Patients_Staging;
GO


-- =============================================================================
-- STEP 6 — Verify the load
-- =============================================================================

-- Row count (must be 1,000)
SELECT COUNT(*) AS TotalPatients
FROM   dbo.Patients;

-- Status split
SELECT   IsActive, COUNT(*) AS PatientCount
FROM     dbo.Patients
GROUP BY IsActive;

-- Insurance payer distribution
SELECT   InsurancePayer, COUNT(*) AS Patients
FROM     dbo.Patients
GROUP BY InsurancePayer
ORDER BY Patients DESC;

-- Top diagnosis codes
SELECT   TOP 5
         PrimaryDiagnosis,
         PrimaryDiagnosisDesc,
         COUNT(*) AS Patients
FROM     dbo.Patients
GROUP BY PrimaryDiagnosis, PrimaryDiagnosisDesc
ORDER BY Patients DESC;

-- Watermark range (pipeline will use these two values)
SELECT
    MIN(LastModifiedDate) AS EarliestRecord,
    MAX(LastModifiedDate) AS WatermarkCeiling
FROM dbo.Patients;

-- The pipeline will:
--   Old watermark  = 1900-01-01 (from Fabric watermark_control table)
--   New watermark  = WatermarkCeiling value shown above
--   Rows extracted = all 1,000 on first run
GO
