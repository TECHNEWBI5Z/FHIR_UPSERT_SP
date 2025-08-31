ALTER PROCEDURE [dbo].[UpsertFHIRResourceAuditColumn_NoCursor]
    @JsonObj NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        -- ===============================
        -- Step 1: Extract key properties from JSON
        -- ===============================
        DECLARE 
            @TargetTable NVARCHAR(128) = JSON_VALUE(@JsonObj, '$.resourceType'),  -- Target table name (FHIR resource type)
            @id NVARCHAR(128) = JSON_VALUE(@JsonObj, '$.id'),                    -- Unique ID for UPSERT operation
            @isDeleted NVARCHAR(10) = JSON_VALUE(@JsonObj, '$.isDeleted'),       -- Deletion flag
            @qualifiedTable NVARCHAR(258),                                       -- Fully qualified table name (e.g. dbo.Patient)
            @insertCols NVARCHAR(MAX) = '',                                      -- Columns part for INSERT statement
            @insertVals NVARCHAR(MAX) = '',                                      -- Values part for INSERT statement
            @updateSet NVARCHAR(MAX) = '',                                       -- SET part for UPDATE statement
            @sql NVARCHAR(MAX);                                                  -- Dynamic SQL to execute

        -- ===============================
        -- Step 2: Validate required parameters
        -- ===============================
        IF @id IS NULL OR @id = ''
        BEGIN
            RAISERROR('FHIR JSON must include a valid "id" field.', 16, 1);
            RETURN;
        END

        IF @TargetTable IS NULL OR @TargetTable = ''
        BEGIN
            RAISERROR('FHIR JSON must include a valid "resourceType" field.', 16, 1);
            RETURN;
        END

        -- ===============================
        -- Step 3: Determine fully qualified table name
        -- Supports optional schema prefix in resourceType
        -- ===============================
        IF CHARINDEX('.', @TargetTable) = 0
            SET @qualifiedTable = QUOTENAME('dbo') + '.' + QUOTENAME(@TargetTable);
        ELSE
            SET @qualifiedTable = QUOTENAME(PARSENAME(@TargetTable, 2)) + '.' + QUOTENAME(PARSENAME(@TargetTable, 1));

        -- ===============================
        -- Step 4: Build INSERT and UPDATE column/value lists from JSON keys and table columns
        -- Exclude audit columns and ID to avoid overwriting them unintentionally
        -- ===============================
        SELECT 
            @insertCols += QUOTENAME(c.COLUMN_NAME) + ',',
            @insertVals += 
                CASE 
                    WHEN j.value IS NULL THEN 'NULL,'
                    WHEN c.DATA_TYPE IN ('bit') AND LOWER(j.value) IN ('true', 'false') THEN 
                        CASE LOWER(j.value) WHEN 'true' THEN '1,' ELSE '0,' END
                    WHEN c.DATA_TYPE IN ('int', 'bigint', 'float', 'decimal', 'numeric') THEN 
                        j.value + ','
                    ELSE 
                        '''' + REPLACE(j.value, '''', '''''') + ''',' 
                END,
            @updateSet += 
                QUOTENAME(c.COLUMN_NAME) + ' = ' + 
                CASE 
                    WHEN j.value IS NULL THEN 'NULL,'
                    WHEN c.DATA_TYPE IN ('bit') AND LOWER(j.value) IN ('true', 'false') THEN 
                        CASE LOWER(j.value) WHEN 'true' THEN '1,' ELSE '0,' END
                    WHEN c.DATA_TYPE IN ('int', 'bigint', 'float', 'decimal', 'numeric') THEN 
                        j.value + ','
                    ELSE 
                        '''' + REPLACE(j.value, '''', '''''') + ''',' 
                END
        FROM OPENJSON(@JsonObj) j
        INNER JOIN INFORMATION_SCHEMA.COLUMNS c
            ON c.TABLE_NAME = PARSENAME(@TargetTable, 1)
            AND c.COLUMN_NAME = j.[key] COLLATE SQL_Latin1_General_CP1_CI_AS
            -- Exclude columns managed by audit triggers or system
            AND c.COLUMN_NAME NOT IN ('id', 'createdDateTime', 'createdBy', 'modifiedDateTime', 'modifiedBy', 'deletedDateTime', 'deletedBy');

        -- ===============================
        -- Step 5: Append audit columns for INSERT and UPDATE
        -- These columns track who and when created/modified/deleted records
        -- ===============================
        SET @insertCols += 'createdDateTime,createdBy,';
        SET @insertVals += 'SYSUTCDATETIME(), SYSTEM_USER,';
        SET @updateSet += 'modifiedDateTime = SYSUTCDATETIME(), modifiedBy = SYSTEM_USER,';

        -- ===============================
        -- Step 6: Handle soft-delete audit columns based on isDeleted flag
        -- ===============================
        IF LOWER(@isDeleted) = 'true'
        BEGIN
            SET @updateSet += 'deletedDateTime = SYSUTCDATETIME(), deletedBy = SYSTEM_USER,';
        END
        ELSE IF LOWER(@isDeleted) = 'false'
        BEGIN
            SET @updateSet += 'deletedDateTime = NULL, deletedBy = NULL,';
        END

        -- ===============================
        -- Step 7: Remove trailing commas from column/value strings
        -- ===============================
        IF RIGHT(@insertCols, 1) = ',' SET @insertCols = LEFT(@insertCols, LEN(@insertCols) - 1);
        IF RIGHT(@insertVals, 1) = ',' SET @insertVals = LEFT(@insertVals, LEN(@insertVals) - 1);
        IF RIGHT(@updateSet, 1) = ',' SET @updateSet = LEFT(@updateSet, LEN(@updateSet) - 1);

        -- ===============================
        -- Step 8: Construct dynamic UPSERT SQL statement
        -- Checks if record exists, then UPDATE; otherwise INSERT
        -- Uses parameterized execution for safety
        -- ===============================
        SET @sql = '
        IF EXISTS (SELECT 1 FROM ' + @qualifiedTable + ' WHERE id = @id)
        BEGIN
            UPDATE ' + @qualifiedTable + '
            SET ' + @updateSet + '
            WHERE id = @id;
        END
        ELSE
        BEGIN
            INSERT INTO ' + @qualifiedTable + ' (id, ' + @insertCols + ')
            VALUES (@id, ' + @insertVals + ');
        END;';

        -- ===============================
        -- Step 9: Execute the dynamic SQL safely with sp_executesql and parameter
        -- ===============================
        EXEC sp_executesql @sql, N'@id NVARCHAR(MAX)', @id = @id;

    END TRY

    BEGIN CATCH
        BEGIN
            -- ===============================
            -- Step 10: Error handling
            -- Rethrow the error with severity and message captured
            -- ===============================
            DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT, @ErrorWithId NVARCHAR(MAX);
            SELECT 
                @ErrMsg = ERROR_MESSAGE(),
                @ErrSeverity = ERROR_SEVERITY();

            -- Append the @id value to the error message for better traceability
            SET @ErrorWithId = CONCAT(@ErrMsg, ' | Error processing id: ', ISNULL(@id, 'NULL'));

            RAISERROR(@ErrorWithId, @ErrSeverity, 1);
        END
    END CATCH
END;