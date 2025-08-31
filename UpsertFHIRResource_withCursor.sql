ALTER PROCEDURE dbo.UpsertFHIRResource_withCursor_oppo
    @JsonObj NVARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        DECLARE 
            @TargetTable NVARCHAR(MAX) = JSON_VALUE(@JsonObj, '$.resourceType'),
            @id NVARCHAR(MAX) = JSON_VALUE(@JsonObj, '$.id'),
            @key NVARCHAR(128),
            @val NVARCHAR(MAX),
            @dataType NVARCHAR(128),
            @escapedVal NVARCHAR(MAX),
            @insertCols NVARCHAR(MAX) = '',
            @insertVals NVARCHAR(MAX) = '',
            @updateSet  NVARCHAR(MAX) = '',
            @qualifiedTable NVARCHAR(258);

        IF @id IS NULL
        BEGIN
            RAISERROR('FHIR JSON must include a valid "id" field.', 16, 1);
            RETURN;
        END

        IF CHARINDEX('.', @TargetTable) = 0
            SET @qualifiedTable = QUOTENAME('dbo') + '.' + QUOTENAME(@TargetTable);
        ELSE
            SET @qualifiedTable = QUOTENAME(PARSENAME(@TargetTable, 2)) + '.' + QUOTENAME(PARSENAME(@TargetTable, 1));

        -- Cursor over input JSON
        DECLARE input_cursor CURSOR LOCAL FOR
        SELECT [key], value
        FROM OPENJSON(@JsonObj)
        WHERE [key] <> 'id' AND [key] <> 'resourceType';

        OPEN input_cursor;
        FETCH NEXT FROM input_cursor INTO @key, @val;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- Get column type from schema
            SELECT @dataType = DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = PARSENAME(@TargetTable, 1)
              AND COLUMN_NAME = @key;

            -- Only proceed if column exists
            IF @dataType IS NOT NULL
            BEGIN
                IF @dataType IN ('bit') AND LOWER(@val) IN ('true', 'false')
                    SET @escapedVal = CASE LOWER(@val) WHEN 'true' THEN '1' ELSE '0' END;
                ELSE IF @dataType IN ('int', 'bigint', 'float', 'decimal', 'numeric')
                    SET @escapedVal = @val;
                ELSE
                    SET @escapedVal = '''' + REPLACE(@val, '''', '''''') + '''';

                SET @insertCols += QUOTENAME(@key) + ',';
                SET @insertVals += @escapedVal + ',';
                SET @updateSet  += QUOTENAME(@key) + ' = ' + @escapedVal + ',';
            END

            FETCH NEXT FROM input_cursor INTO @key, @val;
        END

        CLOSE input_cursor;
        DEALLOCATE input_cursor;

        -- Remove trailing commas
        IF RIGHT(@insertCols, 1) = ',' SET @insertCols = LEFT(@insertCols, LEN(@insertCols) - 1);
        IF RIGHT(@insertVals, 1) = ',' SET @insertVals = LEFT(@insertVals, LEN(@insertVals) - 1);
        IF RIGHT(@updateSet, 1) = ',' SET @updateSet = LEFT(@updateSet, LEN(@updateSet) - 1);

        DECLARE @sql NVARCHAR(MAX);
        IF @updateSet <> ''
        BEGIN
            SET @sql = '
            IF EXISTS (SELECT 1 FROM ' + @qualifiedTable + ' WHERE id = @id)
            BEGIN
                UPDATE ' + @qualifiedTable + '
                SET ' + @updateSet + '
                WHERE id = @id;
            END
            ELSE
            BEGIN
                INSERT INTO ' + @qualifiedTable + ' (id' + 
                    CASE WHEN @insertCols <> '' THEN ', ' + @insertCols ELSE '' END + ')
                VALUES (@id' + 
                    CASE WHEN @insertVals <> '' THEN ', ' + @insertVals ELSE '' END + ');
            END;';
        END
        ELSE
        BEGIN
            SET @sql = '
            IF NOT EXISTS (SELECT 1 FROM ' + @qualifiedTable + ' WHERE id = @id)
            BEGIN
                INSERT INTO ' + @qualifiedTable + ' (id' + 
                    CASE WHEN @insertCols <> '' THEN ', ' + @insertCols ELSE '' END + ')
                VALUES (@id' + 
                    CASE WHEN @insertVals <> '' THEN ', ' + @insertVals ELSE '' END + ');
            END;';
        END

        EXEC sp_executesql @sql, N'@id NVARCHAR(MAX)', @id = @id;

    END TRY
    BEGIN CATCH
        IF CURSOR_STATUS('local', 'input_cursor') >= 0
            CLOSE input_cursor;
        IF CURSOR_STATUS('local', 'input_cursor') >= -1
            DEALLOCATE input_cursor;

        DECLARE @ErrMsg NVARCHAR(4000), @ErrSeverity INT;
        SELECT 
            @ErrMsg = ERROR_MESSAGE(),
            @ErrSeverity = ERROR_SEVERITY();

        RAISERROR(@ErrMsg, @ErrSeverity, 1);
    END CATCH
END;