"# test" 

SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, QUOTED_IDENTIFIER, CONCAT_NULL_YIELDS_NULL, ARITHABORT ON
GO
SET NUMERIC_ROUNDABORT OFF
GO

SET NOCOUNT ON
GO




IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.Settings') AND type in (N'U'))
  AND NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.Settings') AND name = N'IsHidden') 
	ALTER TABLE dbo.Settings
		ADD IsHidden BIT NOT NULL CONSTRAINT DF_Settings_IsHidden DEFAULT 0
GO


-- -------------------------------------------------------------
-- dbo.WorkerQueue
-- -------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.check_constraints WHERE name = N'CK_WorkerQueue_Action' AND parent_object_id = OBJECT_ID(N'dbo.WorkerQueue'))
	ALTER TABLE dbo.WorkerQueue DROP CONSTRAINT CK_WorkerQueue_Action
GO

ALTER TABLE dbo.WorkerQueue WITH CHECK 
	ADD CONSTRAINT CK_WorkerQueue_Action CHECK ([Action] IN (1, 2, 3))
GO

-----------------------------------------------------------
-- dbo.Tree
-----------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.Tree') AND name = N'ParentCreatedBy') 
BEGIN
	ALTER TABLE dbo.Tree ADD 
		ParentCreatedBy	SMALLINT NULL
						CONSTRAINT FK_Tree_ParentCreatedBy FOREIGN KEY (ParentCreatedBy) REFERENCES dbo.ACTOR(ID) 
							ON DELETE SET DEFAULT 
END
GO

-- --------------------------------------------------------
-- dbo.CategoryFlagsType
-- --------------------------------------------------------
IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Legacy.TreeCountDirectDescendants') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
	DROP FUNCTION Legacy.TreeCountDirectDescendants
GO

IF EXISTS (SELECT * FROM sys.types  WHERE is_table_type = 1 AND name ='CategoryFlagsType')
	DROP TYPE dbo.CategoryFlagsType
GO

IF NOT EXISTS (SELECT * FROM sys.types  WHERE is_table_type = 1 AND name ='CategoryFlagsType')
	CREATE TYPE dbo.CategoryFlagsType AS TABLE 
	( 
		CategoryId	SMALLINT, 
		MaxFlag		SMALLINT,
		[Owner]		SMALLINT 
	)
GO

IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = OBJECT_ID(N'dbo.TreeMove') AND OBJECTPROPERTY(id,N'IsProcedure') = 1)
	DROP PROCEDURE dbo.TreeMove
GO


-- -------------------------------------------------------------
-- dbo.SCHEMA
-- -------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.objects WHERE type='D' and name = 'DF_SCHEMA_VERSION_DATE')
	ALTER TABLE dbo.[SCHEMA] DROP CONSTRAINT DF_SCHEMA_VERSION_DATE
GO

IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.[SCHEMA]') AND name = N'VERSION_DATE' AND system_type_id = 61)
	ALTER TABLE dbo.[SCHEMA]
		ALTER COLUMN VERSION_DATE DATETIME2(0) NOT NULL
GO

ALTER TABLE dbo.[SCHEMA]
	ADD CONSTRAINT DF_SCHEMA_VERSION_DATE DEFAULT GETUTCDATE() FOR VERSION_DATE
GO


-- -------------------------------------------------------------
-- dbo.CATEGORY
-- -------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.foreign_keys WHERE Name = N'FK_CATEGORY__FORM' AND parent_object_id = OBJECT_ID(N'dbo.CATEGORY'))
BEGIN
	ALTER TABLE dbo.CATEGORY
		DROP CONSTRAINT FK_CATEGORY__FORM
	
	ALTER TABLE dbo.CATEGORY WITH CHECK 
		ADD CONSTRAINT FK_CATEGORY_FORM_ID FOREIGN KEY (FORM_ID) REFERENCES dbo.FORM(ID) 
			ON DELETE SET DEFAULT
END
GO

IF EXISTS (SELECT * FROM sys.foreign_keys WHERE Name = N'FK_NOTE_RECORD_ID' AND parent_object_id = OBJECT_ID(N'dbo.NOTE') AND delete_referential_action = 0)
	ALTER TABLE dbo.NOTE
		DROP CONSTRAINT FK_NOTE_RECORD_ID
GO
	
--%% NOTE
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE Name = N'FK_NOTE_RECORD_ID' AND parent_object_id = OBJECT_ID(N'dbo.NOTE'))
	ALTER TABLE dbo.NOTE WITH CHECK 
		ADD CONSTRAINT FK_NOTE_RECORD_ID FOREIGN KEY (RECORD_ID) REFERENCES dbo.RECORD (ID) ON DELETE CASCADE
GO


-- dbo.TEMPLATE
-- -------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.TEMPLATE') AND name = N'UQ_TEMPLATE_NAME' AND is_unique_constraint = 1)
	ALTER TABLE dbo.TEMPLATE
		ADD CONSTRAINT UQ_TEMPLATE_NAME UNIQUE NONCLUSTERED  (NAME)
GO



-- -------------------------------------------------------------
-- dbo.FIELD
-- -------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.FIELD') AND name = N'Immutable') 
BEGIN
	EXEC sys.sp_executesql N'
  	ALTER TABLE dbo.FIELD WITH CHECK
		ADD 
		Immutable	 BIT		  NOT NULL CONSTRAINT DF_FIELD_Immutable DEFAULT 0,
		Unselectable BIT		  NOT NULL CONSTRAINT DF_FIELD_Unselectable DEFAULT 0,
		SeqNum		 INT		  NOT NULL CONSTRAINT DF_FIELD_SeqNum DEFAULT 0,
		Label		 NVARCHAR(40) NOT NULL CONSTRAINT DF_FIELD_Label DEFAULT ''''
	'
END
GO	


-- ---------------------------------------------------------------
-- Status (0 - processing; >= 1 - completed)
-- ---------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'Mail.Status') AND type in (N'U'))
BEGIN
	CREATE TABLE Mail.[Status]
	(
		 StatusId		TINYINT				
		,Name			NVARCHAR(50)	NOT NULL
		,[Description]	NVARCHAR(128)	NOT NULL CONSTRAINT DF_Mail_Status_Description DEFAULT N''

		,CONSTRAINT PK_Mail_Status PRIMARY KEY (StatusId)
		,CONSTRAINT UQ_Mail_Status_Name UNIQUE (Name)
		,CONSTRAINT CK_Mail_Status_Name CHECK (LEN(Name) > 0)
	)
	
	INSERT	Mail.[Status](StatusId, Name, [Description]) 
	VALUES	
	-- PROCESSING status
			(0, N'Processing',	N'The message is ready to be sent'),
	-- COMPLETED status
			(1, N'Sent',		N'The message has been sent'),
			(2, N'Canceled',	N'The message will not be sent'),
			(3, N'Faulted',		N'The message failed to send')
END
GO


-- -------------------------------------------------------------
-- dbo.DeferredDelete
-- -------------------------------------------------------------
--%% DeferredDelete
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.DeferredDelete') AND type in (N'U'))
	CREATE TABLE dbo.DeferredDelete
	(
		Id			INT				IDENTITY(1,1) CONSTRAINT PK_DeferredDelete PRIMARY KEY CLUSTERED,
		RecordId	BIGINT			NOT NULL,
		[Version]	SMALLINT		NOT NULL,
		Extension 	NVARCHAR(50)	NOT NULL CONSTRAINT DF_DeferredDelete_Extension DEFAULT '',
        Effective	DATETIME2(7)	NOT NULL CONSTRAINT DF_DeferredDelete_Effective DEFAULT DATEADD(MINUTE, 30, GETUTCDATE()),

		CONSTRAINT UQ_DeferredDelete UNIQUE NONCLUSTERED
		(
			RecordId,
			[Version]
		),
	)
ELSE 
BEGIN
	IF EXISTS (SELECT * FROM sys.objects WHERE type='D' and name = 'DF_DeferredDelete_Effective')
		ALTER TABLE dbo.DeferredDelete DROP CONSTRAINT DF_DeferredDelete_Effective

	ALTER TABLE dbo.DeferredDelete
		ADD CONSTRAINT DF_DeferredDelete_Effective DEFAULT GETUTCDATE() FOR Effective

END
GO



-- -------------------------------------------------------------
-- Workflow.Task
-- -------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'Workflow.Task') AND name = N'DueDate' AND is_computed = 1)
BEGIN
	BEGIN TRANSACTION

	IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'Workflow.Task') AND name = N'DueDate2')
	BEGIN
		ALTER TABLE Workflow.Task ADD DueDate2 DATETIME2(0)
	END

	EXECUTE [dbo].[sp_executesql] N'UPDATE Workflow.Task SET DueDate2 =  DATEADD(second, -1, CAST(DATEADD(day, 1, DueDate) AS DATETIME2(0)))'

	ALTER TABLE Workflow.Task DROP COLUMN DueDate

	IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'Workflow.Task') AND name = N'Duration')
		ALTER TABLE Workflow.Task DROP COLUMN Duration

	EXECUTE [dbo].[sp_rename] 'Workflow.Task.DueDate2', 'DueDate', 'COLUMN'

	COMMIT TRANSACTION
END
GO




IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.SYSTEM_ACTION') AND type in (N'U'))
--	DROP TABLE dbo.SYSTEM_ACTION
	EXEC sp_rename 'dbo.SYSTEM_ACTION', 'BAK_SYSTEM_ACTION';
GO



IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.WorkerQueue') AND name = N'Priority') 
BEGIN
	EXEC sys.sp_executesql N'
	ALTER TABLE dbo.WorkerQueue WITH CHECK
		ADD 
		[Priority] TINYINT NOT NULL CONSTRAINT DF_WorkerQueue_Priority DEFAULT (0)
	'
END
GO


IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD_VERSION') AND name = N'IX_RECORD_VERSION_Extension')
	DROP INDEX IX_RECORD_VERSION_Extension ON dbo.RECORD_VERSION
	
CREATE NONCLUSTERED INDEX IX_RECORD_VERSION_Extension ON dbo.RECORD_VERSION 
(
	Extension
)
INCLUDE	
(
	[VERSION],
	ID
)
GO



IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.STATE_ACTION') AND type in (N'U'))
	--DROP TABLE dbo.STATE_ACTION
	EXEC sp_rename 'dbo.STATE_ACTION', 'BAK_STATE_ACTION';
GO



IF EXISTS (SELECT * FROM sys.tables WHERE name LIKE 'BAK_%' OR name LIKE 'MIGRATION_%' OR name = 'CMx' OR name = 'Ent')
BEGIN
	DECLARE FK_Cursor CURSOR
		LOCAL FORWARD_ONLY READ_ONLY STATIC TYPE_WARNING
		FOR
			SELECT	QUOTENAME(s.name) + N'.' + QUOTENAME(t.name), QUOTENAME(fk.name)
			  FROM	sys.foreign_keys AS fk WITH (NOLOCK)
					JOIN sys.tables AS t WITH (NOLOCK) 
						ON t.[object_id] = fk.parent_object_id
					JOIN sys.schemas AS s WITH (NOLOCK) 
						ON s.[schema_id]= t.[schema_id]
			WHERE	s.name = 'dbo' 
					AND
					(t.name LIKE 'BAK_%' OR t.name LIKE 'MIGRATION_%' OR t.name = 'CMx' OR t.name = 'Ent')

	DECLARE 
		@tableName	SYSNAME,
		@fkName		SYSNAME,
		@sql		NVARCHAR(1024)

	OPEN FK_Cursor
	FETCH NEXT FROM FK_Cursor INTO @tableName, @fkName

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		SET @sql = 'ALTER TABLE ' + @tableName + ' DROP CONSTRAINT ' + @fkName

		EXEC sys.sp_executesql @sql		
 
		FETCH NEXT FROM FK_Cursor INTO @tableName, @fkName
	END        

	CLOSE FK_Cursor
	DEALLOCATE FK_Cursor

	DECLARE Table_Cursor CURSOR
		LOCAL FORWARD_ONLY READ_ONLY STATIC TYPE_WARNING
		FOR
			SELECT	QUOTENAME(s.name) + N'.' + QUOTENAME(t.name)
			  FROM	sys.tables AS t WITH (NOLOCK) 
					JOIN sys.schemas AS s WITH (NOLOCK) 
						ON s.[schema_id]= t.[schema_id]
			WHERE	s.name = 'dbo' 
					AND
					(t.name LIKE 'BAK_%' OR t.name LIKE 'MIGRATION_%' OR t.name = 'CMx' OR t.name = 'Ent')

	OPEN Table_Cursor
	FETCH NEXT FROM Table_Cursor INTO @tableName

	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		SET @sql = 'DROP TABLE ' + @tableName

		EXEC sys.sp_executesql @sql		
 
		FETCH NEXT FROM Table_Cursor INTO @tableName
	END        

	CLOSE Table_Cursor
	DEALLOCATE Table_Cursor
END
GO



-- -------------------------------------------------------------
-- dbo.RECORD_VERSION
-- -------------------------------------------------------------
--%% RECORD_VERSION (missing actors)
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD_VERSION') AND name = N'CreatedOn') 
BEGIN
	UPDATE	dbo.RECORD_VERSION
	   SET	OWNER_ID = 3		-- SYSTEM
	 WHERE  NOT EXISTS (SELECT * FROM ACTOR WHERE ID = OWNER_ID)
END
GO

--%% RECORD_VERSION (converting)
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD_VERSION') AND name = N'CreatedOn') 
BEGIN
	SET XACT_ABORT ON

	DECLARE @internalTransaction INT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END
	
	BEGIN TRY
		IF (@internalTransaction = 1)
			BEGIN TRANSACTION

		IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD_VERSION') AND name = N'CHECKSUM2')
		BEGIN

			EXEC sys.sp_executesql N'
			ALTER TABLE dbo.RECORD_VERSION 
				ADD 
				CreatedOn	DATETIME2(0) NOT NULL CONSTRAINT DF_RECORD_VERSION_CreatedOn DEFAULT (GETUTCDATE()),
				Size		BIGINT		 NOT NULL CONSTRAINT DF_RECORD_VERSION_Size DEFAULT -1,
				Extension	NVARCHAR(50) NOT NULL CONSTRAINT DF_RECORD_VERSION_Extension DEFAULT '''',
				OutFileType	TINYINT		 NULL,
				CHECKSUM2	BINARY(16)
			'
			ALTER TABLE dbo.RECORD_VERSION 
				DROP 
				CONSTRAINT DF_RECORD_VERSION_Size,
				CONSTRAINT DF_RECORD_VERSION_Extension

			EXEC sys.sp_executesql N'
			ALTER TABLE dbo.RECORD_VERSION WITH CHECK 
				ADD 
				CONSTRAINT CK_RECORD_VERSION_OutFileType CHECK (OutFileType IS NULL OR OutFileType IN (1, 4, 5, 6, 7, 9, 10, 11, 13, 14, 101))
			'
		END

		IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD_VERSION') AND name = N'Extension')
		BEGIN
			ALTER TABLE dbo.RECORD_VERSION 
				ADD 
				Extension	NVARCHAR(50) NOT NULL CONSTRAINT DF_RECORD_VERSION_Extension DEFAULT ''

			ALTER TABLE dbo.RECORD_VERSION 
				DROP 
				CONSTRAINT DF_RECORD_VERSION_Extension
		END

		EXEC sys.sp_executesql N'

		DECLARE @TimeZone VARCHAR(50)
		EXEC MASTER.dbo.xp_regread ''HKEY_LOCAL_MACHINE'', ''SYSTEM\CurrentControlSet\Control\TimeZoneInformation'', ''TimeZoneKeyName'', @TimeZone OUT

		DECLARE @utcOffset INT = DATEDIFF(MINUTE, GETDATE(), GETUTCDATE())
		DECLARE @dstOffset INT = (CASE WHEN	CASE 
												WHEN CHARINDEX(''Eastern'',  @TimeZone) > 0 THEN 4 
												WHEN CHARINDEX(''Central'',  @TimeZone) > 0 THEN 5
												WHEN CHARINDEX(''Mountain'', @TimeZone) > 0 THEN 6
												WHEN CHARINDEX(''Pacific'',  @TimeZone) > 0 THEN 7
												WHEN CHARINDEX(''Alaska'',   @TimeZone) > 0 THEN 8
												WHEN CHARINDEX(''Hawaii'',   @TimeZone) > 0 THEN 9
												ELSE 0
											END * 60 = @utcOffset
										 THEN 60
										 ELSE 0
								   END)
		DECLARE @midnight TIME = ''00:00:00''
					
		UPDATE	dbo.RECORD_VERSION 
			SET 
				CreatedOn = CASE WHEN r.VERSION_DATE >= ''9999-12-30 23:59:59'' THEN ''9999-12-31 23:59:59''
							 WHEN CAST(r.VERSION_DATE AS TIME) = @midnight OR DATEPART(MM, r.VERSION_DATE) < 3 OR DATEPART(MM, r.VERSION_DATE) >= 11
								THEN DATEADD(MINUTE, @utcOffset + @dstOffset, r.VERSION_DATE)
								ELSE 
									CASE WHEN @dstOffset = 0
										THEN DATEADD(MINUTE, @utcOffset - 60, r.VERSION_DATE)
										ELSE DATEADD(MINUTE, @utcOffset, r.VERSION_DATE)
									END
							END,
				Extension = COALESCE(R.EXTENSION, ''''),
				CHECKSUM2 = CONVERT(BINARY(16), [CHECKSUM], 2)
			FROM	
				dbo.RECORD_VERSION AS rv
				JOIN dbo.RECORD AS r WITH (NOLOCK) 
					ON r.ID = rv.RECORD_ID
	
		IF EXISTS (
				   SELECT * 
					 FROM dbo.RECORD_VERSION 
					WHERE ([CHECKSUM] IS NULL AND CHECKSUM2 IS NOT NULL) 
					      OR 
						  ([CHECKSUM] IS NOT NULL AND CHECKSUM2 <> CONVERT(BINARY(16), [CHECKSUM], 2))
				  )
			RAISERROR (N''CHECKSUM <> CHECKSUM2'', 16, 1);
		'

		ALTER TABLE dbo.RECORD_VERSION DROP COLUMN [CHECKSUM]

		EXECUTE [dbo].[sp_rename] 'dbo.RECORD_VERSION.CHECKSUM2', 'CHECKSUM', 'COLUMN'
	
		IF (@internalTransaction = 1)
			COMMIT TRANSACTION
    END TRY
    BEGIN CATCH
        IF (@internalTransaction = 1 AND XACT_STATE() <> 0)
			ROLLBACK TRANSACTION;

        DECLARE @ErrorMessage NVARCHAR(MAX) =
                N'Msg ' + CONVERT(NVARCHAR(50), ERROR_NUMBER()) +
                N', Level ' + CONVERT(NVARCHAR(10), ERROR_SEVERITY()) +
                N', State ' + CONVERT(NVARCHAR(10), ERROR_STATE()) +
                COALESCE(N', Procedure ' + ERROR_PROCEDURE(), N'') +
                N', Line ' + CONVERT(VARCHAR(10), ERROR_LINE()) +
                NCHAR(13) + NCHAR(10) + ERROR_MESSAGE()

        RAISERROR(@ErrorMessage, 16, 1)
    END CATCH
END 
ELSE IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD_VERSION') AND name = N'OutFileType') 
BEGIN
	EXEC sys.sp_executesql N'
	ALTER TABLE dbo.RECORD_VERSION WITH CHECK
		ADD 
		OutFileType	TINYINT	NULL
	'
	EXEC sys.sp_executesql N'
	ALTER TABLE dbo.RECORD_VERSION WITH CHECK 
		ADD 
		CONSTRAINT CK_RECORD_VERSION_OutFileType CHECK (OutFileType IS NULL OR OutFileType IN (1, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 101, 102, 103))
	'
END
GO	




CREATE FUNCTION dbo.CreateSortableName 
(
	@name  NVARCHAR(255),
	@date  DATETIME2,
	@order TINYINT = 0
)
	RETURNS NVARCHAR(255)
WITH SCHEMABINDING
BEGIN
	DECLARE 
		@result NVARCHAR(255) = '',
		@sdate  NVARCHAR(23)  = COALESCE(CONVERT(NVARCHAR(23), @date, 126), ''),
		@sorder NVARCHAR(3)   = COALESCE(RIGHT('000' + CAST(@order AS NVARCHAR(3)),3), '')

	IF (@name IS NOT NULL)
	BEGIN
		DECLARE @c   INT
		DECLARE @pos INT = 1

		WHILE @pos <= LEN(@name)
		BEGIN
			SET @c = UNICODE(SUBSTRING(@name, @pos, 1))
			SET @pos = @pos + 1

			-- ISO 6429 control characters (C0 and C1):
			-- < 32    C0: ASCII control characters (U+0000-U+001F) 
			--  127	   C0: ASCII DEL (U+007F) 
			-- 128-159 C1: (U+0080-U+009F)
			--
			-- Unicode: 
			-- 8232 line separator (U+2028) 
			-- 8233 paragraph separator (U+2029)
			-- 9216-9279 control pictures (U+2400 - U+243F)
			IF @c < 32 OR @c = 127 OR 
			   @c BETWEEN 128 AND 159 OR
			   @c = 8232 OR @c = 8233 OR
			   @c BETWEEN 9216 AND 9279 OR
			   (@c = 32 AND LEN(@result) = 0)
			   CONTINUE

			SET @result = @result + UPPER(NCHAR(@c))
		END

	END

	IF (LEN(@result) = 0)
		SET @result = @sorder + ' ' + @sdate
	ELSE 
	BEGIN
		DECLARE @nonDigitPos INT = PATINDEX('%[^0-9]%', @result)
		IF (@nonDigitPos = 1 OR @nonDigitPos > 18)
			SET @result = @result
		ELSE 
			SET @result = COALESCE(REPLICATE('0', 15 - CASE WHEN @nonDigitPos = 0 THEN LEN(@result) ELSE @nonDigitPos - 1 END), '') + @result

		IF (LEN(@result) > 229)
			SET @result = SUBSTRING(@result, 1, 229)
	
		SET @result = @sorder + @result + @sdate
	END

	RETURN @result
END
GO



-- -------------------------------------------------------------
-- dbo.RECORD
-- -------------------------------------------------------------
--%% RECORD (preparing)
IF EXISTS(SELECT * FROM sys.views where object_id = OBJECT_ID('dbo.vTree'))
	DROP VIEW dbo.vTree
GO

--%% RECORD (preparing)
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_CATEGORY_ID')
	DROP INDEX IX_CATEGORY_ID ON dbo.RECORD 
GO

--%% RECORD (preparing)
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_EXTENSION')
	DROP INDEX IX_EXTENSION ON dbo.RECORD 
GO

--%% RECORD (preparing)
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_NAME')
	DROP INDEX IX_NAME ON dbo.RECORD 
GO

--%% RECORD (preparing)
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_RECORD_ID_DeletedBy_CATEGORY_ID')
	DROP INDEX IX_RECORD_ID_DeletedBy_CATEGORY_ID ON dbo.RECORD
GO

--%% RECORD (preparing)
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_RECORD_DeletedBy_CATEGORY')
	DROP INDEX IX_RECORD_DeletedBy_CATEGORY ON dbo.RECORD
GO

--%% RECORD (preparing)
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_RECORD_CATEGORY_ID')
	DROP INDEX IX_RECORD_CATEGORY_ID ON dbo.RECORD 
GO

--%% RECORD (preparing)
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_RECORD_NAME')
	DROP INDEX IX_RECORD_NAME ON dbo.RECORD 
GO

--%% RECORD (preparing)
IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_RECORD_ID_CATEGORY_TYPE')
	DROP INDEX IX_RECORD_ID_CATEGORY_TYPE ON dbo.RECORD 
GO

--%% RECORD (preparing)
IF EXISTS ( SELECT * FROM sys.objects WHERE type='D' and name = 'DF_RECOR_IsHidden')
BEGIN
	ALTER TABLE dbo.RECORD DROP CONSTRAINT DF_RECOR_IsHidden

	IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IsHidden') 
		ALTER TABLE dbo.RECORD DROP COLUMN IsHidden
END
GO

--%% RECORD (preparing)
IF EXISTS ( SELECT * FROM sys.objects WHERE type='D' and name = 'DF_RECORD_IsHidden')
BEGIN
	ALTER TABLE dbo.RECORD DROP CONSTRAINT DF_RECORD_IsHidden

	IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IsHidden') 
		ALTER TABLE dbo.RECORD DROP COLUMN IsHidden
END
GO

--%% RECORD (preparing)
IF EXISTS ( SELECT * FROM sys.objects WHERE type='D' and name = 'DF_RECORD_IsReadOnly')
BEGIN
	ALTER TABLE dbo.RECORD DROP CONSTRAINT DF_RECORD_IsReadOnly

	IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IsReadOnly') 
		ALTER TABLE dbo.RECORD DROP COLUMN IsReadOnly
END
GO

 --%% RECORD (missing actors)
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'CreatedOn') 
BEGIN
	EXEC sys.sp_executesql N'

	 UPDATE	dbo.RECORD
	   SET	STATE_OWNER_ID = 3	-- SYSTEM
	 WHERE  NOT EXISTS (SELECT * FROM ACTOR WHERE ID = STATE_OWNER_ID) 
	 '
END
GO

--%% RECORD (converting)
DECLARE @internalTransaction INT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END
DECLARE @RetentionDateFieldId SMALLINT = (SELECT ID FROM dbo.FIELD WHERE Name = 'RETENTION DATE' AND FLAGS = 1)
DECLARE @ErrorMessage NVARCHAR(MAX)

DECLARE @TimeZone VARCHAR(50)
EXEC MASTER.dbo.xp_regread 'HKEY_LOCAL_MACHINE', 'SYSTEM\CurrentControlSet\Control\TimeZoneInformation', 'TimeZoneKeyName', @TimeZone OUT

DECLARE @utcOffset INT = DATEDIFF(MINUTE, GETDATE(), GETUTCDATE())
DECLARE @dstOffset INT = (CASE WHEN	CASE 
										WHEN CHARINDEX('Eastern',  @TimeZone) > 0 THEN 4 
										WHEN CHARINDEX('Central',  @TimeZone) > 0 THEN 5
										WHEN CHARINDEX('Mountain', @TimeZone) > 0 THEN 6
										WHEN CHARINDEX('Pacific',  @TimeZone) > 0 THEN 7
										WHEN CHARINDEX('Alaska',   @TimeZone) > 0 THEN 8
										WHEN CHARINDEX('Hawaii',   @TimeZone) > 0 THEN 9
										ELSE 0
									END * 60 = @utcOffset
								 THEN 60
								 ELSE 0
						   END)
DECLARE @midnight TIME = '00:00:00'

IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'CreatedOn') 
BEGIN
	SET XACT_ABORT ON

	DECLARE @SysActorId SMALLINT = (SELECT MIN(ID) FROM dbo.ACTOR WHERE NAME IN ('System', '$System'))

	IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_RECORD_EXTENSION')
		DROP INDEX IX_RECORD_EXTENSION ON dbo.RECORD 

	-- CREATE #LastState

	IF OBJECT_ID('tempdb..#LastState') IS NOT NULL
		DROP TABLE #LastState

	CREATE TABLE #LastState
	(
		RECORD_ID	BIGINT		PRIMARY KEY,
		STATE_DATE	DATETIME	NOT NULL,
		STATE_ID	SMALLINT	NOT NULL,
		OWNER_ID	SMALLINT	NOT NULL
	)

	;WITH LastState
	AS
	(
		SELECT	RECORD_ID, STATE_ID, STATE_DATE, OWNER_ID
			   ,ROW_NUMBER() OVER(PARTITION BY RECORD_ID ORDER BY STATE_DATE DESC) AS RowNumber 
		  FROM	
				dbo.RECORD_STATE WITH (NOLOCK)
		 WHERE	
				STATE_ID <> 5 
				AND 
				STATE_ID <> 8 
					
	)
	INSERT  #LastState(RECORD_ID, STATE_DATE, STATE_ID, OWNER_ID)
	SELECT	
			RECORD_ID, STATE_DATE, STATE_ID, OWNER_ID
	  FROM	
			LastState
	 WHERE	
			RowNumber = 1			

	-- CREATE #Created

	IF OBJECT_ID('tempdb..#Created') IS NOT NULL
		DROP TABLE #Created

	CREATE TABLE #Created
	(
		RECORD_ID	BIGINT		PRIMARY KEY,
		CreatedBy	SMALLINT	NOT NULL,
		CreatedOn	DATETIME	NOT NULL
	)

	EXEC sys.sp_executesql N'
	
	DECLARE @CreatedFieldId	SMALLINT = (SELECT ID FROM dbo.FIELD WHERE Name = ''CREATED'' AND FLAGS = 1)

	;WITH Created
	AS
	(
		SELECT	vd.RECORD_ID, vd.VALUE 
		  FROM	[dbo].[VALUE_DATE] AS vd WITH (NOLOCK)
		 WHERE	FIELD_ID = @CreatedFieldId
	)
	,FirstState
	AS
	(
		SELECT	RECORD_ID, STATE_DATE, OWNER_ID
				,ROW_NUMBER() OVER(PARTITION BY RECORD_ID ORDER BY STATE_DATE ASC) AS RowNumber 
			FROM	
				dbo.RECORD_STATE WITH (NOLOCK)
	)
	INSERT  #Created(RECORD_ID, CreatedBy, CreatedOn)
	SELECT	
			r.ID
		   ,COALESCE(fs.OWNER_ID, r.STATE_OWNER_ID)
		   ,CASE 
				WHEN c.VALUE IS NULL AND fs.STATE_DATE IS NULL 
					THEN r.STATE_DATE
				WHEN c.VALUE IS NOT NULL AND (fs.STATE_DATE IS NULL OR c.VALUE < fs.STATE_DATE)
					THEN CASE WHEN c.VALUE < r.STATE_DATE THEN c.VALUE ELSE r.STATE_DATE END
				WHEN fs.STATE_DATE IS NOT NULL AND (c.VALUE IS NULL OR fs.STATE_DATE < c.VALUE)
					THEN CASE WHEN fs.STATE_DATE < r.STATE_DATE THEN fs.STATE_DATE ELSE r.STATE_DATE END
				ELSE 
					r.STATE_DATE
			END
	  FROM	
			dbo.RECORD AS r WITH(NOLOCK)
			LEFT JOIN Created AS c
				ON c.RECORD_ID = r.ID
			LEFT JOIN FirstState AS fs
				ON fs.RECORD_ID = r.ID AND fs.RowNumber = 1

	UPDATE	#Created WITH (TABLOCK)
		SET  
			CreatedOn = CASE WHEN CreatedOn >= ''9999-12-30 23:59:59'' THEN ''9999-12-31 23:59:59''
							WHEN CAST(CreatedOn AS TIME) = @midnight OR DATEPART(MM, CreatedOn) < 3 OR DATEPART(MM, CreatedOn) >= 11
								THEN DATEADD(MINUTE, @utcOffset + @dstOffset, CreatedOn)
								ELSE 
									CASE WHEN @dstOffset = 0
										THEN DATEADD(MINUTE, @utcOffset - 60, CreatedOn)
										ELSE DATEADD(MINUTE, @utcOffset, CreatedOn)
									END
							END
	'	
	, N'@utcOffset INT, @dstOffset INT, @midnight TIME',  @utcOffset, @dstOffset, @midnight

	UPDATE #LastState
		SET STATE_DATE = CASE WHEN STATE_DATE >= '9999-12-30 23:59:59' THEN '9999-12-31 23:59:59'
							WHEN CAST(STATE_DATE AS TIME) = @midnight OR DATEPART(MM, STATE_DATE) < 3 OR DATEPART(MM, STATE_DATE) >= 11
							THEN DATEADD(MINUTE, @utcOffset + @dstOffset, STATE_DATE)
							ELSE 
								CASE WHEN @dstOffset = 0
									THEN DATEADD(MINUTE, @utcOffset - 60, STATE_DATE)
									ELSE DATEADD(MINUTE, @utcOffset, STATE_DATE)
								END
						END
	BEGIN TRY
		IF (@internalTransaction = 1)
			BEGIN TRANSACTION

		-- --------------------------------------
		-- Convert DATETIME fields to UTC
		-- !!! WARNING: Must be run ONLY ONCE !!!
		-- --------------------------------------
		UPDATE dbo.VALUE_DATE
			SET VALUE = CASE WHEN VALUE >= '9999-12-30 23:59:59' THEN '9999-12-31 23:59:59'
							 WHEN CAST(VALUE AS TIME) = @midnight OR DATEPART(MM, VALUE) < 3 OR DATEPART(MM, VALUE) >= 11
								THEN DATEADD(MINUTE, @utcOffset + @dstOffset, VALUE)
								ELSE 
									CASE WHEN @dstOffset = 0
										THEN DATEADD(MINUTE, @utcOffset - 60, VALUE)
										ELSE DATEADD(MINUTE, @utcOffset, VALUE)
									END
							END

		ALTER TABLE dbo.RECORD 
			ADD 
			Flags		SMALLINT		NOT NULL CONSTRAINT DF_RECORD_Flags DEFAULT 0,
			CreatedBy	SMALLINT		NOT NULL CONSTRAINT DF_RECORD_CreatedBy DEFAULT 0,
			CreatedOn	DATETIME2(0)	NOT NULL CONSTRAINT DF_RECORD_CreatedOn DEFAULT (GETUTCDATE()),
			UpdatedBy	SMALLINT		NOT NULL CONSTRAINT DF_RECORD_UpdatedBy DEFAULT 0,	-- temporary
			UpdatedOn	DATETIME2(0)	NOT NULL CONSTRAINT DF_RECORD_UpdatedOn DEFAULT (GETUTCDATE()),
			Expiration	DATETIME2(0)	NULL,	-- the record will be deleted after this date (set by the retention policy)
			OnHold		DATETIME2(0)	NULL,	-- if a record is on hold, it cannot be modified or deleted
			VersionId	INT				NULL
				
		EXEC sys.sp_executesql N'

		UPDATE	dbo.RECORD WITH (TABLOCK)
		   SET  
				VERSION_DATE = CASE WHEN VERSION_DATE >= ''9999-12-30 23:59:59'' THEN ''9999-12-31 23:59:59''
								WHEN CAST(VERSION_DATE AS TIME) = @midnight OR DATEPART(MM, VERSION_DATE) < 3 OR DATEPART(MM, VERSION_DATE) >= 11
									THEN DATEADD(MINUTE, @utcOffset + @dstOffset, VERSION_DATE)
									ELSE 
										CASE WHEN @dstOffset = 0
											THEN DATEADD(MINUTE, @utcOffset - 60, VERSION_DATE)
											ELSE DATEADD(MINUTE, @utcOffset, VERSION_DATE)
										END
								END,
				STATE_DATE = CASE WHEN STATE_DATE >= ''9999-12-30 23:59:59'' THEN ''9999-12-31 23:59:59''
								WHEN CAST(STATE_DATE AS TIME) = @midnight OR DATEPART(MM, STATE_DATE) < 3 OR DATEPART(MM, STATE_DATE) >= 11
									THEN DATEADD(MINUTE, @utcOffset + @dstOffset, STATE_DATE)
									ELSE 
										CASE WHEN @dstOffset = 0
											THEN DATEADD(MINUTE, @utcOffset - 60, STATE_DATE)
											ELSE DATEADD(MINUTE, @utcOffset, STATE_DATE)
										END
								END
					
		UPDATE	dbo.RECORD WITH (TABLOCK)
		   SET  
				RECORD_TYPE =
					CASE r.RECORD_TYPE
						WHEN 2 THEN	1	-- RECORD
						WHEN 3 THEN	0	-- LINK TO FOLDER
						WHEN 4 THEN	1	-- LINK TO FILE
						WHEN 5 THEN	1	-- LINK TO RECORD
						WHEN 9 THEN	6	-- LINK TO AGENDA MEETING
						WHEN 10 THEN 7	-- LINK TO AGENDA SECTION
						WHEN 11 THEN 8	-- LINK TO AGENDA ITEM
						WHEN 15 THEN 12	-- LINK TO DOCUMENT
						ELSE r.RECORD_TYPE
					END 
			    ,Flags = 
					CASE 
						WHEN r.STATE_ID = 5 THEN 0x0801										-- LOCKED | READONLY | CREATED
						WHEN r.STATE_ID = 8 THEN 
                            0x4000 | CASE WHEN r.RECORD_TYPE = 1 AND ls.STATE_ID IS NOT NULL
                                        THEN ls.STATE_ID				                    -- DELETED | [previous]
                                        ELSE 1				            			    	-- DELETED | CREATED
                                     END
						WHEN r.STATE_ID = 10 THEN 0x0001									-- CREATED (SUBMITTED)
						WHEN r.STATE_ID = 15 THEN 0x1001									-- HIDDEN | CREATED (POSTED)
						WHEN r.STATE_ID = 16 THEN 0x0401									-- READONLY | CREATED (PUBLISHED)
						WHEN r.STATE_ID IN (3, 4) AND r.RECORD_TYPE IN (15, 12) THEN 0x0001	-- CREATED
						ELSE r.STATE_ID
					END 
					| CASE 
						WHEN r.RECORD_TYPE IN (6, 8) THEN
							CASE
								WHEN r.STATE_ID IN (5, 8) 	-- LOCKED /	DELETED
									THEN COALESCE(
												CASE WHEN ls.STATE_ID = 15 THEN 0x1001 
													 WHEN ls.STATE_ID = 16 THEN 0x0401 
													 WHEN ls.STATE_ID = 10 THEN 0x0001		--  CREATED (SUBMITTED)
													 WHEN ls.STATE_ID = 1  THEN CASE WHEN RECORD_TYPE = 6 THEN 0x3000 ELSE 0x2000 END	-- DRAFT | HIDDEN (NOT SUBMITTED/POSTED)
													 ELSE 
														CASE 
															WHEN ls.STATE_ID IN (5, 8) THEN 1
															ELSE ls.STATE_ID
														END
												END, 1)
								WHEN r.STATE_ID = 16 THEN 0x0400	-- READONLY (PUBLISHED)
								WHEN r.STATE_ID = 15 THEN 0x1000	-- HIDDEN (POSTED)
								WHEN r.STATE_ID = 1  THEN CASE WHEN RECORD_TYPE = 6 THEN 0x3000 ELSE 0x2000 END -- DRAFT | HIDDEN (NOT SUBMITTED/POSTED)
								ELSE 0
							END 
					 	ELSE 
							CASE 
								WHEN r.STATE_ID = 5 THEN 0x0400	-- LOCKED -> READONLY
								ELSE 0 
							END
					  END
					| CASE 
						WHEN r.RECORD_TYPE = 1 AND r.EXTENSION = ''msg'' THEN 0x0100	-- EMAIL -> COMPOSITE
						ELSE 0
					  END																					  
				,CreatedBy = COALESCE(c.CreatedBy, VERSION_OWNER_ID)
				,CreatedOn = CASE WHEN c.CreatedOn IS NULL OR c.CreatedOn > r.VERSION_DATE THEN r.VERSION_DATE ELSE c.CreatedOn END
				,UpdatedBy = r.STATE_OWNER_ID
				,UpdatedOn = r.STATE_DATE
				,VersionId = rv.ID
				,Expiration = vd.Value
			FROM
				dbo.RECORD AS r
				LEFT JOIN #LastState AS ls
					ON ls.RECORD_ID = r.ID
				LEFT JOIN #Created AS c
					ON c.RECORD_ID = r.ID
				LEFT JOIN dbo.RECORD_VERSION AS rv
					ON rv.RECORD_ID = r.ID AND rv.[VERSION] = r.[VERSION]
				LEFT JOIN dbo.VALUE_DATE AS vd
					ON vd.RECORD_ID = r.ID AND vd.FIELD_ID = @RetentionDateFieldId
		'
		, N'@RetentionDateFieldId SMALLINT, @utcOffset INT, @dstOffset INT, @midnight TIME', @RetentionDateFieldId,  @utcOffset, @dstOffset, @midnight

		ALTER TABLE dbo.RECORD 
			DROP CONSTRAINT DF_RECORD_CreatedBy

		ALTER TABLE dbo.RECORD 
			DROP CONSTRAINT DF_RECORD_UpdatedBy

		IF EXISTS (SELECT * FROM sys.foreign_keys WHERE Name = N'FK_RECORD_STATE_OWNER_ID' AND parent_object_id = OBJECT_ID(N'dbo.RECORD_STATE'))
			ALTER TABLE dbo.RECORD_STATE
				DROP CONSTRAINT FK_RECORD_STATE_OWNER_ID
	
		ALTER TABLE dbo.RECORD WITH CHECK 
			ADD 
				CONSTRAINT FK_RECORD_STATE_OWNER_ID FOREIGN KEY (STATE_OWNER_ID) REFERENCES dbo.ACTOR(ID) 
					ON DELETE NO ACTION,
				CONSTRAINT FK_RECORD_CreatedBy FOREIGN KEY (CreatedBy) REFERENCES dbo.ACTOR(ID) 
					ON DELETE NO ACTION,
				CONSTRAINT FK_RECORD_UpdatedBy FOREIGN KEY (UpdatedBy) REFERENCES dbo.ACTOR(ID) 
					ON DELETE NO ACTION,
				CONSTRAINT FK_RECORD_VersionId FOREIGN KEY (VersionId) REFERENCES dbo.RECORD_VERSION(ID) 
					ON DELETE NO ACTION

		IF EXISTS ( SELECT * FROM sys.objects WHERE type='D' and name = 'DF_RECORD_VERSION_NUM')
			ALTER TABLE dbo.RECORD DROP CONSTRAINT DF_RECORD_VERSION_NUM

		IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'IX_STATE_DATE')
			DROP INDEX IX_STATE_DATE ON dbo.RECORD 

		ALTER TABLE dbo.RECORD 
			ALTER COLUMN NUMBERING NVARCHAR(50)

		IF EXISTS ( SELECT * FROM sys.objects WHERE type='D' and name = 'DF_RECORD_STATE_ID')
			ALTER TABLE dbo.RECORD DROP CONSTRAINT DF_RECORD_STATE_ID

		ALTER TABLE dbo.RECORD 
			DROP COLUMN
				[VERSION],
				VERSION_DATE,
				VERSION_OWNER_ID,
				EXTENSION

 		EXEC sys.sp_executesql N'

		ALTER TABLE dbo.RECORD 
			ADD
			SortName	AS dbo.CreateSortableName(NAME, CreatedOn, 0) PERSISTED,
			[State]		AS CAST(Flags & 255 AS SMALLINT),
			IsDeleted	AS CASE WHEN Flags & 16384 <> 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END,
			IsDraft		AS CASE WHEN Flags &  8192 <> 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END,
			IsHidden	AS CASE WHEN Flags &  4096 <> 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END,
			IsLocked	AS CASE WHEN Flags &  2048 <> 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END,
			IsReadOnly	AS CASE WHEN Flags &  1024 <> 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END,
			IsComposite	AS CASE WHEN Flags &   256 <> 0 THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END,
			IsRetained 	AS CASE WHEN Expiration > GETUTCDATE() OR OnHold > GETUTCDATE() THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END,
			NodeName	AS COALESCE(NUMBERING + '' '' + NAME, NAME)
		'

 		EXEC sys.sp_executesql N'

		ALTER TABLE dbo.RECORD WITH CHECK 
			ADD CONSTRAINT CK_RECORD_State CHECK (CAST(Flags & 255 AS SMALLINT) = 1											-- ALL: CREATED
													OR (RECORD_TYPE =  1 AND CAST(Flags & 255 AS SMALLINT) IN (3, 4, 14))	-- FILE: CHECKED OUT, CHECKED IN, ARCHIVED
													OR (RECORD_TYPE = 12 AND CAST(Flags & 255 AS SMALLINT) = 14)			-- DOCUMENT: ARCHIVED
													OR (RECORD_TYPE =  8 AND CAST(Flags & 255 AS SMALLINT) = 17)			-- AGENDA ITEM: UNASSIGNED
												 )		
		'

		DELETE dbo.RECORD_TYPE
		 WHERE ID IN (2,	-- RECORD
					  3,	-- LINK TO FOLDER
					  4,	-- LINK TO FILE
					  5,	-- LINK TO RECORD
					  9,	-- LINK TO AGENDA MEETING
					  10,	-- LINK TO AGENDA SECTION
					  11,	-- LINK TO AGENDA ITEM
					  15)	-- LINK TO DOCUMENT

		IF (@internalTransaction = 1)
			COMMIT TRANSACTION

		IF OBJECT_ID('tempdb..#Created') IS NOT NULL
			DROP TABLE #Created

		IF OBJECT_ID('tempdb..#LastState') IS NOT NULL
			DROP TABLE #LastState
    END TRY
    BEGIN CATCH
        IF (@internalTransaction = 1 AND XACT_STATE() <> 0)
			ROLLBACK TRANSACTION;

        SET @ErrorMessage =
                N'Msg ' + CONVERT(NVARCHAR(50), ERROR_NUMBER()) +
                N', Level ' + CONVERT(NVARCHAR(10), ERROR_SEVERITY()) +
                N', State ' + CONVERT(NVARCHAR(10), ERROR_STATE()) +
                COALESCE(N', Procedure ' + ERROR_PROCEDURE(), N'') +
                N', Line ' + CONVERT(VARCHAR(10), ERROR_LINE()) +
                NCHAR(13) + NCHAR(10) + ERROR_MESSAGE()

        RAISERROR(@ErrorMessage, 16, 1)
    END CATCH
END
ELSE IF EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'dbo.RECORD') AND name = N'Flags') 
BEGIN
 	EXEC sys.sp_executesql N'
	
	UPDATE	dbo.RECORD
	   SET	Flags = (Flags & 65280) + 1	-- SET TO ''CREATED''
	 WHERE	RECORD_TYPE = 12			-- DOCUMENT	
			AND
			[State] NOT IN (1, 14)
	'

 	EXEC sys.sp_executesql N'

	IF EXISTS (SELECT * FROM sys.check_constraints WHERE name = N''CK_RECORD_State'' AND parent_object_id = OBJECT_ID(N''dbo.RECORD''))
		ALTER TABLE dbo.RECORD DROP CONSTRAINT CK_RECORD_State

	ALTER TABLE dbo.RECORD WITH CHECK 
		ADD CONSTRAINT CK_RECORD_State CHECK (CAST(Flags & 255 AS SMALLINT) = 1											-- ALL: CREATED
												OR (RECORD_TYPE =  1 AND CAST(Flags & 255 AS SMALLINT) IN (3, 4, 14))	-- FILE: CHECKED OUT, CHECKED IN, ARCHIVED
												OR (RECORD_TYPE = 12 AND CAST(Flags & 255 AS SMALLINT) = 14)			-- DOCUMENT: ARCHIVED
												OR (RECORD_TYPE =  8 AND CAST(Flags & 255 AS SMALLINT) = 17)			-- AGENDA ITEM: UNASSIGNED
												)		
	'
END
GO




-- -------------------------------------------------------------
-- Mail.Message
-- -------------------------------------------------------------
--%% Mail.Message
DECLARE 
	@constraintName NVARCHAR(256),
	@sql NVARCHAR(MAX)

DECLARE Constraints_Cursor CURSOR
	LOCAL FORWARD_ONLY READ_ONLY STATIC TYPE_WARNING
	FOR
		SELECT  QUOTENAME(constraints.name)
		  FROM  sys.default_constraints AS constraints WITH(NOLOCK)
		 WHERE  parent_object_id = OBJECT_ID('Mail.Message')
		UNION
		SELECT  QUOTENAME(constraints.name)
		  FROM  sys.check_constraints AS constraints WITH(NOLOCK)
		 WHERE  parent_object_id = OBJECT_ID('Mail.Message')

OPEN Constraints_Cursor
FETCH NEXT FROM Constraints_Cursor INTO @constraintName

WHILE (@@FETCH_STATUS = 0)
BEGIN
	SET @sql = N'ALTER TABLE Mail.Message DROP CONSTRAINT ' + @constraintName
	EXEC sp_executesql @sql

	FETCH NEXT FROM Constraints_Cursor INTO @constraintName
END        

CLOSE Constraints_Cursor
DEALLOCATE Constraints_Cursor
GO







IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.OperationToString') AND type in (N'FN', N'IF', N'TF', N'FS', N'FT'))
	DROP FUNCTION dbo.OperationToString
GO

CREATE FUNCTION dbo.OperationToString (@scopeId TINYINT , @operation BIGINT)
	RETURNS VARCHAR(4000)
	WITH RETURNS NULL ON NULL INPUT 
AS
BEGIN
	IF (@scopeId IS NULL OR @operation IS NULL)
		RETURN NULL

	DECLARE @result NVARCHAR(4000)

	;WITH Operations
	AS
	(
		SELECT	TOP 64 o1.Name
		  FROM	dbo.Operation AS o1 WITH (NOLOCK)
				JOIN dbo.Operation AS o2 WITH (NOLOCK)
					ON o1.Code = o2.Code AND o1.ScopeId = o2.ScopeId AND o2.Code & @operation <> 0 AND o2.ScopeId = @scopeId
		ORDER BY o1.Code
	)
	SELECT @result = STUFF((SELECT ', ' + Name FROM Operations FOR XML PATH(''), TYPE).value('.', 'VARCHAR(4000)'), 1, 1, '')

	RETURN (@result)
END
GO





CREATE FUNCTION [Security].ActorGetCategories
(
	 @ActorId SMALLINT
	,@ScopeId SMALLINT = NULL	-- All
)
RETURNS TABLE
AS
RETURN
(
  	WITH Roles 
	AS
	(
		SELECT	ROLE_ID AS RID
	      FROM  dbo.MEMBER_ROLE WITH(NOLOCK)
		 WHERE	MEMBER_ID = @actorId
	)
	SELECT	DISTINCT CategoryId
	  FROM	[Security].AclCategory WITH(NOLOCK)
	 WHERE	(@ScopeId IS NULL OR ScopeId = @ScopeId)
			AND
			(@ActorId IS NULL
			 OR	
			 ActorId = @ActorId
			 OR
			 EXISTS (SELECT * FROM Roles WHERE RID = actorId))
)
GO



CREATE PROCEDURE Sms.RequestArchiveExpired
AS
BEGIN
    SET NOCOUNT ON

    DECLARE @internalTransaction INT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END

    IF (@internalTransaction = 1)
    BEGIN
        SET XACT_ABORT ON
        SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
        BEGIN TRANSACTION
    END

    DECLARE
        @statusReady    TINYINT
       ,@statusExpired  TINYINT
       ,@now            DATETIME2 = SYSUTCDATETIME()



    ;
    IF (@internalTransaction = 1)
        COMMIT TRANSACTION
END
GO




IF EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE is_table_type = 1 AND st.name = N'RequestTableType' AND ss.name = N'sms')
    DROP TYPE Sms.RequestTableType
GO
*/
IF NOT EXISTS (SELECT * FROM sys.types st JOIN sys.schemas ss ON st.schema_id = ss.schema_id WHERE is_table_type = 1 AND st.name = N'RequestTableType' AND ss.name = N'sms')
    CREATE TYPE Sms.RequestTableType AS TABLE
    (
        RequestCId      BIGINT          NOT NULL UNIQUE -- correlation ID
       ,SubscriptionCId BIGINT			NOT NULL
	   ,RecipientNumber	BIGINT			NOT NULL CHECK (RecipientNumber > 99999) 
       ,NoReply         BIT             NOT NULL DEFAULT (0)   
	   ,[Priority]	    TINYINT			NOT NULL CHECK([Priority] >= 0 AND [Priority] <= 9) DEFAULT (0) -- 0 is highest
       ,Created	        DATETIME2       NOT NULL
       ,Expiration      DATETIME2(0)    NOT NULL
       -- The initial lease period (aka visibility timeout) value in seconds. Default in one second. Max 7 days
       ,LeaseDuration   INT             NOT NULL CHECK(LeaseDuration >= 0 AND LeaseDuration <= 604800) DEFAULT 1
	   ,[Text]			NVARCHAR(1600)	NOT NULL CHECK (LEN([Text]) > 0)
       ,RequestStatus   TINYINT     NOT NULL CHECK(RequestStatus IN (0, 1, 11, 12, 13, 14))
       ,StatusChanged   DATETIME2   NOT NULL DEFAULT SYSUTCDATETIME()
    )
GO

CREATE PROCEDURE Sms.RequestAdd
(
    @request Sms.RequestTableType READONLY
)
AS
BEGIN
    SET NOCOUNT ON

    DECLARE @internalTransaction INT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END

    IF (@internalTransaction = 1)
    BEGIN
        SET XACT_ABORT ON
        SET TRANSACTION ISOLATION LEVEL REPEATABLE READ
        BEGIN TRANSACTION
    END
....


    UPDATE  [Sms].Request
-- TODO: RequestStatus !!!!!!!
--      SET  StatusId = @statusCanceled
       SET  RequestStatus = @statusCanceled
           ,StatusChanged = @now
    --------
    OUTPUT  inserted.CorrelationId, 0, @now, CAST(inserted.RecipientNumber AS NVARCHAR(20)), 'orphan' 
      INTO  [Sms].[DeliveryInfo] (CorrelationId, DeliveryStatus, [Timestamp], [From], ErrorCode)
    --------
      FROM  
            [Sms].Request AS r
                    
            LEFT JOIN Sms.Sender AS s
                ON  s.SubscriptionId = r.SubscriptionId 
                    AND 
                    s.RecipientNumber = r.RecipientNumber
-- TODO: RequestStatus !!!!!!!
--            StatusId = @statusReady
     WHERE  RequestStatus = @statusReady
            AND
            s.SenderId IS NULL
....

   IF (@internalTransaction = 1)
       COMMIT TRANSACTION





CREATE PROC dbo.TreeMove
(
    @RefNodeId		INT,
	@Position		TINYINT,
	@MovedNodeId	INT
)
AS
BEGIN
    SET NOCOUNT ON

	IF (@MovedNodeId IS NULL)
	BEGIN
		RAISERROR (N'Parameter @MovedNodeId is null.', 16, 1);
		RETURN -4
	END

	IF (@MovedNodeId = @RefNodeId)
	BEGIN
		RAISERROR (N'Cannot move to itself.', 16, 1);
		RETURN -3
	END

	DECLARE @internalTransaction INT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END

	IF (@internalTransaction = 1)
	BEGIN
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
	BEGIN TRANSACTION
	END

...

	SET @rowCount = @rowCount + @@ROWCOUNT
	-- DO NOT CHANGE - END --
	
	IF (@internalTransaction = 1)
	COMMIT TRANSACTION

	RETURN @rowCount
END
GO



CREATE PROC dbo.TreeInsert
(
    @RefNodeId	INT,
	@Position	TINYINT,
	@RecordId	BIGINT,
	@IsLink		BIT = 0,
	@NewNodeId	INT = -1 OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON

	DECLARE @internalTransaction INT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END

	IF (@internalTransaction = 1)
	BEGIN
	SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
	BEGIN TRANSACTION
	END

...


	DECLARE @output TABLE(newNodeId INT)

	INSERT dbo.Tree(RecordId, IsLink, [Path])
	OUTPUT INSERTED.NodeId INTO @output(newNodeId)
	VALUES (@RecordId, COALESCE(@IsLink, 0), @nodePath.GetDescendant(@path1, @path2))

	IF (@internalTransaction = 1)
	COMMIT TRANSACTION
		 
	SELECT TOP 1 @NewNodeId = newNodeId FROM @output
END
GO

