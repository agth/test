SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, QUOTED_IDENTIFIER, CONCAT_NULL_YIELDS_NULL, ARITHABORT ON
GO
SET NUMERIC_ROUNDABORT OFF
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'SyntheticFlowProcessPosition')
   EXEC('CREATE PROCEDURE dbo.SyntheticFlowProcessPosition AS SELECT 1;')
GO

ALTER PROCEDURE dbo.SyntheticFlowProcessPosition
    @processDate    DATE,
    @maxProcessDate DATE,
    @canceledCount  INT OUTPUT,
    @generatedCount INT OUTPUT
AS
BEGIN
    DECLARE
        @sfName NVARCHAR(50) = N'Position',
        @errorMessage NVARCHAR(MAX)

    IF @processDate IS NULL
        THROW 55101, N'Invalid parameter: @processDate cannot be NULL', 1;

    IF @maxProcessDate IS NULL
        THROW 55102, N'Invalid parameter: @maxProcessDate cannot be NULL', 1;

    IF @processDate < @maxProcessDate
        THROW 55102, N'The @processDate date must be greater or equal than the @maxProcessDate date', 1;

    SET @errorMessage = N'The SyntheticFlowProcess' + @sfName+  N' stored procedure must be called from within SyntheticFlowProcess stored procedure'

    IF OBJECT_ID('tempdb.dbo.#bizdate') IS NULL
        THROW 55201, @errorMessage, 1;

    IF OBJECT_ID('tempdb.dbo.#exrate') IS NULL
        THROW 55202, @errorMessage, 1;

    IF OBJECT_ID('tempdb.dbo.#txChange') IS NULL
        THROW 55203, @errorMessage, 1;

    IF OBJECT_ID('tempdb.dbo.#txNew') IS NULL
        THROW 55204, @errorMessage, 1;

    SET @errorMessage = N'The SyntheticFlowProcess' + @sfName +  N' stored procedure must be called within a transaction'

    IF @@TRANCOUNT = 0
        THROW 55300, @errorMessage, 1;

    -- trace ---------------------------------------------------------------
    DECLARE
        @traceTotalTimeStart DATETIME = GETDATE(),
        @traceTimeStart      DATETIME,
        @traceTimeEnd        DATETIME,
        @traceRowCount       INT,
        @traceMessage        NVARCHAR(MAX)
    ------------------------------------------------------------------------

    SET @canceledCount = 0
    SET @generatedCount = 0

    --=================================================================================
    -- AM Membership - cancel
    --=================================================================================
    -- trace ---------------------------------------------------------------
    SET @traceTimeStart = GETDATE();
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': AM Membership - canceling affected transactions...'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------

    SET @traceTimeStart = GETDATE();

    -- a same-day second run
    UPDATE  dbo.SyntheticFlowPosition
       SET  CancelationProcessDate = CASE WHEN n.MembershipDate BETWEEN @maxProcessDate AND @processDate OR sfp.PostDate BETWEEN @maxProcessDate AND @processDate
                                        THEN @processDate
                                        ELSE '0001-01-01' -- do not publish
                                     END,
            UpdatedByName = SYSTEM_USER,
            UpdatedTimestamp = SYSUTCDATETIME(),
            UpdatedByProgramName = APP_NAME()
      FROM
            #txNew AS n
            JOIN dbo.SyntheticFlowPosition AS sfp WITH (FORCESEEK)
                ON  sfp.CancelationProcessDate = '9999-12-31'
                    AND sfp.ProcessDate = @processDate
                    AND sfp.PortfolioUid = n.PortfolioUid
                    AND sfp.MemberPortfolioUid = n.MemberPortfolioUid
                    AND sfp.TransactionType = n.TransactionType
                    AND sfp.MembershipDate = n.MembershipDate
    OPTION (RECOMPILE)

    -- trace ---------------------------------------------------------------
    SET @traceRowCount = @@ROWCOUNT
    ------------------------------------------------------------------------

    UPDATE  dbo.SyntheticFlowPosition
       SET  CancelationProcessDate = CASE WHEN c.OldMembershipDate BETWEEN @maxProcessDate AND @processDate OR sfp.PostDate BETWEEN @maxProcessDate AND @processDate
                                        THEN @processDate
                                        ELSE '0001-01-01' -- do not publish
                                     END,
            UpdatedByName = SYSTEM_USER,
            UpdatedTimestamp = SYSUTCDATETIME(),
            UpdatedByProgramName = APP_NAME()
      FROM
            #txChange AS c
            JOIN dbo.SyntheticFlowPosition AS sfp WITH (FORCESEEK)
                ON  sfp.CancelationProcessDate = '9999-12-31'
                    AND sfp.ProcessDate <= @processDate  -- inlude a same-day previous run     
                    AND sfp.PortfolioUid = c.PortfolioUid
                    AND sfp.MemberPortfolioUid = c.MemberPortfolioUid
                    AND sfp.TransactionType = c.TransactionType
                    AND sfp.MembershipDate = c.OldMembershipDate
    OPTION (RECOMPILE)

    -- trace ---------------------------------------------------------------
    SET @traceRowCount = @@ROWCOUNT
    ------------------------------------------------------------------------

    UPDATE  dbo.SyntheticFlowPosition
       SET  CancelationProcessDate = CASE WHEN sfp.MembershipDate BETWEEN @maxProcessDate AND @processDate OR sfp.PostDate BETWEEN @maxProcessDate AND @processDate
                                        THEN @processDate
                                        ELSE '0001-01-01' -- do not publish
                                     END,
            UpdatedByName = SYSTEM_USER,
            UpdatedTimestamp = SYSUTCDATETIME(),
            UpdatedByProgramName = APP_NAME()
      FROM
            #txDelete AS d
            JOIN dbo.SyntheticFlowPosition AS sfp WITH (FORCESEEK)
                ON  sfp.CancelationProcessDate = '9999-12-31'
                    AND sfp.PortfolioAmMembershipUid = d.PortfolioAmMembershipUid
    OPTION (RECOMPILE)

    -- trace ---------------------------------------------------------------
    SET @traceRowCount = @traceRowCount + @@ROWCOUNT
    SET @traceTimeEnd = GETDATE();
    SET @canceledCount = @canceledCount + @traceRowCount
    IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
        SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
    ELSE
        SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': ' + CAST(@traceRowCount AS VARCHAR) + ' row(s) affected (' + @traceMessage
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
    ------------------------------------------------------------------------

    --=================================================================================
    -- AM Membership - new
    --=================================================================================
    -- trace ---------------------------------------------------------------
    SET @traceTimeStart = GETDATE();
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': AM Membership - generating new transactions...'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------

    INSERT dbo.SyntheticFlowPosition
    (
            ProcessDate, PortfolioUid, MemberPortfolioUid, MembershipDate, TransactionType, PortfolioAmMembershipUid, PostDate,
            PositionDate, ListingUid, IsLong, PositionTypeCode, LocalCurrencyUid, BaseCurrencyUid, ExchangeRateSource,
            ShareParQuantity, OriginalFaceAmount, LocalMarketValueAmount, LocalNotionalRiskExposureAmount,
            BaseMarketValueAmount,
            BaseNotionalRiskExposureAmount,
            CreatedByProgramName, UpdatedByProgramName
    )
    SELECT  @processDate AS ProcessDate, n.PortfolioUid, n.MemberPortfolioUid, n.MembershipDate, n.TransactionType, n.PortfolioAmMembershipUid, n.PostDate,
            ppmv.PositionDate, ppmv.ListingUid, ppmv.IsLong, ppmv.PositionTypeCode, ppmv.LocalCurrencyUid, p.BaseCurrencyUid, p.ExchangeRateSource,
            ppmv.ShareParQuantity * n.TransactionType,
            ppmv.OriginalFaceAmount * n.TransactionType,
            ppmv.LocalMarketValueAmount * n.TransactionType,
            ppmv.LocalNotionalRiskExposureAmount * n.TransactionType,
            CASE WHEN p.BaseCurrencyUid != 1 AND i.SecondaryInstrumentTypeCode = 'FWD'
                THEN ppmv.ShareParQuantity * COALESCE(ppmv.LocalPrice, 0) / i.InstrumentUnitParDivisorNumber
                ELSE
                    CASE WHEN ppmv.LocalCurrencyUid = p.BaseCurrencyUid AND i.SecondaryInstrumentTypeCode <> 'FWD'
                        THEN ppmv.LocalMarketValueAmount
                        ELSE ppmv.USDMarketValueAmount * IIF(p.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
                    END
            END * n.TransactionType AS BaseMarketValueAmount,
            CASE WHEN p.BaseCurrencyUid != 1 AND i.SecondaryInstrumentTypeCode = 'FWD'
                THEN ppmv.ShareParQuantity * COALESCE(ppmv.LocalPrice, 0) / i.InstrumentUnitParDivisorNumber
                ELSE
                    CASE WHEN ppmv.LocalCurrencyUid = p.BaseCurrencyUid AND i.SecondaryInstrumentTypeCode <> 'FWD'
                        THEN COALESCE(ppmv.LocalNotionalRiskExposureAmount, 0)
                        ELSE COALESCE(ppmv.USDNotionalRiskExposureAmount, 0) * IIF(p.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
                    END
            END * n.TransactionType AS BaseNotionalRiskExposureAmount,
            APP_NAME() AS CreatedByProgramName, APP_NAME() AS UpdatedByProgramName
      FROM
            #txNew AS n
            JOIN dbo.Portfolio AS p
                ON  p.PortfolioUid = n.PortfolioUid
                    AND
                    p.ResultsreportableIndicator = 'Y'
            JOIN dbo.PortfolioPositionMarketValue AS ppmv WITH (FORCESEEK)
                ON  ppmv.PositionDate = n.PositionDate
                    AND
                    ppmv.PortfolioUId = n.MemberPortfolioUid
                    AND
                    ppmv.PositionTypeCode = 'P'
                    AND
                    ppmv.ShareParQuantity <> 0
            JOIN dbo.AllListing AS l
                ON  l.ListingUid = ppmv.ListingUid
            JOIN dbo.instrument AS i
                ON  i.InstrumentUid = l.InstrumentUid
            LEFT JOIN #exrate AS er
                ON  er.FromCurrencyID = 1
                    AND
                    er.ToCurrencyID = p.BaseCurrencyUid
                    AND
                    er.[Source] = p.ExchangeRateSource
                    AND
                    er.[Date] = n.PositionDate
    OPTION (RECOMPILE)

    -- trace ---------------------------------------------------------------
    SET @traceRowCount = @@ROWCOUNT
    SET @traceTimeEnd = GETDATE();
    SET @generatedCount = @generatedCount + @traceRowCount
    IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
        SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
    ELSE
        SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': ' + CAST(@traceRowCount AS VARCHAR) + ' row(s) affected (' + @traceMessage
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
    ------------------------------------------------------------------------

    --=================================================================================
    -- Validate AM membership changes
    --=================================================================================
    IF @generatedCount > 0
    BEGIN
        -- trace ---------------------------------------------------------------
        SET @traceTimeStart = GETDATE();
        SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': Validating AM membership changes...'
        RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
        ------------------------------------------------------------------------

        DECLARE @validationError TABLE
        (
            PortfolioUid                INT NULL,
            MemberPortfolioUid          INT NULL,
            TransactionType             INT NULL,
            ListingUid                  INT NULL,
            IsLong                      BIT NULL,
            PositionTypeCode            CHAR(1) NULL,
            PositionDate                DATE NULL,
            newMembershipDate           DATE NULL,
            newPortfolioAmMembershipUid INT NULL,
            newPostDate                 DATE NULL,
            newPositionDate             DATE NULL,
            paramCurrentDate            DATE NULL,
            paramMaxProcessDate         DATE NULL
        )

        DECLARE @invalidCount INT = 0

        INSERT  @validationError
        SELECT  DISTINCT
                sfp.PortfolioUid, sfp.MemberPortfolioUid, sfp.TransactionType, sfp.ListingUid, sfp.IsLong, sfp.PositionTypeCode, sfp.PositionDate,
                MIN(n.MembershipDate) AS newMembershipDate, MIN(n.PortfolioAmMembershipUid) AS newPortfolioAmMembershipUid,
                MIN(n.PostDate) AS newPostDate, MIN(n.PositionDate) AS newPositionDate,
                @processDate AS paramCurrentDate, @maxProcessDate AS paramMaxProcessDate
          FROM  #txNew AS n
                JOIN dbo.SyntheticFlowPosition AS sfp
                    ON  sfp.CancelationProcessDate = '9999-12-31'
                        AND sfp.PortfolioUid = n.PortfolioUid
                        AND sfp.MemberPortfolioUid = n.MemberPortfolioUid
                        AND sfp.TransactionType = n.TransactionType
                        AND sfp.PositionDate = n.PositionDate
        GROUP BY
                sfp.PortfolioUid, sfp.MemberPortfolioUid, sfp.TransactionType, sfp.ListingUid, sfp.IsLong, sfp.PositionTypeCode, sfp.PositionDate
        HAVING  COUNT(*) > 1

        IF (@@ROWCOUNT > 0)
        BEGIN
            SET @errorMessage = N'SyntheticFlowProcess' + @sfName + N': Unexpected active (non-canceled) membership(s) found in SyntheticFlow' + @sfName + N' table. A possible cause is inconsistent data in AM membership tables.';

            DECLARE @xml XML = (SELECT TOP 10 * FROM @validationError AS ValidationInfo FOR XML AUTO)
            SET @errorMessage = @errorMessage + N' ' + NCHAR(13) + NCHAR(10) + CAST(@xml AS NVARCHAR(MAX))

            RAISERROR (@errorMessage, 0, 1) WITH NOWAIT

            UPDATE  dbo.SyntheticFlowPosition
               SET  CancelationProcessDate = @processDate,
                    UpdatedByName = SYSTEM_USER,
                    UpdatedTimestamp = SYSUTCDATETIME(),
                    UpdatedByProgramName = APP_NAME()
              FROM
                    @validationError AS ve
                    JOIN dbo.SyntheticFlowPosition AS sfp WITH (FORCESEEK)
                        ON  sfp.CancelationProcessDate = '9999-12-31'
                            AND sfp.ProcessDate < @processDate
                            AND sfp.PortfolioUid = ve.PortfolioUid
                            AND sfp.MemberPortfolioUid = ve.MemberPortfolioUid
                            AND sfp.TransactionType = ve.TransactionType
                            AND sfp.PositionDate = ve.PositionDate
                            AND sfp.ListingUid = ve.ListingUid
                            AND sfp.IsLong = ve.IsLong
                            AND sfp.PositionTypeCode = ve.PositionTypeCode
            OPTION (RECOMPILE)

            SET @traceRowCount = @@ROWCOUNT
        END
        ELSE
            SET @traceRowCount = @@ROWCOUNT

        -- trace ---------------------------------------------------------------
        SET @traceTimeEnd = GETDATE();
        IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
            SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + N's)'
        ELSE
            SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + N'ms)'
        SET @traceMessage = N'SyntheticFlowProcess' + @sfName + N': ' + IIF(@traceRowCount = 0, N'no discrepancies found', CAST(@traceRowCount AS VARCHAR) + ' previous active transaction(s) cancelled') + ' (' + @traceMessage
        RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
        ------------------------------------------------------------------------
    END

    --=================================================================================
    -- As-of position change - cancel
    --=================================================================================
    -- trace ---------------------------------------------------------------
    SET @traceTimeStart = GETDATE();
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': As-of position change - canceling affected transactions...'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------

    CREATE TABLE #canceled
    (
        PortfolioUid                    INT        NOT NULL,
        MembershipDate                  DATE       NOT NULL,
        TransactionType                 INT        NOT NULL,
        PortfolioAmMembershipUid        INT        NOT NULL,
        PostDate                        DATE       NOT NULL,
        PortfolioPositionMarketValueUid INT        NOT NULL,
        BaseCurrencyUid                 INT        NOT NULL,
        ExchangeRateSource              NCHAR(2)   NOT NULL
    )

    UPDATE  dbo.SyntheticFlowPosition
       SET  CancelationProcessDate = @processDate,
            UpdatedByName = SYSTEM_USER,
            UpdatedTimestamp = SYSUTCDATETIME(),
            UpdatedByProgramName = APP_NAME()
     OUTPUT
            DELETED.PortfolioUid, DELETED.MembershipDate, DELETED.TransactionType, DELETED.PortfolioAmMembershipUid, DELETED.PostDate,
            aud.PortfolioPositionMarketValueUid, DELETED.BaseCurrencyUid, DELETED.ExchangeRateSource
      INTO
            #canceled
      FROM
            dbo.SyntheticFlowPosition AS sfp  WITH (FORCESEEK)
            JOIN dbo.AsOfPortfolioPositionDailyAudit AS aud
                ON  sfp.CancelationProcessDate = '9999-12-31'
                    AND sfp.ProcessDate < @processDate
                    AND sfp.PositionDate >= @maxProcessDate
                    AND aud.PositionDate = sfp.PositionDate
                    AND aud.PortfolioUid = sfp.MemberPortfolioUid
                    AND aud.ListingUid = sfp.ListingUid
                    AND aud.IsLong = sfp.IsLong
                    AND aud.PositionTypeCode = sfp.PositionTypeCode
        OPTION (RECOMPILE)

    -- trace ---------------------------------------------------------------
    SET @traceRowCount = @@ROWCOUNT
    SET @traceTimeEnd = GETDATE();
    SET @canceledCount = @canceledCount + @traceRowCount
    IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
        SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
    ELSE
        SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': ' + CAST(@traceRowCount AS VARCHAR) + ' row(s) affected (' + @traceMessage
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
    ------------------------------------------------------------------------

    --=================================================================================
    -- As-of position change - reinstate
    --=================================================================================
    -- trace ---------------------------------------------------------------
    SET @traceTimeStart = GETDATE();
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': As-of position change - reinstating cancelled transactions...'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------

    INSERT dbo.SyntheticFlowPosition
    (
            ProcessDate, PortfolioUid, MemberPortfolioUid, MembershipDate, TransactionType, PortfolioAmMembershipUid,   PostDate,
            PositionDate, ListingUid, IsLong, PositionTypeCode, LocalCurrencyUid, BaseCurrencyUid, ExchangeRateSource,
            ShareParQuantity, OriginalFaceAmount, LocalMarketValueAmount, LocalNotionalRiskExposureAmount,
            BaseMarketValueAmount,
            BaseNotionalRiskExposureAmount,
            CreatedByProgramName, UpdatedByProgramName
    )
    SELECT  @processDate AS ProcessDate, c.PortfolioUid, ppmv.PortfolioUid, c.MembershipDate, c.TransactionType, c.PortfolioAmMembershipUid, c.PostDate,
            ppmv.PositionDate, ppmv.ListingUid, ppmv.IsLong, ppmv.PositionTypeCode, ppmv.LocalCurrencyUid, c.BaseCurrencyUid, c.ExchangeRateSource,
            ppmv.ShareParQuantity * c.TransactionType,
            ppmv.OriginalFaceAmount * c.TransactionType,
            ppmv.LocalMarketValueAmount * c.TransactionType,
            ppmv.LocalNotionalRiskExposureAmount * c.TransactionType,
            CASE WHEN c.BaseCurrencyUid != 1 AND i.SecondaryInstrumentTypeCode = 'FWD'
                THEN ppmv.ShareParQuantity * COALESCE(ppmv.LocalPrice, 0) / i.InstrumentUnitParDivisorNumber
                ELSE
                    CASE WHEN ppmv.LocalCurrencyUid = c.BaseCurrencyUid AND i.SecondaryInstrumentTypeCode <> 'FWD'
                        THEN ppmv.LocalMarketValueAmount
                        ELSE ppmv.USDMarketValueAmount * IIF(c.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
                    END
            END * c.TransactionType AS BaseMarketValueAmount,
            CASE WHEN c.BaseCurrencyUid != 1 AND i.SecondaryInstrumentTypeCode = 'FWD'
                THEN ppmv.ShareParQuantity * COALESCE(ppmv.LocalPrice, 0) / i.InstrumentUnitParDivisorNumber
                ELSE
                    CASE WHEN ppmv.LocalCurrencyUid = c.BaseCurrencyUid AND i.SecondaryInstrumentTypeCode <> 'FWD'
                        THEN COALESCE(ppmv.LocalNotionalRiskExposureAmount, 0)
                        ELSE COALESCE(ppmv.USDNotionalRiskExposureAmount, 0) * IIF(c.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
                    END
            END * c.TransactionType AS BaseNotionalRiskExposureAmount,
            APP_NAME() AS CreatedByProgramName, APP_NAME() AS UpdatedByProgramName
      FROM
            #canceled AS c
            JOIN dbo.PortfolioPositionMarketValue AS ppmv WITH (FORCESEEK)
                ON  ppmv.PortfolioPositionMarketValueUid = c.PortfolioPositionMarketValueUid
                    AND
                    ppmv.ShareParQuantity <> 0
            JOIN dbo.AllListing AS l
                ON  l.ListingUid = ppmv.ListingUid
            JOIN dbo.instrument AS i
                ON  i.InstrumentUid = l.InstrumentUid
            LEFT JOIN #exrate AS er
                ON  er.FromCurrencyID = 1
                    AND
                    er.ToCurrencyID = c.BaseCurrencyUid
                    AND
                    er.[Source] = c.ExchangeRateSource
                    AND
                    er.[Date] = ppmv.PositionDate
    OPTION (RECOMPILE)

    -- trace ---------------------------------------------------------------
    SET @traceRowCount = @@ROWCOUNT
    SET @traceTimeEnd = GETDATE();
    SET @generatedCount = @generatedCount + @traceRowCount
    IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
        SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
    ELSE
        SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': ' + CAST(@traceRowCount AS VARCHAR) + ' row(s) affected (' + @traceMessage
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
    ------------------------------------------------------------------------

    --=================================================================================
    -- New position added prior to the start/end date of a portfolio
    --=================================================================================
    -- trace ---------------------------------------------------------------
    SET @traceTimeStart = GETDATE();
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': New position prior to start/end date - generating new transactions...'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------

    SELECT  DISTINCT aud.PortfolioUid AS MemberPortfolioUid, aud.PositionDate, dbo.NextBusinessDate(aud.PositionDate) AS NextPositionDate,
                     aud.PortfolioPositionMarketValueUid, aud.ListingUid, aud.IsLong, aud.PositionTypeCode
      INTO  #newPosition
      FROM  dbo.AsOfPortfolioPositionDailyAudit AS aud
            JOIN dbo.PortfolioPositionMarketValue AS ppmv
                ON  ppmv.PortfolioPositionMarketValueUid = aud.PortfolioPositionMarketValueUid
                    AND aud.PositionDate >= @maxProcessDate
                    AND aud.PositionDate < @processDate
                    AND ppmv.ShareParQuantity <> 0
                    AND aud.ShareParQuantity = 0
    OPTION (RECOMPILE)

    IF EXISTS (SELECT * FROM #newPosition)
    BEGIN
        SELECT  pam.PortfolioAmMembershipUid, pam.PortfolioUid, pam.MemberPortfolioUid,
                CASE WHEN tt.TransactionType = 1 THEN pam.StartDate ELSE pam.EndDate END AS MembershipDate,
                tt.TransactionType, np.PositionDate,
                CASE WHEN tt.TransactionType = 1 THEN pam.StartDate ELSE dbo.NextBusinessDate(pam.EndDate) END AS PostDate,
                p.BaseCurrencyUid, p.ExchangeRateSource,
                np.PortfolioPositionMarketValueUid
          INTO  #newFlowPosition
          FROM
                #newPosition AS np
                JOIN dbo.portfoliOAMMembership AS pam
                    ON  pam.MemberPortfolioUid = np.MemberPortfolioUid
                        AND (pam.StartDate = np.NextPositionDate OR pam.EndDate = np.PositionDate)
                JOIN dbo.Portfolio AS p
                    ON  pam.PortfolioDateTypeCode = 'R'
                        AND p.ResultsReportableIndicator = 'Y'
                        AND p.PortfolioUid = pam.PortfolioUid
                        AND pam.ProcessDate < @processDate
                JOIN (VALUES(1),(-1)) AS tt(TransactionType)
                        ON (tt.TransactionType = 1 AND pam.StartDate = np.NextPositionDate)
                           OR (tt.TransactionType = -1 AND pam.EndDate = np.PositionDate)
         WHERE
                NOT EXISTS (SELECT *
                              FROM dbo.SyntheticFlowPosition AS sfp WITH (FORCESEEK)
                             WHERE sfp.CancelationProcessDate = '9999-12-31'
                                   AND sfp.PositionDate = np.PositionDate
                                   AND sfp.PortfolioUid = pam.PortfolioUid
                                   AND sfp.MemberPortfolioUid = pam.MemberPortfolioUid
                                   AND sfp.ListingUid = np.ListingUid
                                   AND sfp.IsLong = np.IsLong
                                   AND sfp.PositionTypeCode = np.PositionTypeCode)

            DECLARE
                @minRangeDate  DATE,
                @maxRangeDate  DATE,
                @exrateMinDate DATE,
                @exrateMaxDate DATE

            SELECT @minRangeDate = dbo.PreviousBusinessDate(MIN(PositionDate)), @maxRangeDate = MAX(PositionDate) FROM #newFlowPosition
            SELECT @exrateMinDate = MIN([Date]), @exrateMaxDate = MAX([Date]) FROM #exrate

            SELECT  TOP 0 *
              INTO  #exratePosition
              FROM  #exrate

            IF (@minRangeDate >= @exrateMinDate AND @maxRangeDate <= @exrateMaxDate)
            BEGIN
                INSERT  #exratePosition
                SELECT  *
                  FROM  #exrate
                 WHERE  [Date] BETWEEN @minRangeDate AND @maxRangeDate
            END
            ELSE
            BEGIN
                INSERT  #exratePosition
                  EXEC  dbo.GetExchangeRateTable @minRangeDate, @maxRangeDate WITH RECOMPILE
            END

        INSERT dbo.SyntheticFlowPosition
        (
                ProcessDate, PortfolioUid, MemberPortfolioUid, MembershipDate, TransactionType, PortfolioAmMembershipUid,   PostDate,
                PositionDate, ListingUid, IsLong, PositionTypeCode, LocalCurrencyUid, BaseCurrencyUid, ExchangeRateSource,
                ShareParQuantity, OriginalFaceAmount, LocalMarketValueAmount, LocalNotionalRiskExposureAmount,
                BaseMarketValueAmount,
                BaseNotionalRiskExposureAmount,
                CreatedByProgramName, UpdatedByProgramName
        )
        SELECT  @processDate AS ProcessDate, nfp.PortfolioUid, nfp.MemberPortfolioUid, nfp.MembershipDate, nfp.TransactionType, nfp.PortfolioAmMembershipUid, nfp.PostDate,
                nfp.PositionDate, ppmv.ListingUid, ppmv.IsLong, ppmv.PositionTypeCode, ppmv.LocalCurrencyUid, nfp.BaseCurrencyUid, nfp.ExchangeRateSource,
                ppmv.ShareParQuantity * nfp.TransactionType,
                ppmv.OriginalFaceAmount * nfp.TransactionType,
                ppmv.LocalMarketValueAmount * nfp.TransactionType,
                ppmv.LocalNotionalRiskExposureAmount * nfp.TransactionType,
                CASE WHEN nfp.BaseCurrencyUid != 1 AND i.SecondaryInstrumentTypeCode = 'FWD'
                    THEN ppmv.ShareParQuantity * COALESCE(ppmv.LocalPrice, 0) / i.InstrumentUnitParDivisorNumber
                    ELSE
                        CASE WHEN ppmv.LocalCurrencyUid = nfp.BaseCurrencyUid AND i.SecondaryInstrumentTypeCode <> 'FWD'
                            THEN ppmv.LocalMarketValueAmount
                            ELSE ppmv.USDMarketValueAmount * IIF(nfp.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
                        END
                END * nfp.TransactionType AS BaseMarketValueAmount,
                CASE WHEN nfp.BaseCurrencyUid != 1 AND i.SecondaryInstrumentTypeCode = 'FWD'
                    THEN ppmv.ShareParQuantity * COALESCE(ppmv.LocalPrice, 0) / i.InstrumentUnitParDivisorNumber
                    ELSE
                        CASE WHEN ppmv.LocalCurrencyUid = nfp.BaseCurrencyUid AND i.SecondaryInstrumentTypeCode <> 'FWD'
                            THEN COALESCE(ppmv.LocalNotionalRiskExposureAmount, 0)
                            ELSE COALESCE(ppmv.USDNotionalRiskExposureAmount, 0) * IIF(nfp.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
                        END
                END * nfp.TransactionType AS BaseNotionalRiskExposureAmount,
                APP_NAME() AS CreatedByProgramName, APP_NAME() AS UpdatedByProgramName
          FROM
                #newFlowPosition AS nfp
                JOIN dbo.PortfolioPositionMarketValue AS ppmv WITH (FORCESEEK)
                    ON  ppmv.PortfolioPositionMarketValueUid = nfp.PortfolioPositionMarketValueUid
                JOIN dbo.AllListing AS l
                    ON  l.ListingUid = ppmv.ListingUid
                JOIN dbo.instrument AS i
                    ON  i.InstrumentUid = l.InstrumentUid
                LEFT JOIN #exratePosition AS er
                    ON  er.FromCurrencyID = 1
                        AND
                        er.ToCurrencyID = nfp.BaseCurrencyUid
                        AND
                        er.[Source] = nfp.ExchangeRateSource
                        AND
                        er.[Date] = nfp.PositionDate
        OPTION (RECOMPILE)
    END

    -- trace ---------------------------------------------------------------
    SET @traceRowCount = @@ROWCOUNT
    SET @traceTimeEnd = GETDATE();
    SET @generatedCount = @generatedCount + @traceRowCount
    IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
        SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
    ELSE
        SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': ' + CAST(@traceRowCount AS VARCHAR) + ' row(s) affected (' + @traceMessage
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
    ------------------------------------------------------------------------
END
GO
