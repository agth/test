SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, QUOTED_IDENTIFIER, CONCAT_NULL_YIELDS_NULL, ARITHABORT ON
GO
SET NUMERIC_ROUNDABORT OFF
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'SyntheticFlowProcessAccrual')
   EXEC('CREATE PROCEDURE dbo.SyntheticFlowProcessAccrual AS SELECT 1;')
GO

ALTER PROCEDURE dbo.SyntheticFlowProcessAccrual
    @processDate    DATE,
    @maxProcessDate DATE,
    @canceledCount  INT OUTPUT,
    @generatedCount INT OUTPUT
AS
BEGIN
    DECLARE
        @sfName NVARCHAR(50) = N'Accrual',
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
    UPDATE  dbo.SyntheticFlowAccrual
       SET  CancelationProcessDate = CASE WHEN n.MembershipDate BETWEEN @maxProcessDate AND @processDate OR sfa.PostDate BETWEEN @maxProcessDate AND @processDate
                                        THEN @processDate
                                        ELSE '0001-01-01' -- do not publish
                                     END,
            UpdatedByName = SYSTEM_USER,
            UpdatedTimestamp = SYSUTCDATETIME(),
            UpdatedByProgramName = APP_NAME()
      FROM
            #txNew AS n
            JOIN dbo.SyntheticFlowAccrual AS sfa WITH (FORCESEEK)
                ON  sfa.CancelationProcessDate = '9999-12-31'
                    AND sfa.ProcessDate = @processDate
                    AND sfa.PortfolioUid = n.PortfolioUid
                    AND sfa.MemberPortfolioUid = n.MemberPortfolioUid
                    AND sfa.TransactionType = n.TransactionType
                    AND sfa.MembershipDate = n.MembershipDate
    OPTION (RECOMPILE)

    -- trace ---------------------------------------------------------------
    SET @traceRowCount = @@ROWCOUNT
    ------------------------------------------------------------------------

    UPDATE  dbo.SyntheticFlowAccrual
       SET  CancelationProcessDate = CASE WHEN c.OldMembershipDate BETWEEN @maxProcessDate AND @processDate OR sfa.PostDate BETWEEN @maxProcessDate AND @processDate
                                        THEN @processDate
                                        ELSE '0001-01-01' -- do not publish
                                     END,
            UpdatedByName = SYSTEM_USER,
            UpdatedTimestamp = SYSUTCDATETIME(),
            UpdatedByProgramName = APP_NAME()
      FROM
            #txChange AS c
            JOIN dbo.SyntheticFlowAccrual AS sfa WITH (FORCESEEK)
                ON  sfa.CancelationProcessDate = '9999-12-31'
                    AND sfa.ProcessDate <= @processDate  -- inlude a same-day previous run     
                    AND sfa.PortfolioUid = c.PortfolioUid
                    AND sfa.MemberPortfolioUid = c.MemberPortfolioUid
                    AND sfa.TransactionType = c.TransactionType
                    AND sfa.MembershipDate = c.OldMembershipDate
    OPTION (RECOMPILE)

    -- trace ---------------------------------------------------------------
    SET @traceRowCount = @traceRowCount + @@ROWCOUNT
    ------------------------------------------------------------------------

    UPDATE  dbo.SyntheticFlowAccrual
       SET  CancelationProcessDate = CASE WHEN sfa.MembershipDate BETWEEN @maxProcessDate AND @processDate OR sfa.PostDate BETWEEN @maxProcessDate AND @processDate
                                        THEN @processDate
                                        ELSE '0001-01-01' -- do not publish
                                     END,
            UpdatedByName = SYSTEM_USER,
            UpdatedTimestamp = SYSUTCDATETIME(),
            UpdatedByProgramName = APP_NAME()
      FROM
            #txDelete AS d
            JOIN dbo.SyntheticFlowAccrual AS sfa WITH (FORCESEEK)
                ON  sfa.CancelationProcessDate = '9999-12-31'
                    AND sfa.PortfolioAmMembershipUid = d.PortfolioAmMembershipUid
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

    INSERT dbo.SyntheticFlowAccrual
    (
            ProcessDate, PortfolioUid, MemberPortfolioUid, MembershipDate, TransactionType, PortfolioAmMembershipUid, PostDate,
            PositionDate, ListingUid, AccrualTypeCode,  LocalCurrencyUid, BaseCurrencyUid, ExchangeRateSource,
            LocalAccrualAmount, LocalReclaimAmount,
            BaseAccrualAmount,
            BaseReclaimAmount,
            CreatedByProgramName, UpdatedByProgramName
    )
    SELECT  @processDate AS ProcessDate, n.PortfolioUid, n.MemberPortfolioUid, n.MembershipDate, n.TransactionType, n.PortfolioAmMembershipUid, n.PostDate,
            pamv.PositionDate, pamv.ListingUid, pamv.AccrualTypeCode, pamv.LocalCurrencyUid, p.BaseCurrencyUid, p.ExchangeRateSource,
            pamv.LocalAccrualAmount * n.TransactionType,
            pamv.LocalReclaimAmount * n.TransactionType,
            CASE WHEN pamv.LocalCurrencyUid = p.BaseCurrencyUid
                THEN pamv.LocalAccrualAmount
                ELSE pamv.USDAccrualAmount * IIF(p.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
            END * n.TransactionType AS BaseAccrualAmount,
            CASE WHEN pamv.LocalCurrencyUid = p.BaseCurrencyUid
                THEN pamv.LocalReclaimAmount
                ELSE pamv.USDReclaimAmount * IIF(p.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
            END * n.TransactionType AS BaseReclaimAmount,
            APP_NAME() AS CreatedByProgramName, APP_NAME() AS UpdatedByProgramName
      FROM
            #txNew AS n
            JOIN dbo.Portfolio AS p
                ON  p.PortfolioUid = n.PortfolioUid
                    AND
                    p.ResultsreportableIndicator = 'Y'
            JOIN dbo.PortfolioAccrualMarketValue AS pamv WITH (FORCESEEK)
                ON  pamv.PositionDate = n.PositionDate
                    AND
                    pamv.PortfolioUId = n.MemberPortfolioUid
                    AND
                    (pamv.LocalAccrualAmount <> 0 OR pamv.LocalReclaimAmount <> 0)
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
            AccrualTypeCode             VARCHAR(4) NULL,
            LocalCurrencyUid            INT NULL,
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
                sfa.PortfolioUid, sfa.MemberPortfolioUid, sfa.TransactionType, sfa.ListingUid, sfa.AccrualTypeCode, sfa.LocalCurrencyUid, sfa.PositionDate,
                MIN(n.MembershipDate) AS newMembershipDate, MIN(n.PortfolioAmMembershipUid) AS newPortfolioAmMembershipUid,
                MIN(n.PostDate) AS newPostDate, MIN(n.PositionDate) AS newPositionDate,
                @processDate AS paramCurrentDate, @maxProcessDate AS paramMaxProcessDate
          FROM  #txNew AS n
                JOIN dbo.SyntheticFlowAccrual AS sfa
                    ON  sfa.CancelationProcessDate = '9999-12-31'
                        AND sfa.PortfolioUid = n.PortfolioUid
                        AND sfa.MemberPortfolioUid = n.MemberPortfolioUid
                        AND sfa.TransactionType = n.TransactionType
                        AND sfa.PositionDate = n.PositionDate
        GROUP BY
                sfa.PortfolioUid, sfa.MemberPortfolioUid, sfa.TransactionType, sfa.ListingUid, sfa.AccrualTypeCode, sfa.LocalCurrencyUid, sfa.PositionDate
        HAVING  COUNT(*) > 1

        IF (@@ROWCOUNT > 0)
        BEGIN
            SET @errorMessage = N'SyntheticFlowProcess' + @sfName + N': Unexpected active (non-canceled) membership(s) found in SyntheticFlow' + @sfName + N' table. A possible cause is inconsistent data in AM membership tables.';

            DECLARE @xml XML = (SELECT TOP 10 * FROM @validationError AS ValidationInfo FOR XML AUTO)
            SET @errorMessage = @errorMessage + N' ' + NCHAR(13) + NCHAR(10) + CAST(@xml AS NVARCHAR(MAX))

            RAISERROR (@errorMessage, 0, 1) WITH NOWAIT

            UPDATE  dbo.SyntheticFlowAccrual
               SET  CancelationProcessDate = @processDate,
                    UpdatedByName = SYSTEM_USER,
                    UpdatedTimestamp = SYSUTCDATETIME(),
                    UpdatedByProgramName = APP_NAME()
              FROM
                    @validationError AS ve
                    JOIN dbo.SyntheticFlowAccrual AS sfa WITH (FORCESEEK)
                        ON  sfa.CancelationProcessDate = '9999-12-31'
                            AND sfa.ProcessDate < @processDate
                            AND sfa.PortfolioUid = ve.PortfolioUid
                            AND sfa.MemberPortfolioUid = ve.MemberPortfolioUid
                            AND sfa.TransactionType = ve.TransactionType
                            AND sfa.PositionDate = ve.PositionDate
                            AND sfa.ListingUid = ve.ListingUid
                            AND sfa.AccrualTypeCode = ve.AccrualTypeCode
                            AND sfa.LocalCurrencyUid = ve.LocalCurrencyUid
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
    -- As-of accrual change - cancel
    --=================================================================================
    -- trace ---------------------------------------------------------------
    SET @traceTimeStart = GETDATE();
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': As-of accrual change - canceling affected transactions...'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------

    CREATE TABLE #canceled
    (
        PortfolioUid                    INT      NOT NULL,
        MemberPortfolioUid              INT      NOT NULL,
        MembershipDate                  DATE     NOT NULL,
        TransactionType                 INT      NOT NULL,
        PortfolioAmMembershipUid        INT      NOT NULL,
        PostDate                        DATE     NOT NULL,
        PortfolioAccrualMarketValueUid  INT      NOT NULL,
        BaseCurrencyUid                 INT      NOT NULL,
        ExchangeRateSource              NCHAR(2) NOT NULL
    )

    UPDATE  dbo.SyntheticFlowAccrual
       SET  CancelationProcessDate = @processDate,
            UpdatedByName = SYSTEM_USER,
            UpdatedTimestamp = SYSUTCDATETIME(),
            UpdatedByProgramName = APP_NAME()
     OUTPUT
            DELETED.PortfolioUid, DELETED.MemberPortfolioUid, DELETED.MembershipDate, DELETED.TransactionType, DELETED.PortfolioAmMembershipUid, DELETED.PostDate,
            aud.PortfolioAccrualMarketValueUid, DELETED.BaseCurrencyUid, DELETED.ExchangeRateSource
      INTO
            #canceled
      FROM
            dbo.SyntheticFlowAccrual AS sfa WITH (FORCESEEK)
            JOIN dbo.AsOfPortfolioAccrualDailyAudit AS aud
                ON  sfa.CancelationProcessDate = '9999-12-31'
                    AND sfa.ProcessDate < @processDate
                    AND sfa.PositionDate >= @maxProcessDate
                    AND aud.PositionDate = sfa.PositionDate
                    AND aud.PortfolioUid = sfa.MemberPortfolioUid
                    AND aud.ListingUid = sfa.ListingUid
                    AND aud.AccrualTypeCode = sfa.AccrualTypeCode
                    AND aud.LocalCurrencyUid = sfa.LocalCurrencyUid
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
    -- As-of accrual change - reinstate
    --=================================================================================
    -- trace ---------------------------------------------------------------
    SET @traceTimeStart = GETDATE();
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': As-of accrual change - reinstating cancelled transactions...'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------

    INSERT dbo.SyntheticFlowAccrual
    (
            ProcessDate, PortfolioUid, MemberPortfolioUid, MembershipDate, TransactionType, PortfolioAmMembershipUid, PostDate,
            PositionDate, ListingUid, AccrualTypeCode,  LocalCurrencyUid, BaseCurrencyUid, ExchangeRateSource,
            LocalAccrualAmount, LocalReclaimAmount,
            BaseAccrualAmount,
            BaseReclaimAmount,
            CreatedByProgramName, UpdatedByProgramName
    )
    SELECT  @processDate AS ProcessDate, c.PortfolioUid, c.MemberPortfolioUid, c.MembershipDate, c.TransactionType, c.PortfolioAmMembershipUid, c.PostDate,
            pamv.PositionDate, pamv.ListingUid, pamv.AccrualTypeCode, pamv.LocalCurrencyUid, c.BaseCurrencyUid, c.ExchangeRateSource,
            pamv.LocalAccrualAmount * c.TransactionType,
            pamv.LocalReclaimAmount * c.TransactionType,
            CASE WHEN pamv.LocalCurrencyUid = c.BaseCurrencyUid
                THEN pamv.LocalAccrualAmount
                ELSE pamv.USDAccrualAmount * IIF(c.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
            END * c.TransactionType AS BaseAccrualAmount,
            CASE WHEN pamv.LocalCurrencyUid = c.BaseCurrencyUid
                THEN pamv.LocalReclaimAmount
                ELSE pamv.USDReclaimAmount * IIF(c.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
            END * c.TransactionType AS BaseReclaimAmount,
            APP_NAME() AS CreatedByProgramName, APP_NAME() AS UpdatedByProgramName
      FROM
            #canceled AS c
            JOIN dbo.PortfolioAccrualMarketValue AS pamv WITH (FORCESEEK)
                ON  pamv.PortfolioAccrualMarketValueUid = c.PortfolioAccrualMarketValueUid
                    AND
                    (pamv.LocalAccrualAmount <> 0 OR pamv.LocalReclaimAmount <> 0)
            LEFT JOIN #exrate AS er
                ON  er.FromCurrencyID = 1
                    AND
                    er.ToCurrencyID = c.BaseCurrencyUid
                    AND
                    er.[Source] = c.ExchangeRateSource
                    AND
                    er.[Date] = pamv.PositionDate
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
    -- New accrual added prior to the start/end date of a portfolio
    --=================================================================================
    -- trace ---------------------------------------------------------------
    SET @traceTimeStart = GETDATE();
    SET @traceMessage = N'SyntheticFlowProcess' + @sfName +  N': New accrual prior to start/end date - generating new transactions...'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------

    SELECT  DISTINCT aud.PortfolioUid AS MemberPortfolioUid, aud.PositionDate, dbo.NextBusinessDate(aud.PositionDate) AS NextPositionDate,
                     pamv.PortfolioAccrualMarketValueUid, aud.ListingUid, aud.AccrualTypeCode, aud.LocalCurrencyUid
      INTO  #newAccrual
      FROM  dbo.AsOfPortfolioAccrualDailyAudit AS aud
            JOIN dbo.PortfolioAccrualMarketValue AS pamv
                ON  pamv.PortfolioAccrualMarketValueUid = aud.PortfolioAccrualMarketValueUid
                    AND aud.PositionDate >= @maxProcessDate
                    AND aud.PositionDate < @processDate
                    AND aud.LocalAccrualAmount = 0
                    AND aud.LocalReclaimAmount = 0
                    AND (pamv.LocalAccrualAmount <> 0 OR pamv.LocalReclaimAmount <> 0)
    OPTION (RECOMPILE)

    IF EXISTS (SELECT * FROM #newAccrual)
    BEGIN
        SELECT  pam.PortfolioAmMembershipUid, pam.PortfolioUid, pam.MemberPortfolioUid,
                CASE WHEN tt.TransactionType = 1 THEN pam.StartDate ELSE pam.EndDate END AS MembershipDate,
                tt.TransactionType, na.PositionDate,
                CASE WHEN tt.TransactionType = 1 THEN pam.StartDate ELSE dbo.NextBusinessDate(pam.EndDate) END AS PostDate,
                p.BaseCurrencyUid, p.ExchangeRateSource,
                na.PortfolioAccrualMarketValueUid
          INTO  #newFlowAccrual
          FROM
                #newAccrual AS na
                JOIN dbo.portfoliOAMMembership AS pam
                    ON  pam.MemberPortfolioUid = na.MemberPortfolioUid
                        AND (pam.StartDate = na.NextPositionDate OR pam.EndDate = na.PositionDate)
                JOIN dbo.Portfolio AS p
                    ON  pam.PortfolioDateTypeCode = 'R'
                        AND p.ResultsReportableIndicator = 'Y'
                        AND p.PortfolioUid = pam.PortfolioUid
                        AND pam.ProcessDate < @processDate
                JOIN (VALUES(1),(-1)) AS tt(TransactionType)
                        ON (tt.TransactionType = 1 AND pam.StartDate = na.NextPositionDate)
                           OR (tt.TransactionType = -1 AND pam.EndDate = na.PositionDate)
         WHERE
                NOT EXISTS (SELECT *
                              FROM dbo.SyntheticFlowAccrual AS sfa WITH (FORCESEEK)
                             WHERE sfa.CancelationProcessDate = '9999-12-31'
                                   AND sfa.PositionDate = na.PositionDate
                                   AND sfa.PortfolioUid = pam.PortfolioUid
                                   AND sfa.MemberPortfolioUid = pam.MemberPortfolioUid
                                   AND sfa.ListingUid = na.ListingUid
                                   AND sfa.AccrualTypeCode = na.AccrualTypeCode
                                   AND sfa.LocalCurrencyUid = na.LocalCurrencyUid)

        DECLARE
            @minRangeDate  DATE,
            @maxRangeDate  DATE,
            @exrateMinDate DATE,
            @exrateMaxDate DATE

        SELECT @minRangeDate = dbo.PreviousBusinessDate(MIN(PositionDate)), @maxRangeDate = MAX(PositionDate) FROM #newFlowAccrual
        SELECT @exrateMinDate = MIN([Date]), @exrateMaxDate = MAX([Date]) FROM #exrate

        SELECT  TOP 0 *
          INTO  #exrateAccrual
          FROM  #exrate

        IF (@minRangeDate >= @exrateMinDate AND @maxRangeDate <= @exrateMaxDate)
        BEGIN
            INSERT  #exrateAccrual
            SELECT  *
              FROM  #exrate
             WHERE  [Date] BETWEEN @minRangeDate AND @maxRangeDate
        END
        ELSE
        BEGIN
            INSERT  #exrateAccrual
              EXEC  dbo.GetExchangeRateTable @minRangeDate, @maxRangeDate WITH RECOMPILE
        END

        INSERT dbo.SyntheticFlowAccrual
        (
                ProcessDate, PortfolioUid, MemberPortfolioUid, MembershipDate, TransactionType, PortfolioAmMembershipUid, PostDate,
                PositionDate, ListingUid, AccrualTypeCode,  LocalCurrencyUid, BaseCurrencyUid, ExchangeRateSource,
                LocalAccrualAmount, LocalReclaimAmount,
                BaseAccrualAmount,
                BaseReclaimAmount,
                CreatedByProgramName, UpdatedByProgramName
        )
        SELECT  @processDate AS ProcessDate, nfa.PortfolioUid, nfa.MemberPortfolioUid, nfa.MembershipDate, nfa.TransactionType, nfa.PortfolioAmMembershipUid, nfa.PostDate,
                nfa.PositionDate, pamv.ListingUid, pamv.AccrualTypeCode, pamv.LocalCurrencyUid, nfa.BaseCurrencyUid, nfa.ExchangeRateSource,
                pamv.LocalAccrualAmount * nfa.TransactionType,
                pamv.LocalReclaimAmount * nfa.TransactionType,
                CASE WHEN pamv.LocalCurrencyUid = nfa.BaseCurrencyUid
                    THEN pamv.LocalAccrualAmount
                    ELSE pamv.USDAccrualAmount * IIF(nfa.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
                END * nfa.TransactionType AS BaseAccrualAmount,
                CASE WHEN pamv.LocalCurrencyUid = nfa.BaseCurrencyUid
                    THEN pamv.LocalReclaimAmount
                    ELSE pamv.USDReclaimAmount * IIF(nfa.BaseCurrencyUid = 1, 1, COALESCE(er.Rate, 1))
                END * nfa.TransactionType AS BaseReclaimAmount,
                APP_NAME() AS CreatedByProgramName, APP_NAME() AS UpdatedByProgramName
          FROM
                #newFlowAccrual AS nfa
                JOIN dbo.PortfolioAccrualMarketValue AS pamv WITH (FORCESEEK)
                    ON  pamv.PortfolioAccrualMarketValueUid = nfa.PortfolioAccrualMarketValueUid
                LEFT JOIN #exrateAccrual AS er
                    ON  er.FromCurrencyID = 1
                        AND
                        er.ToCurrencyID = nfa.BaseCurrencyUid
                        AND
                        er.[Source] = nfa.ExchangeRateSource
                        AND
                        er.[Date] = nfa.PositionDate
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
