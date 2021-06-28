SET ANSI_NULLS, ANSI_PADDING, ANSI_WARNINGS, QUOTED_IDENTIFIER, CONCAT_NULL_YIELDS_NULL, ARITHABORT ON
GO
SET NUMERIC_ROUNDABORT OFF
GO

IF NOT EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'SyntheticFlowProcess')
   EXEC('CREATE PROCEDURE dbo.SyntheticFlowProcess AS SELECT 1;')
GO

ALTER PROCEDURE dbo.SyntheticFlowProcess
    @processDate    DATE = NULL,
    @maxProcessDate DATE = NULL
AS
/*
[version 2]

S   start date              // PortfolioAmMembership.StartDate
E   end date                // PortfolioAmMembership.EndDate
Sp  previous start date     // PortfolioAmMembershipAudit.StartDate
Ep  previous end date       // PortfolioAmMembershipAudit.EndDate
C   current/process date    // ProcessDate.CurrentDate
M   max process date        // ProcessDate.MaxProcessDate
P() previous business day   // PreviousBusinessDate()
N() next business day       // NextBusinessDate()
R   result reportable flag  // Portfolio.ResultsreportableIndicator
D   position date           // Portfolio[Position|Accrual]MarketValue.PositionDate

Notes: CANCEL() - cancel (or invalidate if out of process_range) if exists
       Rules, if applied, are listed in precedence order (highest first)

1.  process_range - the process dates
1.1     if E > P(C) then CANCEL(Ep); do not process the end date
1.2     if E < M then CANCEL(Ep); do not process the portfolio
1.3     if S > C then CANCEL(Sp); do not process the start date
1.4     if S < M then CANCEL(Sp); set S = M

2.  reportable_flag - results reportable for the rollup portfolio (PortfolioAmMembership.PortfolioUid)
2.1     if R is changed to 'N' then cancel all members
2.2     if R is changed to 'Y' then generate synthetic flows for all members in process_range
2.3     if R = 'N' then do not process the portfolio
2.4     if R = 'Y' then generate synthetic flows

3.   deleted - AM is deleted from PortfolioAmMembership table
3.1     CANCEL(Sp); CANCEL(Ep)

4.   start_date - the membership start date
4.1     if changed then CANCEL(Sp); generate BUY for PostDate = S and PositionDate = P(S)
4.2     if uchanged and (S = C or a new AM is added) then generate BUY for PostDate = S and PositionDate = P(S)

5.   end_date - the membership end date
5.1     if changed then CANCEL(Ep); generate SELL for PostDate = N(E) and PositionDate = E
5.2     if uchanged and (E = P(C) or a new AM is added) then generate SELL for PostDate = N(E) and PositionDate = E

6.   position - as-of position change
6.1     if not falling under rules 1-5 then CANCEL(S) and CANCEL(E); if canceled then generate BUY or/and SELL for the same PositionDate

7.   position - new position added prior to the start/end date of a portfolio
7.2     if there is no an active transaction for N(D) = S then generate BUY for PostDate = N(D) and S = N(D)
7.2     if there is no an active transaction for D = E then generate SELL for PostDate = N(D) and E = D


Filters
    PortfolioAmMembership
        PortfolioDateTypeCode ='R'
        PortfolioTypeUid NOT IN AggregatorExcludedPortfolioType table

    PortfolioPositionMarketValue
        PositionTypeCode = 'P'
        ShareParQuantity <> 0

    PortfolioAccrualMarketValue
        LocalAccrualAmount <> 0 OR LocalReclaimAmount <> 0
*/
BEGIN
    SET NOCOUNT ON

    IF (@processDate IS NULL)
    BEGIN
        SELECT @processDate = CurrentDate FROM dbo.ProcessDate WHERE JobName = 'PositionAggregatorJobProcess'
        SET @maxProcessDate = dbo.GetMaxProcessDate()
    END
    ELSE IF (@maxProcessDate IS NULL)
        SET @maxProcessDate = DATEADD(yy, -2, @processDate)

    SET @maxProcessDate = dbo.CurrentOrPreviousBusinessDate(@maxProcessDate)

    -- trace ---------------------------------------------------------------
    DECLARE
        @traceTotalTimeStart DATETIME = GETDATE(),
        @traceTimeStart      DATETIME,
        @traceTimeEnd        DATETIME,
        @traceRowCount       INT,
        @traceCanceledCount  INT,
        @traceGeneratedCount INT,
        @traceMessage        NVARCHAR(500),
        @traceTotalCanceledCount  INT = 0,
        @traceTotalGeneratedCount INT = 0
    ------------------------------------------------------------------------

    IF (@processDate <> dbo.CurrentOrPreviousBusinessDate(@processDate))
    BEGIN
        SET @traceMessage = N'ERROR: The process date ' + CAST(@processDate AS NVARCHAR(10)) + N' is not a business day';
        THROW 55001, @traceMessage, 1;
    END

    DECLARE
        @skipAccrual  BIT = 0,
        @skipPosition BIT = 0
    --  @skipAccrual  BIT = 1,
    --  @skipPosition BIT = 1

    --IF NOT EXISTS (SELECT * FROM dbo.SyntheticFlowAccrual WITH (FORCESEEK) WHERE ProcessDate = @processDate OR CancelationProcessDate = @processDate)
    --    SET @skipAccrual = 0

    --IF NOT EXISTS (SELECT * FROM dbo.SyntheticFlowPosition WITH (FORCESEEK) WHERE ProcessDate = @processDate OR CancelationProcessDate = @processDate)
    --    SET @skipPosition = 0

    IF (@skipAccrual = 1 AND @skipPosition = 1)
    BEGIN
        SET @traceMessage = N'ERROR: The processing of Synthetic Flows was aborted because it has already been processed. The current process date is ' + CAST(@processDate AS NVARCHAR(10));
        THROW 55002, @traceMessage, 1;
    END

    -- trace ---------------------------------------------------------------
    SET @traceMessage = N'Stored Proc SyntheticFlowProcess ' + CAST(@processDate AS NVARCHAR(10)) + ' ' + CAST(@maxProcessDate AS NVARCHAR(10)) + N' - Started'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------

    DECLARE @internalTransaction INT = CASE WHEN @@TRANCOUNT = 0 THEN 1 ELSE 0 END

    DECLARE
        @membershipEndDate  DATE = dbo.PreviousBusinessDate(@processDate),
        @minRangeDate       DATE,
        @maxRangeDate       DATE

    BEGIN TRY
        --=================================================================================
        -- #bizdate - business dates
        --=================================================================================
        -- trace ---------------------------------------------------------------
        SET @traceTimeStart = GETDATE();
        ------------------------------------------------------------------------

        SET @minRangeDate = @maxProcessDate
        SET @maxRangeDate = @processDate

        SELECT  [date], dbo.NextBusinessDate([date]) AS nextDay, dbo.PreviousBusinessDate([date]) AS prevDay
          INTO  #bizdate
          FROM  dbo.RangeOfDates(@minRangeDate, @maxRangeDate)

        -- trace ---------------------------------------------------------------
        SET @traceRowCount = @@ROWCOUNT
        SET @traceTimeEnd = GETDATE();
        IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
            SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
        ELSE
            SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
        SET @traceMessage = 'SyntheticFlowProcess: Generated business dates between ' + CAST(@minRangeDate AS VARCHAR) + ' and ' + CAST(@maxRangeDate AS VARCHAR) + ' (' + CAST(@traceRowCount AS VARCHAR) + ' row' + IIF(@traceRowCount=1, '', 's') +  ' in ' + @traceMessage
        RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
        ------------------------------------------------------------------------

        --=================================================================================
        -- Changes in AM membership
        --=================================================================================
        -- trace ---------------------------------------------------------------
        SET @traceTimeStart = GETDATE();
        RAISERROR ('SyntheticFlowProcess: Getting AM membership changes...', 0, 1) WITH NOWAIT;
        ------------------------------------------------------------------------

        ;WITH tt AS
        (
            SELECT 1 AS TransactionType
            UNION ALL
            SELECT -1 AS TransactionType
        ),
        affectedPortfolios AS
        (
            -- Changes in start date for current business date
            SELECT  pam.PortfolioAmMembershipUid, pam.PortfolioUid, pam.MemberPortfolioUid,
                    CASE WHEN pam.StartDate > @processDate OR pam.EndDate < @maxProcessDate
                        THEN '9999-12-31'                                                   -- R1.3, R1.2
                        ELSE pam.StartDate
                    END AS NewMembershipDate,
                    aud.StartDate AS OldMembershipDate,
                    1 AS TransactionType
              FROM
                    dbo.portfoliOAMMembership AS pam
                    JOIN dbo.PortfolioAMMembershipAudit AS aud
                        ON  pam.PortfolioDateTypeCode = 'R'
                            AND
                            aud.ActionCode = 'U'
                            AND
                            pam.ProcessDate = @processDate
                            AND
                            aud.ProcessDate = pam.ProcessDate
                            AND
                            aud.PortfolioAMMembershipUid = pam.PortfolioAMMembershipUid
                            AND
                            aud.StartDate <> pam.StartDate
            -- Changes in end date for current business date
            UNION ALL
            SELECT  pam.PortfolioAmMembershipUid, pam.PortfolioUid, pam.MemberPortfolioUid,
                    CASE WHEN pam.EndDate > @membershipEndDate OR pam.EndDate < @maxProcessDate THEN '9999-12-31' ELSE pam.EndDate END AS NewMembershipDate,    -- R1.1, R1.2
                    aud.EndDate AS OldMembershipDate,
                    -1 AS TransactionType
              FROM
                    dbo.portfoliOAMMembership AS pam
                    JOIN dbo.PortfolioAMMembershipAudit AS aud
                        ON  pam.PortfolioDateTypeCode = 'R'
                            AND
                            aud.ActionCode = 'U'
                            AND
                            pam.ProcessDate = @processDate
                            AND
                            aud.ProcessDate = pam.ProcessDate
                            AND
                            aud.PortfolioAMMembershipUid = pam.PortfolioAMMembershipUid
                            AND
                            aud.EndDate <> pam.EndDate
            UNION ALL
            -- AM Deletes
            SELECT  aud.PortfolioAmMembershipUid, aud.PortfolioUid, aud.MemberPortfolioUid,
                    '9999-12-31' AS NewMembershipDate,                                                              -- R3.1
                    CASE WHEN tt.TransactionType = 1 THEN aud.StartDate ELSE aud.EndDate END AS OldMembershipDate,
                    tt.TransactionType
              FROM
                    dbo.PortfolioAmMembershipAudit AS aud,
                    tt
             WHERE
                    aud.PortfolioDateTypeCode = 'R'
                    AND
                    aud.ActionCode = 'D'
                    AND
                    aud.ProcessDate = @processDate
                    AND
                    (tt.TransactionType = 1 OR (tt.TransactionType = -1 AND aud.EndDate <> '9999-12-31' ))
                    AND
                    NOT EXISTS (SELECT  *
                                  FROM  dbo.PortfolioAmMembershipAudit AS upd
                                 WHERE  aud.PortfolioDateTypeCode = 'R'
                                        AND
                                        upd.ActionCode = 'U'
                                        AND
                                        upd.MemberPortfolioUid = aud.MemberPortfolioUid
                                        AND
                                        upd.ProcessDate = @processDate
                                        AND
                                        upd.PortfolioUid = aud.PortfolioUid)
        )
        SELECT  ap.PortfolioAmMembershipUid, ap.PortfolioUid, ap.MemberPortfolioUid,
                CASE WHEN p.ResultsReportableIndicator = 'N'            -- R2.3
                    THEN '9999-12-31'
                    ELSE ap.NewMembershipDate
                END AS NewMembershipDate,
                ap.OldMembershipDate,
                ap.TransactionType,
                CASE WHEN ap.TransactionType = 1 AND ap.NewMembershipDate < @maxProcessDate THEN @maxProcessDate ELSE ap.NewMembershipDate END AS ActivityDate  -- R1.4
          INTO
                #txChange
          FROM
                affectedPortfolios AS ap
                JOIN dbo.Portfolio AS p
                    ON  p.PortfolioUid = ap.PortfolioUid
                LEFT JOIN dbo.PortfolioAudit AS pa
                    ON  pa.PortfolioUid = p.PortfolioUid
                        AND
                        pa.ProcessDate = @processDate
                        AND
                        pa.ResultsreportableIndicator <> p.ResultsreportableIndicator
                LEFT JOIN dbo.AggregatorExcludedPortfolioType AS axpt
                    ON p.PortfolioTypeUid = axpt.PortfolioTypeUid
         WHERE
                axpt.PortfolioTypeUid IS NULL
                AND (
                    p.ResultsreportableIndicator = 'Y'              -- R2.2, R2.4
                    OR (
                        pa.ResultsreportableIndicator = 'Y'         -- R2.1
                        AND
                        ap.OldMembershipDate <> '9999-12-31'
                    )
                )
        OPTION(RECOMPILE)

        -- trace ---------------------------------------------------------------
        SET @traceRowCount = @@ROWCOUNT
        ------------------------------------------------------------------------

        -- R3.1 (AM record is deleted)
        SELECT  aud.PortfolioAmMembershipUid
          INTO
                #txDelete
          FROM
                dbo.PortfolioAmMembershipAudit AS aud
          WHERE
                aud.PortfolioDateTypeCode = 'R'
                AND
                aud.ActionCode = 'D'
                AND
                aud.ProcessDate = @processDate
                AND
                NOT EXISTS (SELECT  *
                              FROM  #txChange AS chg
                             WHERE  aud.PortfolioAmMembershipUid = chg.PortfolioAmMembershipUid)

        -- trace ---------------------------------------------------------------
        SET @traceRowCount = @traceRowCount + @@ROWCOUNT
        SET @traceTimeEnd = GETDATE();
        IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
            SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
        ELSE
            SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
        SET @traceMessage = 'SyntheticFlowProcess: ' + CAST(@traceRowCount AS VARCHAR) + ' row(s) found (' + @traceMessage
        RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
        ------------------------------------------------------------------------

        --=================================================================================
        -- Changes in PortfolioResultsReportable (excluding new and todays AM changes)
        --=================================================================================
        -- trace ---------------------------------------------------------------
        SET @traceTimeStart = GETDATE();
        RAISERROR ('SyntheticFlowProcess: Getting PortfolioResultsReportable changes...', 0, 1) WITH NOWAIT;
        ------------------------------------------------------------------------

        ;WITH tt AS
        (
            SELECT 1 AS TransactionType
            UNION ALL
            SELECT -1 AS TransactionType
        )
        , affectedPortfolios AS
        (
            SELECT  pam.PortfolioAmMembershipUid, pam.PortfolioUid, pam.MemberPortfolioUid,
                    CASE WHEN p.ResultsreportableIndicator = 'N' OR pam.EndDate < @maxProcessDate
                        THEN '9999-12-31'                                                               -- R2.1 (no new)
                        ELSE CASE WHEN tt.TransactionType = 1
                                THEN CASE WHEN pam.StartDate > @processDate THEN '9999-12-31' ELSE pam.StartDate END    -- R1.3
                                ELSE CASE WHEN pam.EndDate > @membershipEndDate THEN '9999-12-31' ELSE pam.EndDate END  -- R1.1, R1.2
                            END
                    END AS NewMembershipDate,
                    CASE WHEN p.ResultsreportableIndicator = 'Y'
                        THEN '9999-12-31'                                                               -- R2.2 (no old)
                        ELSE CASE WHEN tt.TransactionType = 1 THEN pam.StartDate ELSE pam.EndDate END
                    END AS OldMembershipDate,
                    tt.TransactionType,
                    p.ResultsReportableIndicator
              FROM
                    PortfolioAmMembership AS pam
                    JOIN dbo.Portfolio AS p
                        ON  pam.PortfolioDateTypeCode = 'R'
                            AND
                            p.PortfolioUid = pam.PortfolioUid
                            AND (
                                pam.ProcessDate < @processDate
                                OR (                                                        -- exclude new memebers
                                    pam.ProcessDate = @processDate
                                    AND
                                    EXISTS (SELECT  *
                                              FROM  dbo.PortfolioAMMembershipAudit AS aud WITH (FORCESEEK)
                                             WHERE  aud.PortfolioAMMembershipUid = pam.PortfolioAmMembershipUid)
                                   )
                            )
                    JOIN dbo.PortfolioAudit AS pa
                        ON  pa.PortfolioUid = p.PortfolioUid
                            AND
                            pa.ProcessDate = @processDate
                    LEFT JOIN dbo.AggregatorExcludedPortfolioType AS axpt
                        ON p.PortfolioTypeUid = axpt.PortfolioTypeUid
                    ,tt
             WHERE
                    axpt.PortfolioTypeUid IS NULL
                    AND (
                        (tt.TransactionType = 1 AND pam.StartDate < @processDate)           -- not current StartDate
                        OR
                        (tt.TransactionType = -1 AND pam.EndDate < @membershipEndDate)      -- not current NextBusinessDay(EndDate)
                    )
                    AND
                    pa.ResultsreportableIndicator <> p.ResultsreportableIndicator           -- R2.1, R2.2
                    AND
                    NOT EXISTS (SELECT  *                                                   -- exclude changes in AM membership
                                  FROM  #txChange AS c
                                 WHERE  c.PortfolioUid = pam.PortfolioUid
                                        AND c.PortfolioAMMembershipUid = pam.PortfolioAMMembershipUid
                                        AND c.TransactionType = tt.TransactionType)
        )
        INSERT
                #txChange
        SELECT
                ap.PortfolioAmMembershipUid, ap.PortfolioUid, ap.MemberPortfolioUid,
                ap.NewMembershipDate,
                ap.OldMembershipDate,
                ap.TransactionType,
                CASE WHEN ap.TransactionType = 1 AND ap.NewMembershipDate < @maxProcessDate THEN @maxProcessDate ELSE ap.NewMembershipDate END AS ActivityDate  -- R1.4
          FROM
                affectedPortfolios AS ap
         WHERE
                ap.NewMembershipDate <> '9999-12-31'
                OR
                ap.OldMembershipDate <> '9999-12-31'
        OPTION(RECOMPILE)

        -- trace ---------------------------------------------------------------
        SET @traceRowCount = @@ROWCOUNT
        SET @traceTimeEnd = GETDATE();
        IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
            SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
        ELSE
            SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
        SET @traceMessage = 'SyntheticFlowProcess: ' + CAST(@traceRowCount AS VARCHAR) + ' row(s) found (' + @traceMessage
        RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
        ------------------------------------------------------------------------

        --=================================================================================
        -- New memebers or members with current start or previous end business date
        --=================================================================================
        -- trace ---------------------------------------------------------------
        SET @traceTimeStart = GETDATE();
        RAISERROR ('SyntheticFlowProcess: Getting new memebers or members with current start or previous end business date...', 0, 1) WITH NOWAIT;
        ------------------------------------------------------------------------

        ;WITH tt AS
        (
            SELECT 1 AS TransactionType
            UNION ALL
            SELECT -1 AS TransactionType
        )
        , affectedPortfolios AS             -- R4.2, R5.2
        (
            SELECT  pam.PortfolioAmMembershipUid, pam.PortfolioUid, pam.MemberPortfolioUid,
                    CASE WHEN tt.TransactionType = 1 THEN pam.StartDate ELSE pam.EndDate END AS MembershipDate,
                    tt.TransactionType
              FROM  dbo.PortfolioAmMembership AS pam,
                    tt
             WHERE  pam.PortfolioDateTypeCode = 'R'
                    AND
                    pam.StartDate <= @processDate
                    AND
                    pam.EndDate >= @maxProcessDate
                    AND (
                        (                                   -- current start or previous end business date
                            pam.ProcessDate < @processDate
                            AND
                            ((tt.TransactionType = 1 AND pam.StartDate = @processDate)
                             OR
                             (tt.TransactionType = -1 AND pam.EndDate = @membershipEndDate))
                        )
                        OR (                                -- new memeber
                            pam.ProcessDate = @processDate
                            AND
                            NOT EXISTS (SELECT  *
                                          FROM  dbo.PortfolioAMMembershipAudit AS aud WITH (FORCESEEK)
                                         WHERE  aud.PortfolioAMMembershipUid = pam.PortfolioAmMembershipUid)
                        )
                    )
        )
        , new AS
        (
            SELECT  ap.*,
                    CASE WHEN ap.TransactionType = 1 AND ap.MembershipDate < @maxProcessDate THEN @maxProcessDate ELSE ap.MembershipDate END AS ActivityDate    -- R1.4
              FROM  affectedPortfolios AS ap
                    JOIN dbo.Portfolio AS p
                        ON  p.PortfolioUid = ap.PortfolioUid
                            AND
                            p.ResultsreportableIndicator = 'Y'              -- R2.4
                    LEFT JOIN dbo.AggregatorExcludedPortfolioType AS axpt
                        ON p.PortfolioTypeUid = axpt.PortfolioTypeUid
             WHERE
                    axpt.PortfolioTypeUid IS NULL
                    AND (
                        (ap.TransactionType = 1 AND ap.MembershipDate <= @processDate)          -- BUY: PostDate = StartDate
                        OR
                        (ap.TransactionType = -1 AND ap.MembershipDate <= @membershipEndDate)   -- SELL: PostDate = NextBusinessDay(EndDate)
                    )
        )
        , [all] AS
        (
            SELECT  n.PortfolioAmMembershipUid, n.PortfolioUid, n.MemberPortfolioUid, n.MembershipDate, n.TransactionType,
                    CASE WHEN n.TransactionType = 1 THEN n.ActivityDate ELSE bd.nextDay END AS PostDate,
                    CASE WHEN n.TransactionType = 1 THEN bd.prevDay ELSE n.ActivityDate END AS PositionDate
              FROM  new AS n
                    JOIN #bizdate AS bd
                        ON  bd.[date] = n.ActivityDate
            UNION ALL
            SELECT  c.PortfolioAmMembershipUid, c.PortfolioUid, c.MemberPortfolioUid, c.NewMembershipDate, c.TransactionType,
                    CASE WHEN c.TransactionType = 1 THEN c.ActivityDate ELSE bd.nextDay END AS PostDate,
                    CASE WHEN c.TransactionType = 1 THEN bd.prevDay ELSE c.ActivityDate END AS PositionDate
              FROM  #txChange AS c
                    JOIN #bizdate AS bd
                        ON bd.[date] = c.ActivityDate
             WHERE
                    NewMembershipDate <> '9999-12-31'
        )
        -- Note: Grouping is required for the case when the same membership has multiple date ranges
        --       and two or more start dates were set to @maxProcessDate (i.e. duplicate PostDate)
        SELECT  MAX(PortfolioAmMembershipUid) AS PortfolioAmMembershipUid, PortfolioUid, MemberPortfolioUid,
                MAX(MembershipDate) AS MembershipDate, TransactionType, PostDate, PositionDate
          INTO
                #txNew
          FROM
                [all]
        GROUP BY
                PortfolioUid, MemberPortfolioUid, TransactionType, PostDate, PositionDate
        OPTION (RECOMPILE)

        -- trace ---------------------------------------------------------------
        SET @traceRowCount = @@ROWCOUNT
        SET @traceTimeEnd = GETDATE();
        IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
            SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
        ELSE
            SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
        SET @traceMessage = 'SyntheticFlowProcess: ' + CAST(@traceRowCount AS VARCHAR) + ' row(s) found (' + @traceMessage
        RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
        ------------------------------------------------------------------------

        --=================================================================================
        -- #exrate - exchange rates
        --=================================================================================
        -- trace ---------------------------------------------------------------
        SET @traceTimeStart = GETDATE();
        ------------------------------------------------------------------------

        SET @minRangeDate = dbo.PreviousBusinessDate(@maxProcessDate)
        SET @maxRangeDate = @processDate

        CREATE Table #exrate
        (
            FromCurrencyID  INT             NOT NULL,
            ToCurrencyID    INT             NOT NULL,
            [Date]          DATE            NOT NULL,
            [Source]        NCHAR(2)        NOT NULL,
            Rate            DECIMAL(28,10)  NOT NULL

            PRIMARY KEY
            (
                FromCurrencyID,
                ToCurrencyID,
                [Date],
                [Source]
            )
        )

        INSERT  #exrate
          EXEC  dbo.GetExchangeRateTable @minRangeDate, @maxRangeDate WITH RECOMPILE

        -- trace ---------------------------------------------------------------
        SET @traceRowCount = @@ROWCOUNT
        SET @traceTimeEnd = GETDATE();
        IF DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
            SET @traceMessage = CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's)'
        ELSE
            SET @traceMessage = CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms)'
        SET @traceMessage = 'SyntheticFlowProcess: Generated exchange rates between ' + CAST(@minRangeDate AS VARCHAR) + ' and ' + CAST(@maxRangeDate AS VARCHAR) + ' (' + CAST(@traceRowCount AS VARCHAR) + ' row' + IIF(@traceRowCount=1, '', 's') +  ' in ' + @traceMessage
        RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
        ------------------------------------------------------------------------

        --=================================================================================
        -- PortfolioAccrualMarketValue
        --=================================================================================
        IF (@skipAccrual = 0)
        BEGIN
            -- trace ---------------------------------------------------------------
            SET @traceTimeStart = GETDATE();
            RAISERROR ('SyntheticFlowProcess: Executing SyntheticFlowProcessAccrual...', 0, 1) WITH NOWAIT;
            ------------------------------------------------------------------------
            IF (@internalTransaction = 1)
                BEGIN TRANSACTION

            EXEC dbo.SyntheticFlowProcessAccrual @processDate, @maxProcessDate, @canceledCount = @traceCanceledCount OUTPUT, @generatedCount = @traceGeneratedCount OUTPUT

            IF (@internalTransaction = 1)
                COMMIT TRANSACTION

            -- trace ---------------------------------------------------------------
            SET @traceTotalCanceledCount = @traceTotalCanceledCount + @traceCanceledCount
            SET @traceTotalGeneratedCount = @traceTotalGeneratedCount + @traceGeneratedCount
            SET @traceTimeEnd = GETDATE();
            SET @traceMessage = ''
            IF (@traceCanceledCount > 0)
                SET @traceMessage = @traceMessage + CAST(@traceCanceledCount AS VARCHAR) + ' canceled'
            IF (@traceGeneratedCount > 0)
                SET @traceMessage = @traceMessage + IIF(LEN(@traceMessage) = 0, '', ' and ') + CAST(@traceGeneratedCount AS VARCHAR) + ' generated'
            IF (LEN(@traceMessage) = 0)
                SET @traceMessage = 'nothing found'
            SET @traceMessage = 'SyntheticFlowProcess: SyntheticFlowProcessAccrual completed in ' +
                                CASE WHEN DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
                                    THEN CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's'
                                    ELSE CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms'
                                END + ' (' + @traceMessage + ')'
            RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
            ------------------------------------------------------------------------
        END
        ELSE
            RAISERROR ('WARNING: The processing of Synthetic Flows for accruals will be skipped because it has already been processed', 0, 1) WITH NOWAIT;

        --=================================================================================
        -- PortfolioPositionMarketValue
        --=================================================================================
        IF (@skipPosition = 0)
        BEGIN
            -- trace ---------------------------------------------------------------
            SET @traceTimeStart = GETDATE();
            RAISERROR ('SyntheticFlowProcess: Executing SyntheticFlowProcessPosition...', 0, 1) WITH NOWAIT;
            ------------------------------------------------------------------------

            IF (@internalTransaction = 1)
                BEGIN TRANSACTION

            EXEC dbo.SyntheticFlowProcessPosition @processDate, @maxProcessDate, @canceledCount = @traceCanceledCount OUTPUT, @generatedCount = @traceGeneratedCount OUTPUT

            IF (@internalTransaction = 1)
                COMMIT TRANSACTION

            -- trace ---------------------------------------------------------------
            SET @traceTotalCanceledCount = @traceTotalCanceledCount + @traceCanceledCount
            SET @traceTotalGeneratedCount = @traceTotalGeneratedCount + @traceGeneratedCount
            SET @traceTimeEnd = GETDATE();
            SET @traceMessage = ''
            IF (@traceCanceledCount > 0)
                SET @traceMessage = @traceMessage + CAST(@traceCanceledCount AS VARCHAR) + ' canceled'
            IF (@traceGeneratedCount > 0)
                SET @traceMessage = @traceMessage + IIF(LEN(@traceMessage) = 0, '', ' and ') + CAST(@traceGeneratedCount AS VARCHAR) + ' generated'
            IF (LEN(@traceMessage) = 0)
                SET @traceMessage = 'nothing found'
            SET @traceMessage = 'SyntheticFlowProcess: SyntheticFlowProcessPosition completed in ' +
                                CASE WHEN DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) >= 1000
                                    THEN CAST(DATEDIFF(second, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 's'
                                    ELSE CAST(DATEDIFF(millisecond, @traceTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms'
                                END + ' (' + @traceMessage + ')'
            RAISERROR (@traceMessage, 0, 1) WITH NOWAIT
            ------------------------------------------------------------------------
        END
        ELSE
            RAISERROR ('WARNING: The processing of Synthetic Flows for positions will be skipped because it has already been processed', 0, 1) WITH NOWAIT;
    END TRY
    BEGIN CATCH
        IF (@internalTransaction = 1 AND XACT_STATE() <> 0)
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH

    -- trace ---------------------------------------------------------------
    SET @traceTimeEnd = GETDATE();
            SET @traceMessage = ''
            IF (@traceTotalCanceledCount > 0)
                SET @traceMessage = @traceMessage + CAST(@traceTotalCanceledCount AS VARCHAR) + ' canceled'
            IF (@traceTotalGeneratedCount > 0)
                SET @traceMessage = @traceMessage + IIF(LEN(@traceMessage) = 0, '', ' and ') + CAST(@traceTotalGeneratedCount AS VARCHAR) + ' generated'
            IF (LEN(@traceMessage) = 0)
                SET @traceMessage = 'nothing found'
            SET @traceMessage = 'Stored Proc SyntheticFlowProcess ' + CAST(@processDate AS VARCHAR(10)) + ' ' + CAST(@maxProcessDate AS VARCHAR(10)) + ' - Completed in ' +
                                CASE WHEN DATEDIFF(millisecond, @traceTotalTimeStart, @traceTimeEnd) >= 1000
                                    THEN CAST(DATEDIFF(second, @traceTotalTimeStart, @traceTimeEnd) AS VARCHAR) + 's'
                                    ELSE CAST(DATEDIFF(millisecond, @traceTotalTimeStart, @traceTimeEnd) AS VARCHAR) + 'ms'
                                END + ' (' + @traceMessage + ')'
    RAISERROR (@traceMessage, 0, 1) WITH NOWAIT;
    ------------------------------------------------------------------------
END
