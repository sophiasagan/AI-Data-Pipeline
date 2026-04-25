-- 90-day transaction summary: one row per transaction within the lookback window.
-- :days is a named bind parameter supplied by pull_transactions(days=N).
SELECT
    t.TransactionID,
    t.AccountID,
    t.MemberID,
    t.TransactionDate,
    t.PostedDate,
    t.TransactionTypeCode,        -- DEP, WD, TRF, FEE, DIV, PMT, ADJ
    t.TransactionTypeDescription,
    t.ChannelCode,                -- ATM, ACH, WEB, TEL, BR, POS, WIRE
    t.Amount,                     -- DECIMAL(15,2); positive=credit, negative=debit
    t.RunningBalance,             -- DECIMAL(15,2) balance after this txn
    t.Description,
    t.MerchantName,
    t.MerchantCategoryCode,       -- MCC (4-digit ISO 18245)
    t.IsReversed,
    t.ReversalTransactionID,      -- FK back to tblTransaction, NULL if not a reversal
    t.IsInternalTransfer,
    t.RelatedAccountID,           -- populated for internal transfers
    t.CheckNumber,                -- NULL for non-check transactions
    t.RoutingNumber,              -- populated for ACH
    t.CreatedDate
FROM dbo.tblTransaction t
INNER JOIN dbo.tblMember m
    ON t.MemberID = m.MemberID
WHERE
    t.TransactionDate >= DATEADD(DAY, -:days, CAST(GETDATE() AS DATE))
    AND t.IsReversed = 0
    AND m.IsTestAccount = 0
ORDER BY
    t.MemberID,
    t.AccountID,
    t.TransactionDate,
    t.TransactionID;
