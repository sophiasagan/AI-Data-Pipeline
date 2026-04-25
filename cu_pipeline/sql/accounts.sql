-- Account balance history: all open share and loan accounts with current balances.
-- Monetary columns are DECIMAL(15,2) — never cast to float downstream.
SELECT
    a.AccountID,
    a.AccountNumber,
    a.MemberID,
    a.AccountTypeCode,            -- SHR=Share, CHK=Checking, SAV=Savings, CD=Certificate, IRA
    a.AccountTypeDescription,
    a.ProductCode,
    a.OpenDate,
    a.MaturityDate,               -- NULL for non-term products
    a.CloseDate,                  -- NULL if still open
    a.AccountStatus,              -- O=Open, D=Dormant, C=Closed
    a.CurrentBalance,             -- DECIMAL(15,2)
    a.AvailableBalance,           -- DECIMAL(15,2)
    a.MinimumBalance,             -- DECIMAL(15,2)
    a.InterestRate,               -- DECIMAL(7,6)
    a.APY,                        -- DECIMAL(7,6)
    a.DividendYTD,                -- DECIMAL(15,2)
    a.InterestPaidYTD,            -- DECIMAL(15,2)
    a.LastTransactionDate,
    a.LastStatementDate,
    a.OverdraftProtectionFlag,
    a.JointOwnerCount,
    a.BranchID,
    a.OfficerID,
    a.CreatedDate,
    a.ModifiedDate
FROM dbo.tblAccount a
INNER JOIN dbo.tblMember m
    ON a.MemberID = m.MemberID
WHERE
    a.AccountStatus IN ('O', 'D')
    AND m.IsTestAccount = 0
ORDER BY
    a.MemberID,
    a.AccountID;
