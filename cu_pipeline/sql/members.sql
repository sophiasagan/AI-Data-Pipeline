-- Raw member pull: active members with demographic and relationship data.
-- No OFFSET/FETCH — caller applies date-windowed filters at the ETL layer.
SELECT
    m.MemberID,
    m.MemberNumber,
    m.FirstName,
    m.LastName,
    m.DateOfBirth,
    m.SSNLast4,
    m.MembershipOpenDate,
    m.MembershipStatus,           -- A=Active, I=Inactive, C=Closed
    m.MemberType,                 -- IND, BUS, JNT
    m.PrimaryBranchID,
    m.EmployerID,
    m.CreditScore,
    m.CreditScoreDate,
    m.AnnualIncome,               -- DECIMAL(15,2)
    m.DebtToIncomeRatio,          -- DECIMAL(5,4)
    m.IsEmployee,
    m.IsMinor,
    m.EmailAddress,
    m.PhoneNumber,
    m.AddressLine1,
    m.City,
    m.StateCode,
    m.ZipCode,
    m.LastActivityDate,
    m.CreatedDate,
    m.ModifiedDate
FROM dbo.tblMember m
WHERE
    m.MembershipStatus = 'A'
    AND m.IsTestAccount = 0
ORDER BY
    m.MemberID;
