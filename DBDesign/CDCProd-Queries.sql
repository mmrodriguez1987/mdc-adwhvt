USE [cdcProdccb]
GO

DECLARE	@return_value int

DECLARE @startDate AS DATETIME
DECLARE @endDate AS DATETIME

SET @startDate = '2022-01-10 12:30'
SET @endDate = '2022-01-11 12:30'

EXEC	@return_value = [cdc].[sp_ci_acct_ct] @startDate, @endDate
		

SELECT	'Return Value' = @return_value

GO
