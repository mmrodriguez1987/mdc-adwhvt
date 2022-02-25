USE [cdcProdccb]
GO

DECLARE	@return_value int

DECLARE @startDate AS DATETIME
DECLARE @endDate AS DATETIME

SET @startDate = '2022-02-21 10:00'
SET @endDate = '2022-02-22 10:00'

EXEC	@return_value = [cdc].[sp_ci_bseg_ct] @startDate, @endDate
		

SELECT	'Return Value' = @return_value

GO
