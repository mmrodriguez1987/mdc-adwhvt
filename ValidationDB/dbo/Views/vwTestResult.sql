CREATE VIEW vwTestResult
AS
SELECT 
TR.testResultID, 
S.stateName, 
TR.result, 
TR.iniEvalDate, 
TR.endEvalDate, 
TR.countCDC, 
TR.countDTW, 
TR.queryCDC, 
TR.queryDTW, 
TR.effectDate, 
TR.isActive, 
T.testName, 
F1.columnName col1,
F2.columnName col2,
F3.columnName col3,
E1.entityShortName TableName1,
E2.entityShortName TableName2,
E3.entityShortName TableName3
FROM TestResult TR 
INNER JOIN [State] S ON TR.stateID=S.stateID
INNER JOIN Test T ON T.testID=TR.testID
INNER JOIN [Column] F1 ON F1.columnID=T.firstColumnID
INNER JOIN [Column] F2 ON F2.columnID=T.secondColumnID
INNER JOIN [Column] F3 ON F3.columnID=T.secondColumnID
INNER JOIN Entity E1 ON E1.entityID=F1.columnID
INNER JOIN Entity E2 ON E2.entityID=F2.columnID
INNER JOIN Entity E3 ON E3.entityID=F3.columnID