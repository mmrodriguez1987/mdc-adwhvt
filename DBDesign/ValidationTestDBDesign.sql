/*DELETE OBJECTS IF EXIST*/

IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'TestResultsDetail') DROP TABLE TestResultsDetail
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'TestResults') DROP TABLE TestResults
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'TestState') DROP TABLE TestState
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'TestInfo') DROP TABLE TestInfo
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'HistoricalIndicators') DROP TABLE HistoricalIndicators
IF EXISTS (SELECT * FROM sys.all_views WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'vwTestResultAll') DROP VIEW vwTestResultAll

GO
/*CREATE TABLES*/

CREATE TABLE TestState
(
	ID BIGINT IDENTITY(1,1) PRIMARY KEY,
	state_name VARCHAR(50) ,
	state_message VARCHAR(300) ,
	isActive BIT
)

GO

CREATE TABLE TestInfo
(
	ID BIGINT IDENTITY(1,1) PRIMARY KEY,
	entity VARCHAR(100),
	testName VARCHAR(500),
	purpose VARCHAR(500),	
	isBusinessRule BIT,
	isStatisticalRule BIT,
	isUnilateral BIT,
	isActive BIT
)

GO

CREATE TABLE TestResults
(
	ID BIGINT IDENTITY(1,1) PRIMARY KEY,
	stateID BIGINT FOREIGN KEY REFERENCES TestState(ID),
	infoID BIGINT FOREIGN KEY REFERENCES TestInfo(ID),
	entity VARCHAR(100) ,
	result VARCHAR(3000) ,
	iniEvalDate datetime ,
	endEvalDate datetime ,
	countCDC BIGINT ,
	countDTW BIGINT ,
	queryCDC VARCHAR(3000),
	queryDTW VARCHAR(3000),
	effectDate DATETIME ,
	isActive BIT
)

GO

CREATE TABLE TestResultsDetail
(
	ID BIGINT IDENTITY(1,1)  PRIMARY KEY,
	testResultID BIGINT ,
	affected_keys_array VARCHAR(5000) ,
	affected_key_name VARCHAR(100) ,
	dbName VARCHAR(100)
)

GO

CREATE VIEW vwTestResultAll
AS
	SELECT 
	R.ID TestResulID, 
	S.state_name TestState, 
	I.testName TestName,
	R.entity Entities, 
	R.result Comment, 
	R.iniEvalDate initDate, 
	R.endEvalDate EndDate,
	R.queryCDC count_cdc,
	R.queryDTW count_dtw,
	R.effectDate TestEffectiveDate
	FROM TestResults R 
	INNER JOIN TestState s on R.stateID=s.ID
	INNER JOIN TestInfo i on R.infoID=i.ID

go

CREATE TABLE HistoricalIndicators 
(                                                                                                                                                              
ID BIGINT IDENTITY(1,1) PRIMARY KEY,
entityName VARCHAR(100),
columnName VARCHAR(100),
distinctCountVal BIGINT,
newCountVal BIGINT,
updatedCountVal BIGINT,
maxVal BIGINT,
minVal BIGINT,
calculatedDate DATETIME,
isActive BIT
)
GO

/*INSERT TEST INFO*/
INSERT INTO TestInfo VALUES ('BILLED_USAGE','Bills Generated On Weekend','Check that no bills generated on weekend', 1,0,1,1)
INSERT INTO TestInfo VALUES ('BILLED_USAGE','Bills Generated On Wrong Fiscal Year','Check that no bills generated on a wrong fiscal year',1,0,1,1)
INSERT INTO TestInfo VALUES ('BILLED_USAGE','Count Distinct Bills Against The Historical Maximun','Contrast the historical maximum with the current count to detect errors', 0,1,1,1)
INSERT INTO TestInfo VALUES ('BILLED_USAGE','New Bills Count','Compare count of new bills between CCB and DTWH', 0,0,0,1)

INSERT INTO TestInfo VALUES ('ACCT','Distinct Accounts','Compare count of Distinct ACCT between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('ACCT','New Accounts','Compare count of New ACCT between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('ACCT','Updated Accounts','Compare count of Updated ACCT between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('ACCT','Maximun Historical Accounts','Compare count of Distinct ACCT between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('ACCT','Statistical Average Accounts','Compare count of Distinct ACCT between CCB and DTWH',0,0,0,1)

INSERT INTO TestInfo VALUES ('PER','Distinct Persons','Compare count of Distinct PER between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('PER','New Persons','Compare count of New PER between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('PER','Updated Persons','Compare count of Updated PER between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('PER','Maximun Historical Persons','Compare count of Distinct PER between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('PER','Statistical Average Persons','Compare count of Distinct PER between CCB and DTWH',0,0,0,1)


INSERT INTO TestInfo VALUES ('PREM','Distinct Premises','Compare count of Distinct PREM between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('PREM','New Premises','Compare count of New PREM between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('PREM','Updated Premises','Compare count of Updated PREM between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('PREM','Maximun Historical Premises','Compare count of Distinct PREM between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('PREM','Statistical Average Premises','Compare count of Distinct PREM between CCB and DTWH',0,0,0,1)


INSERT INTO TestInfo VALUES ('SA','Distinct Service Agreements','Compare count of Distinct SA between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('SA','New Service Agreements','Compare count of SA PREM between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('SA','Updated Service Agreements','Compare count of Updated SA between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('SA','Maximun Historical Service Agreements','Compare count of Distinct SA between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('SA','Statistical Average Service Agreements','Compare count of Distinct SA between CCB and DTWH',0,0,0,1)

INSERT INTO TestInfo VALUES ('BSEG','Distinct Bill Segments','Compare count of Distinct BSEG between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('BSEG','New Bill Segments','Compare count of BSEG between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('BSEG','Updated Bill Segments','Compare count of Updated BSEG between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('BSEG','Maximun Historical Bill Segments','Compare count of Distinct BSEG between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('BSEG','Statistical Average Bill Segments','Compare count of Distinct BSEG between CCB and DTWH',0,0,0,1)

INSERT INTO TestInfo VALUES ('FT','Distinct FT','Compare count of Distinct FT between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('FT','New FT','Compare count of FT between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('FT','Updated FT','Compare count of Updated FT between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('FT','Maximun Historical FT','Compare count of Distinct FT between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('FT','Statistical Average FT','Compare count of Distinct FT between CCB and DTWH',0,0,0,1)

INSERT INTO TestInfo VALUES ('SQI','Distinct SQI','Compare count of Distinct SQI between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('SQI','New SQI','Compare count of SQI between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('SQI','Updated SQI','Compare count of Updated SQI between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('SQI','Maximun Historical SQI','Compare count of Distinct SQI between CCB and DTWH',0,0,0,1)
INSERT INTO TestInfo VALUES ('SQI','Statistical Average SQI','Compare count of Distinct SQI between CCB and DTWH',0,0,0,1)


/*Test States*/
INSERT INTO TestState VALUES ('Warning','Test with some warnings',1)
INSERT INTO TestState VALUES ('Failed','Test with errors, need atention',1)
INSERT INTO TestState VALUES ('OK!','Test Successfully completed',1)


/* HistoricalIndicators*/

INSERT INTO HistoricalIndicators VALUES('ACCT','ID',135800, 2583, 150, -1, -1, '2022-01-18 12:00',1)
INSERT INTO HistoricalIndicators VALUES('ACCT','ID',135800, 2583, 150, -1, -1, '2022-01-17 12:00',1)
INSERT INTO HistoricalIndicators VALUES('ACCT','ID',135800, 2583, 150, -1, -1, '2022-01-16 12:00',1)