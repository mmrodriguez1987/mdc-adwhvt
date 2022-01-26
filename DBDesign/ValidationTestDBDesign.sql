
--create database [valdat-m2c-dtwh-dev]

--use [valdat-m2c-dtwh-dev]
/*DELETE OBJECTS IF EXIST*/
IF EXISTS (SELECT * FROM sys.all_views WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'vwTestInformation') DROP VIEW vwTestInformation
IF EXISTS (SELECT * FROM sys.all_views WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'vwDataEntity') DROP VIEW vwDataEntity
IF EXISTS (SELECT * FROM sys.all_views WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'vwTestResult') DROP VIEW vwTestResult
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'HistoricalIndicator') DROP TABLE HistoricalIndicator
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'ResultDetail') DROP TABLE ResultDetail
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'Result') DROP TABLE Result
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'Test') DROP TABLE Test
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'State') DROP TABLE [State]
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'Column') DROP TABLE [Column]
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'Entity') DROP TABLE Entity
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'Database') DROP TABLE [Database]



GO
/*CREATE TABLES*/
CREATE TABLE [Source] 
(
	sourceID BIGINT IDENTITY(1,1) PRIMARY KEY,
	dbName VARCHAR(50),
	dbDescription VARCHAR(500),
	isActive BIT
)
go
CREATE TABLE Entity 
(
	entityID BIGINT IDENTITY(1,1) PRIMARY KEY,
	sourceID BIGINT FOREIGN KEY REFERENCES [Source](sourceID),
	entityFormalName VARCHAR(50),
	entityShortName VARCHAR(50),
	typeEntity CHAR(1),
	isActive BIT
)

go
CREATE TABLE ColumnDefinition
(
	columnID BIGINT IDENTITY(1,1) PRIMARY KEY,
	entityID BIGINT FOREIGN KEY REFERENCES Entity(entityID),
	columnName VARCHAR(50),
	[description] VARCHAR(300),
	ordinalPosition INT,
	isActive BIT
)
go
CREATE TABLE [State]
(
	stateID BIGINT IDENTITY(1,1) PRIMARY KEY,
	stateName VARCHAR(50) ,
	stateMessage VARCHAR(300) ,
	isActive BIT
)

GO
CREATE TABLE Test
(
	testID BIGINT IDENTITY(1,1) PRIMARY KEY,
	--firstColumnID BIGINT FOREIGN KEY REFERENCES [Column](columnID),	
	--secondColumnID BIGINT FOREIGN KEY REFERENCES [Column](columnID),	
	--thirdColumnID BIGINT FOREIGN KEY REFERENCES [Column](columnID),
	testName VARCHAR(500),
	testDescription VARCHAR(500),	
	isBusinessRule BIT,
	isStatisticalRule BIT,
	isUnilateral BIT,
	isETLRule BIT,
	isActive BIT
)

CREATE TABLE TestParameter
(
paramID BIGINT IDENTITY(1,1) PRIMARY KEY,
testID BIGINT FOREIGN KEY REFERENCES Test(testID),
columnID BIGINT FOREIGN KEY REFERENCES ColumnDefinition(columnID),
)

GO
CREATE TABLE Result
(
	resultID BIGINT IDENTITY(1,1) PRIMARY KEY,
	stateID BIGINT FOREIGN KEY REFERENCES [State](stateID),
	testID BIGINT FOREIGN KEY REFERENCES Test(testID),	
	[description] VARCHAR(3000) ,
	iniEvalDate datetime ,
	endEvalDate datetime ,
	countCDC BIGINT ,
	countDTW BIGINT ,
	queryCDC VARCHAR(3000),
	queryDTW VARCHAR(3000),
	effectDate DATETIME,
	isActive BIT
)

GO
CREATE TABLE ResultDetail
(
	resultDetailID BIGINT IDENTITY(1,1)  PRIMARY KEY,
	resultID BIGINT FOREIGN KEY REFERENCES Result(ResultID),
	affected_key_array VARCHAR(5000) ,
	affected_key_name VARCHAR(100)
)

GO
CREATE TABLE HistoricalIndicator
(                                                                                                                                                              
histIndicatorID BIGINT IDENTITY(1,1) PRIMARY KEY,
columnID BIGINT FOREIGN KEY REFERENCES ColumnDefinition(columnID),
distinctCountVal BIGINT,
newCountVal BIGINT,
updatedCountVal BIGINT,
maxVal BIGINT,
minVal BIGINT,
calculatedDate DATETIME,
isActive BIT
)

GO
/*
CREATE VIEW vwTestResult
AS
SELECT 
TR.resultID, 
S.stateName, 
TR.[description], 
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
FROM Result TR 
INNER JOIN [State] S ON TR.stateID=S.stateID
INNER JOIN Test T ON T.testID=TR.testID
INNER JOIN [Column] F1 ON F1.columnID=T.firstColumnID
INNER JOIN [Column] F2 ON F2.columnID=T.secondColumnID
INNER JOIN [Column] F3 ON F3.columnID=T.secondColumnID
INNER JOIN Entity E1 ON E1.entityID=F1.columnID
INNER JOIN Entity E2 ON E2.entityID=F2.columnID
INNER JOIN Entity E3 ON E3.entityID=F3.columnID
*/
GO

CREATE VIEW vwDataEntity
AS
SELECT  DB.dbName,t.entityID, T.entityFormalName,T.entityShortName,T.typeEntity,C.columnID,  C.columnName,C.ordinalPosition FROM 
[Source] DB 
INNER JOIN Entity T ON DB.sourceID=T.sourceID
INNER JOIN ColumnDefinition C ON T.entityID=C.entityID

GO
/*
CREATE VIEW vwTestInformation
AS
SELECT 
T.testID,
F1.columnID ColumnID1,
F1.columnName ColumnName1,
E1.entityID EntityID1,
E1.entityFormalName EntityFormalName1,
E1.entityShortName EntityShortName1,
D1.databaseID databaseID1,
D1.dbName databaseName1,

F2.columnID ColumnID2,
F2.columnName ColumnName2,
E2.entityID EntityID2,
E2.entityFormalName EntityFormalName2,
E2.entityShortName EntityShortName2,
D2.databaseID databaseID2,
D2.dbName databaseName2,

F3.columnID ColumnID3,
F3.columnName ColumnName3,
E3.entityID EntityID3,
E3.entityFormalName EntityFormalName3,
E3.entityShortName EntityShortName3,
D3.databaseID databaseID3,
D3.dbName databaseName3,
T.testName,
T.testDescription,
T.isBusinessRule,
T.isStatisticalRule,
T.isUnilateral,
T.isETLRule,
T.isActive
FROM  Test T
INNER JOIN [Column] F1 ON F1.columnID=T.firstColumnID
INNER JOIN [Column] F2 ON F2.columnID=T.secondColumnID
INNER JOIN [Column] F3 ON F3.columnID=T.thirdColumnID
INNER JOIN Entity E1 ON E1.entityID=F1.entityID
INNER JOIN Entity E2 ON E2.entityID=F2.entityID
INNER JOIN Entity E3 ON E3.entityID=F3.entityID
INNER JOIN [Database] D1 ON D1.databaseID=E1.databaseID
INNER JOIN [Database] D2 ON D2.databaseID=E2.databaseID
INNER JOIN [Database] D3 ON D3.databaseID=E3.databaseID
*/

GO

INSERT INTO [Source] VALUES('NA', 'NA',1)											--1
INSERT INTO [Source] VALUES('cdcProd','Change Data Capture from CCBProd',1)		--2
INSERT INTO [Source] VALUES('dw-ttdp', 'Datawarehouse',1)							--3
									--3

INSERT INTO Entity VALUES (1,'NA','NA','N',1)
INSERT INTO Entity VALUES (3,'dwadm2.CD_ACCT','CD_ACCT','T',1)							--2
INSERT INTO Entity VALUES (3,'dwadm2.CD_ADDR','CD_ADDR','T',1)							--3
INSERT INTO Entity VALUES (3,'dwadm2.CD_DATE','CD_DATE','T',1)							--4
INSERT INTO Entity VALUES (3,'dwadm2.CD_FISCAL_CAL','CD_FISCAL_CAL','T',1)				--5
INSERT INTO Entity VALUES (3,'dwadm2.CD_PER','CD_PER','T',1)							--6
INSERT INTO Entity VALUES (3,'dwadm2.CD_PREM','CD_PREM','T',1)							--7
INSERT INTO Entity VALUES (3,'dwadm2.CD_RATE','CD_RATE','T',1)							--8
INSERT INTO Entity VALUES (3,'dwadm2.CD_SA','CD_SA','T',1)								--9
INSERT INTO Entity VALUES (3,'dwadm2.CD_SQI','CD_SQI','T',1)							--10
INSERT INTO Entity VALUES (3,'dwadm2.CD_UOM','CD_UOM','T',1)							--11
INSERT INTO Entity VALUES (3,'dwadm2.CF_BILLED_USAGE','CF_BILLED_USAGE','T',1)			--12

INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_ACCT_CT','ACCT','T',1)						--13
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_BSEG_CT','BSEG','T',1)						--14
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_BSEG_CALC_CT','BSEG_CALC','T',1)			--15
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_BSEG_CALC_LN_CT','CALC_LN','T',1)			--16
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_BSEG_SQ_CT','BSEG_SQ','T',1)				--17
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_CAL_PERIOD_CT','CAL_PERIOD','T',1)			--18
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_FT_CT','FT','T',1)							--19
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_PER_CT','PER','T',1)						--20
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_PREM_CT','PREM','T',1)						--21
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_RS_CT','RS','T',1)							--22
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_SA_CT','SA','T',1)							--23
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_SQI_CT','SQI','T',1)						--24
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_UOM_CT','UOM','T',1)						--25
INSERT INTO Entity VALUES (2,'dwadm2.vw_BillDate','vw_BillDate','V',1)					--26

--ACCOUNT
INSERT INTO [columnDefinition] VALUES(1,'NA',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(2,'ACCT_KEY',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(2,'SRC_ACCT_ID','Account_ID',2,1)
INSERT INTO [columnDefinition] VALUES(2,'DATA_LOAD_DTTM',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(2,'DATA_SOURCE_IND',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(2,'ACCT_INFO',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF10_CD',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF10_DESCR',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF1_CD','Customer Class Code',8,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF1_DESCR','Customer Class',9,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF2_CD',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF2_DESCR',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF3_CD','CIS Division Code',12,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF3_DESCR','CIS Division',13,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF4_CD','Bill Cycle Code',14,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF4_DESCR','Bill Cycle',15,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF5_CD','Item Collection Class Code',16,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF5_DESCR','Collection Class',17,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF6_CD',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF6_DESCR','Account Setup Date',19,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF7_CD',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF7_DESCR',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF8_CD',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF8_DESCR',NULL,23,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF9_CD',NULL,24,1)
INSERT INTO [columnDefinition] VALUES(2,'UDF9_DESCR',NULL,25,1)
INSERT INTO [columnDefinition] VALUES(2,'EFF_END_DTTM','Effective End Date',26,1)
INSERT INTO [columnDefinition] VALUES(2,'EFF_START_DTTM','Effective Start Date',27,1)
INSERT INTO [columnDefinition] VALUES(2,'JOB_NBR',NULL,28,1)

--ADDRES
INSERT INTO [columnDefinition] VALUES(3,'ADDR_KEY',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(3,'ADDR_LINE1',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(3,'ADDR_LINE2',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(3,'ADDR_LINE3',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(3,'ADDR_LINE4',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(3,'DATA_LOAD_DTTM',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(3,'DATA_SOURCE_IND',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(3,'SRC_ADDR_ID','PremiseID',8,1)
INSERT INTO [columnDefinition] VALUES(3,'ADDR_INFO','Full Address',9,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF10_CD',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF10_DESCR',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF1_CD',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF1_DESCR','City Upper',13,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF2_CD',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF2_DESCR','latitude',15,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF3_CD',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF3_DESCR','longitude',17,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF4_CD',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF4_DESCR',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF5_CD',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF5_DESCR',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF6_CD',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF6_DESCR',NULL,23,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF7_CD',NULL,24,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF7_DESCR',NULL,25,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF8_CD',NULL,26,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF8_DESCR',NULL,27,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF9_CD',NULL,28,1)
INSERT INTO [columnDefinition] VALUES(3,'UDF9_DESCR',NULL,29,1)
INSERT INTO [columnDefinition] VALUES(3,'EFF_END_DTTM','Effective End Date',30,1)
INSERT INTO [columnDefinition] VALUES(3,'EFF_START_DTTM','Effective Start Date',31,1)
INSERT INTO [columnDefinition] VALUES(3,'JOB_NBR',NULL,32,1)
INSERT INTO [columnDefinition] VALUES(3,'CITY','City',33,1)
INSERT INTO [columnDefinition] VALUES(3,'COUNTY','County',34,1)
INSERT INTO [columnDefinition] VALUES(3,'POSTAL','Zip Code',35,1)
INSERT INTO [columnDefinition] VALUES(3,'STATE_CD','State Code',36,1)
INSERT INTO [columnDefinition] VALUES(3,'STATE_DESCR','State',37,1)
INSERT INTO [columnDefinition] VALUES(3,'COUNTRY_CD','Country Code',38,1)
INSERT INTO [columnDefinition] VALUES(3,'COUNTRY_DESCR','Country',39,1)
INSERT INTO [columnDefinition] VALUES(3,'GEO_CODE',NULL,40,1)
INSERT INTO [columnDefinition] VALUES(3,'CROSS_STREET',NULL,41,1)
INSERT INTO [columnDefinition] VALUES(3,'SUBURB',NULL,42,1)

--PER
INSERT INTO [columnDefinition] VALUES(6,'PER_KEY',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(6,'BUSINESS_IND','Business Indicator',2,1)
INSERT INTO [columnDefinition] VALUES(6,'DATA_LOAD_DTTM',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(6,'DATA_SOURCE_IND',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(6,'PER_NAME','Name',5,1)
INSERT INTO [columnDefinition] VALUES(6,'PER_PHONE_NBR',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(6,'SRC_PER_ID','Person ID',7,1)
INSERT INTO [columnDefinition] VALUES(6,'PER_INFO',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF1_CD','Lssl Code',9,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF1_DESCR',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF2_CD','Person Type',11,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF2_DESCR',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF3_CD',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF3_DESCR',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF4_CD',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF4_DESCR',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF5_CD',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(6,'UDF5_DESCR',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(6,'EFF_END_DTTM','Effective End Date',19,1)
INSERT INTO [columnDefinition] VALUES(6,'EFF_START_DTTM','Effective Start Date',20,1)
INSERT INTO [columnDefinition] VALUES(6,'JOB_NBR',NULL,21,1)

--PREM
INSERT INTO [columnDefinition] VALUES(7,'PREM_KEY',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(7,'DATA_LOAD_DTTM',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(7,'DATA_SOURCE_IND',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(7,'SRC_PREM_ID','PremiseID',4,1)
INSERT INTO [columnDefinition] VALUES(7,'PREM_INFO',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF10_CD',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF10_DESCR',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF1_CD','Jurisdiction Code',8,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF1_DESCR','Jurisdiction',9,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF2_CD','Premise Type Code',10,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF2_DESCR','Premise Type',11,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF3_CD',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF3_DESCR',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF4_CD','Trend Area Code',14,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF4_DESCR','Trend Area',15,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF5_CD','Incity Limit Code',16,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF5_DESCR',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF6_CD','Municipality Code',18,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF6_DESCR','Municipality',19,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF7_CD','Meter Size Decimal',20,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF7_DESCR','Meter Size',21,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF8_CD','Bill Muni Val',22,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF8_DESCR','Bill Muni',23,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF9_CD','Muni Tax Val',24,1)
INSERT INTO [columnDefinition] VALUES(7,'UDF9_DESCR','Muni Tax Code',25,1)
INSERT INTO [columnDefinition] VALUES(7,'EFF_END_DTTM','Effective End Date',26,1)
INSERT INTO [columnDefinition] VALUES(7,'EFF_START_DTTM','Effective Start Date',27,1)
INSERT INTO [columnDefinition] VALUES(7,'JOB_NBR',NULL,28,1)

--RATE
INSERT INTO [columnDefinition] VALUES(8,'RATE_KEY',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(8,'DATA_LOAD_DTTM',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(8,'DATA_SOURCE_IND',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(8,'RATE_SCHED_CD','Rate Scheduled Code',4,1)
INSERT INTO [columnDefinition] VALUES(8,'RATE_SCHED_DESCR','Rate',5,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF1_CD','Service Type Code',6,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF1_DESCR','Service Type',7,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF2_CD','Frequency Code',8,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF2_DESCR',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF3_CD',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF3_DESCR',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF4_CD',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF4_DESCR',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF5_CD',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(8,'UDF5_DESCR',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(8,'EFF_END_DTTM','Effective End Date',16,1)
INSERT INTO [columnDefinition] VALUES(8,'EFF_START_DTTM','Effective Start Date',17,1)
INSERT INTO [columnDefinition] VALUES(8,'JOB_NBR',NULL,18,1)

--SA
INSERT INTO [columnDefinition] VALUES(9,'SA_KEY',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(9,'DATA_LOAD_DTTM',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(9,'SRC_SA_ID','SaID',3,1)
INSERT INTO [columnDefinition] VALUES(9,'DATA_SOURCE_IND',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(9,'SPECIAL_ROLE_DESCR','Special Role',5,1)
INSERT INTO [columnDefinition] VALUES(9,'SPECIAL_ROLE_CD','Special Role Code',6,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF10_CD',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF10_DESCR',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF1_CD','Service Type Code',9,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF1_DESCR','Service Type',10,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF2_CD','Cis Division Code',11,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF2_DESCR','Cis Division',12,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF3_CD','Sa Type Code',13,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF3_DESCR','Sa Type',14,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF4_CD','Revenue Class Code',15,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF4_DESCR','Revenue Class',16,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF5_CD',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF5_DESCR',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF6_CD','Deposit Class Code',19,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF6_DESCR','Deposit Class',20,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF7_CD','Sa Status Flag Code',21,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF7_DESCR','Sa Status',22,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF8_CD','Debt Class Code',23,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF8_DESCR','Debt Class',24,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF9_CD','Customer Category',25,1)
INSERT INTO [columnDefinition] VALUES(9,'UDF9_DESCR',NULL,26,1)
INSERT INTO [columnDefinition] VALUES(9,'EFF_END_DTTM','Effective End Date',27,1)
INSERT INTO [columnDefinition] VALUES(9,'EFF_START_DTTM','Effective Star tDate',28,1)
INSERT INTO [columnDefinition] VALUES(9,'JOB_NBR',NULL,29,1)

--SQI
INSERT INTO [columnDefinition] VALUES(10,'SQI_KEY',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(10,'DATA_LOAD_DTTM',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(10,'DATA_SOURCE_IND',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(10,'SQI_CD','Sqi Code',4,1)
INSERT INTO [columnDefinition] VALUES(10,'SQI_DESCR','Sqi Description',5,1)
INSERT INTO [columnDefinition] VALUES(10,'UPDATE_DTTM',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(10,'JOB_NBR',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(10,'hashKey',NULL,8,1)

--UOM
INSERT INTO [columnDefinition] VALUES(11,'UOM_KEY',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(11,'DATA_LOAD_DTTM',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(11,'DATA_SOURCE_IND',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(11,'MEAS_PEAK_IND',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(11,'UOM_CD','UomCode',5,1)
INSERT INTO [columnDefinition] VALUES(11,'UOM_DESCR','Uom',6,1)
INSERT INTO [columnDefinition] VALUES(11,'UPDATE_DTTM',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(11,'JOB_NBR',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(11,'hashKey',NULL,9,1)

--CF_BILLED_USAGE
INSERT INTO [columnDefinition] VALUES(12,'BILLED_USAGE_KEY','Billed Usage Key',1,1)
INSERT INTO [columnDefinition] VALUES(12,'BILLED_QTY','Billed Quantity',2,1)
INSERT INTO [columnDefinition] VALUES(12,'CALC_AMT','Calculated Amount',3,1)
INSERT INTO [columnDefinition] VALUES(12,'CURRENCY_CD','Currency Code',4,1)
INSERT INTO [columnDefinition] VALUES(12,'DATA_SOURCE_IND','No Of Units',5,1)
INSERT INTO [columnDefinition] VALUES(12,'FACT_CNT','Fact Count',6,1)
INSERT INTO [columnDefinition] VALUES(12,'INIT_QTY','Initial Quantity',7,1)
INSERT INTO [columnDefinition] VALUES(12,'PER_KEY',8,1)
INSERT INTO [columnDefinition] VALUES(12,'SEG_DAYS','Billed Segment Days',9,1)
INSERT INTO [columnDefinition] VALUES(12,'SRC_BILL_ID','Bill ID',10,1)
INSERT INTO [columnDefinition] VALUES(12,'SRC_BSEG_ID','Billed Segment ID',11,1)
INSERT INTO [columnDefinition] VALUES(12,'SRC_FT_ID','Financial Transaction ID',12,1)
INSERT INTO [columnDefinition] VALUES(12,'UDM1',13,1)
INSERT INTO [columnDefinition] VALUES(12,'UDM2',14,1)
INSERT INTO [columnDefinition] VALUES(12,'UDM3',15,1)
INSERT INTO [columnDefinition] VALUES(12,'ACCT_KEY',16,1)
INSERT INTO [columnDefinition] VALUES(12,'ADDR_KEY',17,1)
INSERT INTO [columnDefinition] VALUES(12,'BUSG_UDD1_KEY',18,1)
INSERT INTO [columnDefinition] VALUES(12,'BUSG_UDD2_KEY',19,1)
INSERT INTO [columnDefinition] VALUES(12,'BILL_DATE_KEY',20,1)
INSERT INTO [columnDefinition] VALUES(12,'BSEG_STRT_DATE_KEY',21,1)
INSERT INTO [columnDefinition] VALUES(12,'BSEG_END_DATE_KEY',22,1)
INSERT INTO [columnDefinition] VALUES(12,'FISCAL_CAL_KEY',23,1)
INSERT INTO [columnDefinition] VALUES(12,'PREM_KEY',24,1)
INSERT INTO [columnDefinition] VALUES(12,'RATE_KEY',25,1)
INSERT INTO [columnDefinition] VALUES(12,'SA_KEY',26,1)
INSERT INTO [columnDefinition] VALUES(12,'SQI_KEY',27,1)
INSERT INTO [columnDefinition] VALUES(12,'TOU_KEY',28,1)
INSERT INTO [columnDefinition] VALUES(12,'UOM_KEY',29,1)
INSERT INTO [columnDefinition] VALUES(12,'JOB_NBR',30,1)
INSERT INTO [columnDefinition] VALUES(12,'UDDGEN1','Financial Transaction Creation Date',31,1)
INSERT INTO [columnDefinition] VALUES(12,'UDDGEN2','Financial Transaction Type',32,1)
INSERT INTO [columnDefinition] VALUES(12,'UDDGEN3','Distribution Code',33,1)
INSERT INTO [columnDefinition] VALUES(12,'UDDGEN_DST','Distribution Code Description',34,1)
INSERT INTO [columnDefinition] VALUES(12,'DATA_LOAD_DTTM',35,1)

--- CDC PRODUCTION

--ACCT
INSERT INTO [columnDefinition] VALUES(13,'ACCT_ID',1,1)
INSERT INTO [columnDefinition] VALUES(13,'BILL_CYC_CD',2,1)
INSERT INTO [columnDefinition] VALUES(13,'SETUP_DT',3,1)
INSERT INTO [columnDefinition] VALUES(13,'CURRENCY_CD',4,1)
INSERT INTO [columnDefinition] VALUES(13,'ACCT_MGMT_GRP_CD',5,1)
INSERT INTO [columnDefinition] VALUES(13,'ALERT_INFO',6,1)
INSERT INTO [columnDefinition] VALUES(13,'BILL_AFTER_DT',7,1)
INSERT INTO [columnDefinition] VALUES(13,'PROTECT_CYC_SW',8,1)
INSERT INTO [columnDefinition] VALUES(13,'CIS_DIVISION',9,1)
INSERT INTO [columnDefinition] VALUES(13,'MAILING_PREM_ID',10,1)
INSERT INTO [columnDefinition] VALUES(13,'PROTECT_PREM_SW',11,1)
INSERT INTO [columnDefinition] VALUES(13,'COLL_CL_CD',12,1)
INSERT INTO [columnDefinition] VALUES(13,'CR_REVIEW_DT',13,1)
INSERT INTO [columnDefinition] VALUES(13,'POSTPONE_CR_RVW_DT',14,1)
INSERT INTO [columnDefinition] VALUES(13,'INT_CR_REVIEW_SW',15,1)
INSERT INTO [columnDefinition] VALUES(13,'CUST_CL_CD',16,1)
INSERT INTO [columnDefinition] VALUES(13,'BILL_PRT_INTERCEPT',17,1)
INSERT INTO [columnDefinition] VALUES(13,'NO_DEP_RVW_SW',18,1)
INSERT INTO [columnDefinition] VALUES(13,'BUD_PLAN_CD',19,1)
INSERT INTO [columnDefinition] VALUES(13,'VERSION',20,1)
INSERT INTO [columnDefinition] VALUES(13,'PROTECT_DIV_SW',21,1)
INSERT INTO [columnDefinition] VALUES(13,'ACCESS_GRP_CD',22,1)
INSERT INTO [columnDefinition] VALUES(13,'ACCT_DATA_AREA',23,1)

-- BSEG
INSERT INTO [columnDefinition] VALUES(14,'BSEG_ID',1,1)
INSERT INTO [columnDefinition] VALUES(14,'BILL_CYC_CD',2,1)
INSERT INTO [columnDefinition] VALUES(14,'WIN_START_DT',3,1)
INSERT INTO [columnDefinition] VALUES(14,'CAN_RSN_CD',4,1)
INSERT INTO [columnDefinition] VALUES(14,'CAN_BSEG_ID',5,1)
INSERT INTO [columnDefinition] VALUES(14,'SA_ID',6,1)
INSERT INTO [columnDefinition] VALUES(14,'BILL_ID',7,1)
INSERT INTO [columnDefinition] VALUES(14,'START_DT',8,1)
INSERT INTO [columnDefinition] VALUES(14,'END_DT',9,1)
INSERT INTO [columnDefinition] VALUES(14,'EST_SW',10,1)
INSERT INTO [columnDefinition] VALUES(14,'CLOSING_BSEG_SW',11,1)
INSERT INTO [columnDefinition] VALUES(14,'SQ_OVERRIDE_SW',12,1)
INSERT INTO [columnDefinition] VALUES(14,'ITEM_OVERRIDE_SW',13,1)
INSERT INTO [columnDefinition] VALUES(14,'PREM_ID',14,1)
INSERT INTO [columnDefinition] VALUES(14,'BSEG_STAT_FLG',15,1)
INSERT INTO [columnDefinition] VALUES(14,'CRE_DTTM',16,1)
INSERT INTO [columnDefinition] VALUES(14,'STAT_CHG_DTTM',17,1)
INSERT INTO [columnDefinition] VALUES(14,'REBILL_SEG_ID',18,1)
INSERT INTO [columnDefinition] VALUES(14,'VERSION',19,1)
INSERT INTO [columnDefinition] VALUES(14,'MASTER_BSEG_ID',20,1)
INSERT INTO [columnDefinition] VALUES(14,'QUOTE_DTL_ID',21,1)
INSERT INTO [columnDefinition] VALUES(14,'BILL_SCNR_ID',22,1)
INSERT INTO [columnDefinition] VALUES(14,'MDM_START_DTTM',23,1)
INSERT INTO [columnDefinition] VALUES(14,'MDM_END_DTTM',24,1)
INSERT INTO [columnDefinition] VALUES(14,'BSEG_DATA_AREA',25,1)
INSERT INTO [columnDefinition] VALUES(14,'ILM_DT',26,1)
INSERT INTO [columnDefinition] VALUES(14,'ILM_ARCH_SW',27,1)

--BSEG CALC
INSERT INTO [columnDefinition] VALUES(15,'BSEG_ID',1,1)
INSERT INTO [columnDefinition] VALUES(15,'HEADER_SEQ',2,1)
INSERT INTO [columnDefinition] VALUES(15,'START_DT',3,1)
INSERT INTO [columnDefinition] VALUES(15,'CURRENCY_CD',4,1)
INSERT INTO [columnDefinition] VALUES(15,'END_DT',5,1)
INSERT INTO [columnDefinition] VALUES(15,'RS_CD',6,1)
INSERT INTO [columnDefinition] VALUES(15,'EFFDT',7,1)
INSERT INTO [columnDefinition] VALUES(15,'BILLABLE_CHG_ID',8,1)
INSERT INTO [columnDefinition] VALUES(15,'CALC_AMT',9,1)
INSERT INTO [columnDefinition] VALUES(15,'DESCR_ON_BILL',10,1)
INSERT INTO [columnDefinition] VALUES(15,'VERSION',11,1)

--BSEG CALC LN
INSERT INTO [columnDefinition] VALUES(16,'BSEG_ID',1,1)
INSERT INTO [columnDefinition] VALUES(16,'HEADER_SEQ',2,1)
INSERT INTO [columnDefinition] VALUES(16,'SEQNO',3,1)
INSERT INTO [columnDefinition] VALUES(16,'CHAR_TYPE_CD',4,1)
INSERT INTO [columnDefinition] VALUES(16,'CURRENCY_CD',5,1)
INSERT INTO [columnDefinition] VALUES(16,'CHAR_VAL',6,1)
INSERT INTO [columnDefinition] VALUES(16,'DST_ID',7,1)
INSERT INTO [columnDefinition] VALUES(16,'UOM_CD',8,1)
INSERT INTO [columnDefinition] VALUES(16,'TOU_CD',9,1)
INSERT INTO [columnDefinition] VALUES(16,'RC_SEQ',10,1)
INSERT INTO [columnDefinition] VALUES(16,'PRT_SW',11,1)
INSERT INTO [columnDefinition] VALUES(16,'APP_IN_SUMM_SW',12,1)
INSERT INTO [columnDefinition] VALUES(16,'CALC_AMT',13,1)
INSERT INTO [columnDefinition] VALUES(16,'EXEMPT_AMT',14,1)
INSERT INTO [columnDefinition] VALUES(16,'BASE_AMT',15,1)
INSERT INTO [columnDefinition] VALUES(16,'SQI_CD',16,1)
INSERT INTO [columnDefinition] VALUES(16,'BILL_SQ',17,1)
INSERT INTO [columnDefinition] VALUES(16,'MSR_PEAK_QTY_SW',18,1)
INSERT INTO [columnDefinition] VALUES(16,'DESCR_ON_BILL',19,1)
INSERT INTO [columnDefinition] VALUES(16,'VERSION',20,1)
INSERT INTO [columnDefinition] VALUES(16,'AUDIT_CALC_AMT',21,1)
INSERT INTO [columnDefinition] VALUES(16,'CALC_GRP_CD',22,1)
INSERT INTO [columnDefinition] VALUES(16,'CALC_RULE_CD',23,1)

--BSEG SQ
INSERT INTO [columnDefinition] VALUES(17,'BSEG_ID',1,1)
INSERT INTO [columnDefinition] VALUES(17,'UOM_CD',2,1)
INSERT INTO [columnDefinition] VALUES(17,'TOU_CD',3,1)
INSERT INTO [columnDefinition] VALUES(17,'SQI_CD',4,1)
INSERT INTO [columnDefinition] VALUES(17,'INIT_SQ',5,1)
INSERT INTO [columnDefinition] VALUES(17,'BILL_SQ',6,1)
INSERT INTO [columnDefinition] VALUES(17,'VERSION',7,1)

--CAL PERIOD
INSERT INTO [columnDefinition] VALUES(18,'CALENDAR_ID',1,1)
INSERT INTO [columnDefinition] VALUES(18,'FISCAL_YEAR',2,1)
INSERT INTO [columnDefinition] VALUES(18,'ACCOUNTING_PERIOD',3,1)
INSERT INTO [columnDefinition] VALUES(18,'BEGIN_DT',4,1)
INSERT INTO [columnDefinition] VALUES(18,'END_DT',5,1)
INSERT INTO [columnDefinition] VALUES(18,'OPEN_FROM_DT',6,1)
INSERT INTO [columnDefinition] VALUES(18,'OPEN_TO_DT',7,1)
INSERT INTO [columnDefinition] VALUES(18,'VERSION',8,1)

--FT
INSERT INTO [columnDefinition] VALUES(19,'FT_ID',1,1)
INSERT INTO [columnDefinition] VALUES(19,'SIBLING_ID',2,1)
INSERT INTO [columnDefinition] VALUES(19,'SA_ID',3,1)
INSERT INTO [columnDefinition] VALUES(19,'PARENT_ID',4,1)
INSERT INTO [columnDefinition] VALUES(19,'GL_DIVISION',5,1)
INSERT INTO [columnDefinition] VALUES(19,'CIS_DIVISION',6,1)
INSERT INTO [columnDefinition] VALUES(19,'CURRENCY_CD',7,1)
INSERT INTO [columnDefinition] VALUES(19,'FT_TYPE_FLG',8,1)
INSERT INTO [columnDefinition] VALUES(19,'CUR_AMT',9,1)
INSERT INTO [columnDefinition] VALUES(19,'TOT_AMT',10,1)
INSERT INTO [columnDefinition] VALUES(19,'CRE_DTTM',11,1)
INSERT INTO [columnDefinition] VALUES(19,'FREEZE_SW',12,1)
INSERT INTO [columnDefinition] VALUES(19,'FREEZE_USER_ID',13,1)
INSERT INTO [columnDefinition] VALUES(19,'FREEZE_DTTM',14,1)
INSERT INTO [columnDefinition] VALUES(19,'ARS_DT',15,1)
INSERT INTO [columnDefinition] VALUES(19,'CORRECTION_SW',16,1)
INSERT INTO [columnDefinition] VALUES(19,'REDUNDANT_SW',17,1)
INSERT INTO [columnDefinition] VALUES(19,'NEW_DEBIT_SW',18,1)
INSERT INTO [columnDefinition] VALUES(19,'SHOW_ON_BILL_SW',19,1)
INSERT INTO [columnDefinition] VALUES(19,'NOT_IN_ARS_SW',20,1)
INSERT INTO [columnDefinition] VALUES(19,'BILL_ID',21,1)
INSERT INTO [columnDefinition] VALUES(19,'ACCOUNTING_DT',22,1)
INSERT INTO [columnDefinition] VALUES(19,'VERSION',23,1)
INSERT INTO [columnDefinition] VALUES(19,'XFERRED_OUT_SW',24,1)
INSERT INTO [columnDefinition] VALUES(19,'XFER_TO_GL_DT',25,1)
INSERT INTO [columnDefinition] VALUES(19,'GL_DISTRIB_STATUS',26,1)
INSERT INTO [columnDefinition] VALUES(19,'SCHED_DISTRIB_DT',27,1)
INSERT INTO [columnDefinition] VALUES(19,'BAL_CTL_GRP_ID',28,1)
INSERT INTO [columnDefinition] VALUES(19,'MATCH_EVT_ID',29,1)

INSERT INTO [columnDefinition] VALUES(20,'PER_ID',1,1)
INSERT INTO [columnDefinition] VALUES(20,'LANGUAGE_CD',2,1)
INSERT INTO [columnDefinition] VALUES(20,'PER_OR_BUS_FLG',3,1)
INSERT INTO [columnDefinition] VALUES(20,'LS_SL_FLG',4,1)
INSERT INTO [columnDefinition] VALUES(20,'LS_SL_DESCR',5,1)
INSERT INTO [columnDefinition] VALUES(20,'EMAILID',6,1)
INSERT INTO [columnDefinition] VALUES(20,'OVRD_MAIL_NAME1',7,1)
INSERT INTO [columnDefinition] VALUES(20,'OVRD_MAIL_NAME2',8,1)
INSERT INTO [columnDefinition] VALUES(20,'OVRD_MAIL_NAME3',9,1)
INSERT INTO [columnDefinition] VALUES(20,'ADDRESS1',10,1)
INSERT INTO [columnDefinition] VALUES(20,'ADDRESS2',11,1)
INSERT INTO [columnDefinition] VALUES(20,'ADDRESS3',12,1)
INSERT INTO [columnDefinition] VALUES(20,'ADDRESS4',13,1)
INSERT INTO [columnDefinition] VALUES(20,'CITY',14,1)
INSERT INTO [columnDefinition] VALUES(20,'NUM1',15,1)
INSERT INTO [columnDefinition] VALUES(20,'NUM2',16,1)
INSERT INTO [columnDefinition] VALUES(20,'COUNTY',17,1)
INSERT INTO [columnDefinition] VALUES(20,'POSTAL',18,1)
INSERT INTO [columnDefinition] VALUES(20,'HOUSE_TYPE',19,1)
INSERT INTO [columnDefinition] VALUES(20,'GEO_CODE',20,1)
INSERT INTO [columnDefinition] VALUES(20,'IN_CITY_LIMIT',21,1)
INSERT INTO [columnDefinition] VALUES(20,'STATE',22,1)
INSERT INTO [columnDefinition] VALUES(20,'COUNTRY',23,1)
INSERT INTO [columnDefinition] VALUES(20,'VERSION',24,1)
INSERT INTO [columnDefinition] VALUES(20,'RECV_MKTG_INFO_FLG',25,1)
INSERT INTO [columnDefinition] VALUES(20,'WEB_PASSWD',26,1)
INSERT INTO [columnDefinition] VALUES(20,'WEB_PWD_HINT_FLG',27,1)
INSERT INTO [columnDefinition] VALUES(20,'WEB_PASSWD_ANS',28,1)
INSERT INTO [columnDefinition] VALUES(20,'PER_DATA_AREA',29,1)

--PREM
INSERT INTO [columnDefinition] VALUES(21,'PREM_ID',1,1)
INSERT INTO [columnDefinition] VALUES(21,'PREM_TYPE_CD',2,1)
INSERT INTO [columnDefinition] VALUES(21,'CIS_DIVISION',3,1)
INSERT INTO [columnDefinition] VALUES(21,'LL_ID',4,1)
INSERT INTO [columnDefinition] VALUES(21,'KEY_SW',5,1)
INSERT INTO [columnDefinition] VALUES(21,'KEY_ID',6,1)
INSERT INTO [columnDefinition] VALUES(21,'OK_TO_ENTER_SW',7,1)
INSERT INTO [columnDefinition] VALUES(21,'MR_INSTR_CD',8,1)
INSERT INTO [columnDefinition] VALUES(21,'MR_INSTR_DETAILS',9,1)
INSERT INTO [columnDefinition] VALUES(21,'MR_WARN_CD',10,1)
INSERT INTO [columnDefinition] VALUES(21,'TREND_AREA_CD',11,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS1',12,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS2',13,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS3',14,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS4',15,1)
INSERT INTO [columnDefinition] VALUES(21,'MAIL_ADDR_SW',16,1)
INSERT INTO [columnDefinition] VALUES(21,'CITY',17,1)
INSERT INTO [columnDefinition] VALUES(21,'NUM1',18,1)
INSERT INTO [columnDefinition] VALUES(21,'NUM2',19,1)
INSERT INTO [columnDefinition] VALUES(21,'COUNTY',20,1)
INSERT INTO [columnDefinition] VALUES(21,'POSTAL',21,1)
INSERT INTO [columnDefinition] VALUES(21,'HOUSE_TYPE',22,1)
INSERT INTO [columnDefinition] VALUES(21,'GEO_CODE',23,1)
INSERT INTO [columnDefinition] VALUES(21,'IN_CITY_LIMIT',24,1)
INSERT INTO [columnDefinition] VALUES(21,'STATE',25,1)
INSERT INTO [columnDefinition] VALUES(21,'COUNTRY',26,1)
INSERT INTO [columnDefinition] VALUES(21,'VERSION',27,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS1_UPR',28,1)
INSERT INTO [columnDefinition] VALUES(21,'CITY_UPR',29,1)
INSERT INTO [columnDefinition] VALUES(21,'TIME_ZONE_CD',30,1)
INSERT INTO [columnDefinition] VALUES(21,'LS_SL_FLG',31,1)
INSERT INTO [columnDefinition] VALUES(21,'LS_SL_DESCR',32,1)
INSERT INTO [columnDefinition] VALUES(21,'PRNT_PREM_ID',33,1)
INSERT INTO [columnDefinition] VALUES(21,'PREM_DATA_AREA',34,1)

--RS
INSERT INTO [columnDefinition] VALUES(22,'RS_CD',1,1)
INSERT INTO [columnDefinition] VALUES(22,'CURRENCY_CD',2,1)
INSERT INTO [columnDefinition] VALUES(22,'SVC_TYPE_CD',3,1)
INSERT INTO [columnDefinition] VALUES(22,'FREQ_CD',4,1)
INSERT INTO [columnDefinition] VALUES(22,'ALLOW_EST_SW',5,1)
INSERT INTO [columnDefinition] VALUES(22,'VERSION',6,1)
INSERT INTO [columnDefinition] VALUES(22,'NO_PRO_REF_FLG',7,1)
INSERT INTO [columnDefinition] VALUES(22,'ALLOW_PRO_SW',8,1)
INSERT INTO [columnDefinition] VALUES(22,'RS_TYPE_FLG',9,1)
INSERT INTO [columnDefinition] VALUES(22,'RS_VERSION_FLG',10,1)

--SA
INSERT INTO [columnDefinition] VALUES(23,'SA_ID',1,1)
INSERT INTO [columnDefinition] VALUES(23,'PROP_DCL_RSN_CD',2,1)
INSERT INTO [columnDefinition] VALUES(23,'PROP_SA_ID',3,1)
INSERT INTO [columnDefinition] VALUES(23,'CIS_DIVISION',4,1)
INSERT INTO [columnDefinition] VALUES(23,'SA_TYPE_CD',5,1)
INSERT INTO [columnDefinition] VALUES(23,'START_OPT_CD',6,1)
INSERT INTO [columnDefinition] VALUES(23,'START_DT',7,1)
INSERT INTO [columnDefinition] VALUES(23,'SA_STATUS_FLG',8,1)
INSERT INTO [columnDefinition] VALUES(23,'ACCT_ID',9,1)
INSERT INTO [columnDefinition] VALUES(23,'END_DT',10,1)
INSERT INTO [columnDefinition] VALUES(23,'OLD_ACCT_ID',11,1)
INSERT INTO [columnDefinition] VALUES(23,'CUST_READ_FLG',12,1)
INSERT INTO [columnDefinition] VALUES(23,'ALLOW_EST_SW',13,1)
INSERT INTO [columnDefinition] VALUES(23,'SIC_CD',14,1)
INSERT INTO [columnDefinition] VALUES(23,'CHAR_PREM_ID',15,1)
INSERT INTO [columnDefinition] VALUES(23,'TOT_TO_BILL_AMT',16,1)
INSERT INTO [columnDefinition] VALUES(23,'CURRENCY_CD',17,1)
INSERT INTO [columnDefinition] VALUES(23,'VERSION',18,1)
INSERT INTO [columnDefinition] VALUES(23,'SA_REL_ID',19,1)
INSERT INTO [columnDefinition] VALUES(23,'STRT_RSN_FLG',20,1)
INSERT INTO [columnDefinition] VALUES(23,'STOP_RSN_FLG',21,1)
INSERT INTO [columnDefinition] VALUES(23,'STRT_REQED_BY',22,1)
INSERT INTO [columnDefinition] VALUES(23,'STOP_REQED_BY',23,1)
INSERT INTO [columnDefinition] VALUES(23,'HIGH_BILL_AMT',24,1)
INSERT INTO [columnDefinition] VALUES(23,'INT_CALC_DT',25,1)
INSERT INTO [columnDefinition] VALUES(23,'CIAC_REVIEW_DT',26,1)
INSERT INTO [columnDefinition] VALUES(23,'BUS_ACTIVITY_DESC',27,1)
INSERT INTO [columnDefinition] VALUES(23,'IB_SA_CUTOFF_TM',28,1)
INSERT INTO [columnDefinition] VALUES(23,'IB_BASE_TM_DAY_FLG',29,1)
INSERT INTO [columnDefinition] VALUES(23,'ENRL_ID',30,1)
INSERT INTO [columnDefinition] VALUES(23,'SPECIAL_USAGE_FLG',31,1)
INSERT INTO [columnDefinition] VALUES(23,'PROP_SA_STAT_FLG',32,1)
INSERT INTO [columnDefinition] VALUES(23,'NBR_PYMNT_PERIODS',33,1)
INSERT INTO [columnDefinition] VALUES(23,'NB_RULE_CD',34,1)
INSERT INTO [columnDefinition] VALUES(23,'EXPIRE_DT',35,1)
INSERT INTO [columnDefinition] VALUES(23,'RENEWAL_DT',36,1)
INSERT INTO [columnDefinition] VALUES(23,'NB_APAY_FLG',37,1)
INSERT INTO [columnDefinition] VALUES(23,'SA_DATA_AREA',38,1)

--SQI
INSERT INTO [columnDefinition] VALUES(24,'SQI_CD',1,1)
INSERT INTO [columnDefinition] VALUES(24,'VERSION',2,1)
INSERT INTO [columnDefinition] VALUES(24,'DECIMAL_POSITIONS',3,1)

--UOM
INSERT INTO [columnDefinition] VALUES(25,'UOM_CD',1,1)
INSERT INTO [columnDefinition] VALUES(25,'SVC_TYPE_CD',2,1)
INSERT INTO [columnDefinition] VALUES(25,'ALLOWED_ON_REG_SW',3,1)
INSERT INTO [columnDefinition] VALUES(25,'MSR_PEAK_QTY_SW',4,1)
INSERT INTO [columnDefinition] VALUES(25,'DECIMAL_POSITIONS',5,1)
INSERT INTO [columnDefinition] VALUES(25,'VERSION',6,1)

--vwBillDate
INSERT INTO [columnDefinition] VALUES(26,'BillDate',1,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDATE_KEY',2,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDate_YYYY-MM',3,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDateKey',4,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDateNumeric_YYYY-MM',5,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayNumberinMonth',6,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayNumberinweek',7,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayNumberinyear',8,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayofWeek',9,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayofWeekCode',10,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDaysinMonth',11,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDaysinQuarter',12,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDaysInYear',13,1)
INSERT INTO [columnDefinition] VALUES(26,'BillMonth',14,1)
INSERT INTO [columnDefinition] VALUES(26,'BillMonthCode',15,1)
INSERT INTO [columnDefinition] VALUES(26,'BillMonthEndDate',16,1)
INSERT INTO [columnDefinition] VALUES(26,'BillMonthNumber',17,1)
INSERT INTO [columnDefinition] VALUES(26,'BillQuarter',18,1)
INSERT INTO [columnDefinition] VALUES(26,'BillQuarterCode',19,1)
INSERT INTO [columnDefinition] VALUES(26,'BillQuarterEndDate',20,1)
INSERT INTO [columnDefinition] VALUES(26,'BillQuarterNumber',21,1)
INSERT INTO [columnDefinition] VALUES(26,'BillSeason',22,1)
INSERT INTO [columnDefinition] VALUES(26,'BillWeekEndDate',23,1)
INSERT INTO [columnDefinition] VALUES(26,'BillWeekNumber',24,1)
INSERT INTO [columnDefinition] VALUES(26,'BillWorkDayCode',25,1)
INSERT INTO [columnDefinition] VALUES(26,'BillWorkDayIndicator',26,1)
INSERT INTO [columnDefinition] VALUES(26,'BillYear',27,1)
INSERT INTO [columnDefinition] VALUES(26,'BillYearEndDate',28,1)

INSERT INTO [State] VALUES ('Warning','Test with some warnings',1)
INSERT INTO [State] VALUES ('Failed','Test with errors, need atention',1)
INSERT INTO [State] VALUES ('OK!','Test Successfully completed',1)

INSERT INTO Test VALUES (204,469,219,'Bills Generated On Weekend','Check that no bills generated on weekend',1,0,1,0,1)
INSERT INTO Test VALUES (215,465,219,'Bills Generated On Wrong Fiscal Year','Check that no bills generated on a wrong fiscal year',1,0,1,0,1)
INSERT INTO Test VALUES (194,194,219,'Count Distinct Bills Against The Historical Maximun','Contrast the historical maximum with the current count to detect errors', 0,1,1,0,1)

INSERT INTO Test VALUES (3,220,219,'Distinct Accounts','Compare count of Distinct ACCT between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (3,220,219,'New Accounts','Compare count of New ACCT between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (3,220,219,'Updated Accounts','Compare count of Updated ACCT between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (3,220,219,'Maximun Historical Accounts','Compare count of Distinct ACCT between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (3,220,219,'Statistical Average Accounts','Compare count of Distinct ACCT between CCB and DTWH',0,0,0,0,1)

INSERT INTO Test VALUES (78,348,219,'Distinct Persons','Compare count of Distinct PER between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (78,348,219,'New Persons','Compare count of New PER between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (78,348,219,'Updated Persons','Compare count of Updated PER between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (78,348,219,'Maximun Historical Persons','Compare count of Distinct PER between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (78,348,219,'Statistical Average Persons','Compare count of Distinct PER between CCB and DTWH',0,0,0,0,1)

INSERT INTO Test VALUES (96,377,219,'Distinct Premises','Compare count of Distinct PREM between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (96,377,219,'New Premises','Compare count of New PREM between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (96,377,219,'Updated Premises','Compare count of Updated PREM between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (96,377,219,'Maximun Historical Premises','Compare count of Distinct PREM between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (96,377,219,'Statistical Average Premises','Compare count of Distinct PREM between CCB and DTWH',0,0,0,0,1)

INSERT INTO Test VALUES (141,421,219,'Distinct Service Agreements','Compare count of Distinct SA between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (141,421,219,'New Service Agreements','Compare count of SA PREM between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (141,421,219,'Updated Service Agreements','Compare count of Updated SA between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (141,421,219,'Maximun Historical Service Agreements','Compare count of Distinct SA between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (141,421,219,'Statistical Average Service Agreements','Compare count of Distinct SA between CCB and DTWH',0,0,0,0,1)

INSERT INTO Test VALUES (196,319,219,'Distinct FT','Compare count of Distinct FT between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (196,319,219,'New FT','Compare count of FT between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (196,319,219,'Updated FT','Compare count of Updated FT between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (196,319,219,'Maximun Historical FT','Compare count of Distinct FT between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (196,319,219,'Statistical Average FT','Compare count of Distinct FT between CCB and DTWH',0,0,0,0,1)

INSERT INTO Test VALUES (196,319,219,'Distinct SQI','Compare count of Distinct SQI between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (196,319,219,'New SQI','Compare count of SQI between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (196,319,219,'Updated SQI','Compare count of Updated SQI between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (196,319,219,'Maximun Historical SQI','Compare count of Distinct SQI between CCB and DTWH',0,0,0,0,1)
INSERT INTO Test VALUES (196,319,219,'Statistical Average SQI','Compare count of Distinct SQI between CCB and DTWH',0,0,0,0,1) 




