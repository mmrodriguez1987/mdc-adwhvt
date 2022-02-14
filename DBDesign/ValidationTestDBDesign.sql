
--create database [valdat-m2c-dtwh-dev]

--use [valdat-m2c-dtwh-prod] 
/*DELETE OBJECTS IF EXIST*/
IF EXISTS (SELECT * FROM sys.all_views WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'vwHistoricalInformation') DROP VIEW vwHistoricalInformation
IF EXISTS (SELECT * FROM sys.all_views WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'vwTestInformationSummary') DROP VIEW vwTestInformationSummary
IF EXISTS (SELECT * FROM sys.all_views WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'vwTestInformation') DROP VIEW vwTestInformation
IF EXISTS (SELECT * FROM sys.all_views WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'vwDataEntity') DROP VIEW vwDataEntity
IF EXISTS (SELECT * FROM sys.all_views WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'vwTestResult') DROP VIEW vwTestResult

IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'ResultStat') DROP TABLE ResultStat
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'IndicatorType') DROP TABLE IndicatorType
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'ResultDetail') DROP TABLE ResultDetail
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'Result') DROP TABLE Result
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'ResultType') DROP TABLE ResultType

IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'TestParameter') DROP TABLE TestParameter
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'Test') DROP TABLE Test
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'TestType') DROP TABLE TestType
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'State') DROP TABLE [State]
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'ColumnDefinition') DROP TABLE [ColumnDefinition]
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'Entity') DROP TABLE Entity
IF EXISTS (SELECT * FROM sys.tables WHERE SCHEMA_NAME(schema_id) LIKE 'dbo' AND name like 'Source') DROP TABLE [Source]



GO
/*CREATE TABLES*/
CREATE TABLE [Source] 
(
	sourceID SMALLINT IDENTITY(1,1) PRIMARY KEY,
	sourceName VARCHAR(50),
	sourceDescription VARCHAR(500),
	active BIT
)
go
CREATE TABLE Entity 
(
	entityID BIGINT IDENTITY(1,1) PRIMARY KEY,
	sourceID SMALLINT FOREIGN KEY REFERENCES [Source](sourceID),
	entityQualifyName VARCHAR(50),
	entityShortName VARCHAR(50),
	typeEntity CHAR(1),
	active BIT
)

go
CREATE TABLE ColumnDefinition
(
	columnID BIGINT IDENTITY(1,1) PRIMARY KEY,
	entityID BIGINT FOREIGN KEY REFERENCES Entity(entityID),
	columnName VARCHAR(50),
	[description] VARCHAR(300),
	ordinalPosition INT,
	active BIT
)
go
CREATE TABLE [State]
(
	stateID SMALLINT IDENTITY(1,1) PRIMARY KEY,
	stateName VARCHAR(50) ,
	stateMessage VARCHAR(300) ,
	active BIT
)

GO
CREATE TABLE TestType 
(
	testTypeID SMALLINT IDENTITY(1,1) PRIMARY KEY,	
	typeDescription VARCHAR(100)
)

GO
CREATE TABLE Test
(
	testID BIGINT IDENTITY(1,1) PRIMARY KEY,
	testTypeID SMALLINT FOREIGN KEY REFERENCES TestType(testTypeID),
	testName VARCHAR(500),
	testDescription VARCHAR(500),		
	query VARCHAR(3000), 
	active BIT
)

GO


CREATE TABLE TestParameter
(
	paramID BIGINT IDENTITY(1,1) PRIMARY KEY,
	testID BIGINT FOREIGN KEY REFERENCES Test(testID),
	columnID BIGINT FOREIGN KEY REFERENCES ColumnDefinition(columnID),
)

GO
CREATE TABLE ResultType
(
	resultTypeID SMALLINT IDENTITY(1,1) PRIMARY KEY,
	acronym VARCHAR(50),
	resultTypeDesc VARCHAR(100)
)
GO
CREATE TABLE Result
(
	resultID BIGINT IDENTITY(1,1) PRIMARY KEY,
	stateID SMALLINT FOREIGN KEY REFERENCES [State](stateID),
	testID BIGINT FOREIGN KEY REFERENCES Test(testID),	
	[description] VARCHAR(3000),
	startDate DATETIME,
	endDate DATETIME,	
	testDate DATETIME
)

GO
CREATE TABLE ResultDetail
(
	resultDetailID BIGINT IDENTITY(1,1)  PRIMARY KEY,
	resultID BIGINT FOREIGN KEY REFERENCES Result(ResultID),
	resultTypeID SMALLINT FOREIGN KEY REFERENCES ResultType(resultTypeID),
	[count] FLOAT,
	affectedIDs VARCHAR(5000),	
	affectedDesc VARCHAR(100)
)

GO
CREATE TABLE IndicatorType
(
	indicatorTypeID SMALLINT IDENTITY(1,1) PRIMARY KEY,
	typeDesc VARCHAR(100),
	active BIT
)
CREATE TABLE ResultStat
(                                                                                                                                                              
	resultStatID BIGINT IDENTITY(1,1) PRIMARY KEY,
	columnID BIGINT FOREIGN KEY REFERENCES ColumnDefinition(columnID),
	indicatorTypeID SMALLINT FOREIGN KEY REFERENCES IndicatorType(indicatorTypeID),
	[count] float,
	calculatedDate DATETIME
)



GO

create VIEW vwTestResult
AS
SELECT 
R.resultID, 
S.stateName [State], 
T.testName Test, 
TT.typeDescription [Type], 
RT.acronym,
RD.[count] [Count],
R.[description] [Description], 
R.startDate [Start Date], 
R.endDate [End Date], 
R.testDate
FROM Result R
INNER JOIN [State] S ON R.stateID=S.stateID
INNER JOIN Test T ON T.testID=R.testID
INNER JOIN TestType TT ON TT.testTypeID=T.testTypeID
INNER JOIN ResultDetail RD ON R.resultID=RD.resultID
INNER JOIN ResultType RT on RT.resultTypeID=RD.resultTypeID

Go




CREATE VIEW vwDataEntity
AS
SELECT  DB.sourceName, t.entityID, T.entityQualifyName,T.entityShortName,T.typeEntity,C.columnID,  C.columnName,C.[description] ColDescription, C.ordinalPosition 
FROM [Source] DB 
INNER JOIN Entity T ON DB.sourceID=T.sourceID
INNER JOIN ColumnDefinition C ON T.entityID=C.entityID

GO

CREATE VIEW vwTestInformationSummary
AS
SELECT 
T.testID,
T.testName,
T.testDescription,
T.query,
TT.typeDescription TestType,
STRING_AGG(CONCAT(S.sourceName,':',E.entityShortName,':',CD.columnName),', ') Entity, 
STRING_AGG(CD.columnID,', ') ColumnsID
FROM Test T
INNER JOIN TestType TT ON T.testTypeID=TT.testTypeID
INNER JOIN TestParameter TP ON TP.testID=T.testID
INNER JOIN ColumnDefinition CD ON CD.columnID=TP.columnID
INNER JOIN Entity E ON E.entityID=CD.entityID
INNER JOIN [Source] S ON S.sourceID=E.sourceID
GROUP BY T.testID, T.testDescription, T.query, TT.typeDescription, T.testName

GO

CREATE VIEW vwTestInformation
AS
SELECT 
T.testID,
T.testName,
T.testDescription,
T.query,
TT.typeDescription TestType,
S.sourceName,
E.entityShortName,
CD.columnName columnName,
CD.columnID
FROM Test T
INNER JOIN TestType TT ON T.testTypeID=TT.testTypeID
INNER JOIN TestParameter TP ON TP.testID=T.testID
INNER JOIN ColumnDefinition CD ON CD.columnID=TP.columnID
INNER JOIN Entity E ON E.entityID=CD.entityID
INNER JOIN [Source] S ON S.sourceID=E.sourceID
GO

create  VIEW vwHistoricalInformation
AS
SELECT resultStatID, H.columnID, H.[count] Count, C.columnName [Column], E.entityShortName Entity, h.indicatorTypeID, i.typeDesc TypeIndicator,h.calculatedDate
FROM ResultStat H
INNER JOIN ColumnDefinition C ON H.columnID=C.columnID
INNER JOIN Entity E ON E.entityID = C.entityID
INNER JOIN IndicatorType I ON I.indicatorTypeID=H.indicatorTypeID

GO

INSERT INTO [Source] VALUES('NA', 'NA',1)											--1
INSERT INTO [Source] VALUES('cdcProd','Change Data Capture from CCBProd',1)		--2
INSERT INTO [Source] VALUES('dw-ttdp', 'Datawarehouse',1)							--3
									--3

INSERT INTO IndicatorType VALUES('Rows Number',1)
INSERT INTO IndicatorType VALUES('New',1)
INSERT INTO IndicatorType VALUES('Updated',1)
INSERT INTO IndicatorType VALUES('Max Value',1)
INSERT INTO IndicatorType VALUES('Min Value',1)
INSERT INTO IndicatorType VALUES('Weekly Average (5 Business Day)',1)

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

INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_ACCT_CT','CI_ACCT','T',1)						--13
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_BSEG_CT','CI_BSEG','T',1)						--14
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_BSEG_CALC_CT','CI_BSEG_CALC','T',1)			--15
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_BSEG_CALC_LN_CT','CI_CALC_LN','T',1)			--16
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_BSEG_SQ_CT','CI_BSEG_SQ','T',1)				--17
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_CAL_PERIOD_CT','CI_CAL_PERIOD','T',1)			--18
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_FT_CT','CI_FT','T',1)							--19
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_PER_CT','CI_PER','T',1)						--20
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_PREM_CT','CI_PREM','T',1)						--21
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_RS_CT','CI_RS','T',1)							--22
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_SA_CT','CI_SA','T',1)							--23
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_SQI_CT','CI_SQI','T',1)						--24
INSERT INTO Entity VALUES (2,'cdc.CISADM_CI_UOM_CT','CI_UOM','T',1)						--25
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
INSERT INTO [columnDefinition] VALUES(12,'PER_KEY',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(12,'SEG_DAYS','Billed Segment Days',9,1)
INSERT INTO [columnDefinition] VALUES(12,'SRC_BILL_ID','Bill ID',10,1)
INSERT INTO [columnDefinition] VALUES(12,'SRC_BSEG_ID','Billed Segment ID',11,1)
INSERT INTO [columnDefinition] VALUES(12,'SRC_FT_ID','Financial Transaction ID',12,1)
INSERT INTO [columnDefinition] VALUES(12,'UDM1','GL Extract Date Actual Key',13,1)
INSERT INTO [columnDefinition] VALUES(12,'UDM2','Posted Date Key',14,1)
INSERT INTO [columnDefinition] VALUES(12,'UDM3','Accounting Date key',15,1)
INSERT INTO [columnDefinition] VALUES(12,'ACCT_KEY',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(12,'ADDR_KEY',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(12,'BUSG_UDD1_KEY',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(12,'BUSG_UDD2_KEY',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(12,'BILL_DATE_KEY',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(12,'BSEG_STRT_DATE_KEY',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(12,'BSEG_END_DATE_KEY',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(12,'FISCAL_CAL_KEY',NULL,23,1)
INSERT INTO [columnDefinition] VALUES(12,'PREM_KEY',NULL,24,1)
INSERT INTO [columnDefinition] VALUES(12,'RATE_KEY',NULL,25,1)
INSERT INTO [columnDefinition] VALUES(12,'SA_KEY',NULL,26,1)
INSERT INTO [columnDefinition] VALUES(12,'SQI_KEY',NULL,27,1)
INSERT INTO [columnDefinition] VALUES(12,'TOU_KEY',NULL,28,1)
INSERT INTO [columnDefinition] VALUES(12,'UOM_KEY',NULL,29,1)
INSERT INTO [columnDefinition] VALUES(12,'JOB_NBR',NULL,30,1)
INSERT INTO [columnDefinition] VALUES(12,'UDDGEN1','Financial Transaction Creation Date',31,1)
INSERT INTO [columnDefinition] VALUES(12,'UDDGEN2','Financial Transaction Type',32,1)
INSERT INTO [columnDefinition] VALUES(12,'UDDGEN3','Distribution Code',33,1)
INSERT INTO [columnDefinition] VALUES(12,'UDDGEN_DST','Distribution Code Description',34,1)
INSERT INTO [columnDefinition] VALUES(12,'DATA_LOAD_DTTM',NULL,35,1)

--- CDC PRODUCTION

--ACCT
INSERT INTO [columnDefinition] VALUES(13,'ACCT_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(13,'BILL_CYC_CD',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(13,'SETUP_DT',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(13,'CURRENCY_CD',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(13,'ACCT_MGMT_GRP_CD',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(13,'ALERT_INFO',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(13,'BILL_AFTER_DT',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(13,'PROTECT_CYC_SW',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(13,'CIS_DIVISION',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(13,'MAILING_PREM_ID',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(13,'PROTECT_PREM_SW',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(13,'COLL_CL_CD',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(13,'CR_REVIEW_DT',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(13,'POSTPONE_CR_RVW_DT',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(13,'INT_CR_REVIEW_SW',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(13,'CUST_CL_CD',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(13,'BILL_PRT_INTERCEPT',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(13,'NO_DEP_RVW_SW',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(13,'BUD_PLAN_CD',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(13,'VERSION',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(13,'PROTECT_DIV_SW',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(13,'ACCESS_GRP_CD',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(13,'ACCT_DATA_AREA',NULL,23,1)

-- BSEG
INSERT INTO [columnDefinition] VALUES(14,'BSEG_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(14,'BILL_CYC_CD',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(14,'WIN_START_DT',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(14,'CAN_RSN_CD',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(14,'CAN_BSEG_ID',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(14,'SA_ID',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(14,'BILL_ID',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(14,'START_DT',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(14,'END_DT',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(14,'EST_SW',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(14,'CLOSING_BSEG_SW',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(14,'SQ_OVERRIDE_SW',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(14,'ITEM_OVERRIDE_SW',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(14,'PREM_ID',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(14,'BSEG_STAT_FLG',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(14,'CRE_DTTM',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(14,'STAT_CHG_DTTM',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(14,'REBILL_SEG_ID',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(14,'VERSION',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(14,'MASTER_BSEG_ID',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(14,'QUOTE_DTL_ID',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(14,'BILL_SCNR_ID',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(14,'MDM_START_DTTM',NULL,23,1)
INSERT INTO [columnDefinition] VALUES(14,'MDM_END_DTTM',NULL,24,1)
INSERT INTO [columnDefinition] VALUES(14,'BSEG_DATA_AREA',NULL,25,1)
INSERT INTO [columnDefinition] VALUES(14,'ILM_DT',NULL,26,1)
INSERT INTO [columnDefinition] VALUES(14,'ILM_ARCH_SW',NULL,27,1)

--BSEG CALC
INSERT INTO [columnDefinition] VALUES(15,'BSEG_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(15,'HEADER_SEQ',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(15,'START_DT',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(15,'CURRENCY_CD',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(15,'END_DT',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(15,'RS_CD',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(15,'EFFDT',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(15,'BILLABLE_CHG_ID',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(15,'CALC_AMT',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(15,'DESCR_ON_BILL',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(15,'VERSION',NULL,11,1)

--BSEG CALC LN
INSERT INTO [columnDefinition] VALUES(16,'BSEG_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(16,'HEADER_SEQ',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(16,'SEQNO',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(16,'CHAR_TYPE_CD',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(16,'CURRENCY_CD',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(16,'CHAR_VAL',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(16,'DST_ID',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(16,'UOM_CD',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(16,'TOU_CD',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(16,'RC_SEQ',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(16,'PRT_SW',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(16,'APP_IN_SUMM_SW',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(16,'CALC_AMT',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(16,'EXEMPT_AMT',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(16,'BASE_AMT',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(16,'SQI_CD',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(16,'BILL_SQ',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(16,'MSR_PEAK_QTY_SW',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(16,'DESCR_ON_BILL',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(16,'VERSION',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(16,'AUDIT_CALC_AMT',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(16,'CALC_GRP_CD',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(16,'CALC_RULE_CD',NULL,23,1)

--BSEG SQ
INSERT INTO [columnDefinition] VALUES(17,'BSEG_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(17,'UOM_CD',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(17,'TOU_CD',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(17,'SQI_CD',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(17,'INIT_SQ',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(17,'BILL_SQ',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(17,'VERSION',NULL,7,1)

--CAL PERIOD
INSERT INTO [columnDefinition] VALUES(18,'CALENDAR_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(18,'FISCAL_YEAR',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(18,'ACCOUNTING_PERIOD',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(18,'BEGIN_DT',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(18,'END_DT',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(18,'OPEN_FROM_DT',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(18,'OPEN_TO_DT',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(18,'VERSION',NULL,8,1)

--FT
INSERT INTO [columnDefinition] VALUES(19,'FT_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(19,'SIBLING_ID',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(19,'SA_ID',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(19,'PARENT_ID',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(19,'GL_DIVISION',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(19,'CIS_DIVISION',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(19,'CURRENCY_CD',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(19,'FT_TYPE_FLG',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(19,'CUR_AMT',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(19,'TOT_AMT',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(19,'CRE_DTTM',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(19,'FREEZE_SW',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(19,'FREEZE_USER_ID',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(19,'FREEZE_DTTM',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(19,'ARS_DT',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(19,'CORRECTION_SW',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(19,'REDUNDANT_SW',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(19,'NEW_DEBIT_SW',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(19,'SHOW_ON_BILL_SW',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(19,'NOT_IN_ARS_SW',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(19,'BILL_ID',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(19,'ACCOUNTING_DT',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(19,'VERSION',NULL,23,1)
INSERT INTO [columnDefinition] VALUES(19,'XFERRED_OUT_SW',NULL,24,1)
INSERT INTO [columnDefinition] VALUES(19,'XFER_TO_GL_DT',NULL,25,1)
INSERT INTO [columnDefinition] VALUES(19,'GL_DISTRIB_STATUS',NULL,26,1)
INSERT INTO [columnDefinition] VALUES(19,'SCHED_DISTRIB_DT',NULL,27,1)
INSERT INTO [columnDefinition] VALUES(19,'BAL_CTL_GRP_ID',NULL,28,1)
INSERT INTO [columnDefinition] VALUES(19,'MATCH_EVT_ID',NULL,29,1)

INSERT INTO [columnDefinition] VALUES(20,'PER_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(20,'LANGUAGE_CD',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(20,'PER_OR_BUS_FLG',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(20,'LS_SL_FLG',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(20,'LS_SL_DESCR',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(20,'EMAILID',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(20,'OVRD_MAIL_NAME1',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(20,'OVRD_MAIL_NAME2',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(20,'OVRD_MAIL_NAME3',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(20,'ADDRESS1',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(20,'ADDRESS2',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(20,'ADDRESS3',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(20,'ADDRESS4',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(20,'CITY',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(20,'NUM1',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(20,'NUM2',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(20,'COUNTY',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(20,'POSTAL',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(20,'HOUSE_TYPE',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(20,'GEO_CODE',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(20,'IN_CITY_LIMIT',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(20,'STATE',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(20,'COUNTRY',NULL,23,1)
INSERT INTO [columnDefinition] VALUES(20,'VERSION',NULL,24,1)
INSERT INTO [columnDefinition] VALUES(20,'RECV_MKTG_INFO_FLG',NULL,25,1)
INSERT INTO [columnDefinition] VALUES(20,'WEB_PASSWD',NULL,26,1)
INSERT INTO [columnDefinition] VALUES(20,'WEB_PWD_HINT_FLG',NULL,27,1)
INSERT INTO [columnDefinition] VALUES(20,'WEB_PASSWD_ANS',NULL,28,1)
INSERT INTO [columnDefinition] VALUES(20,'PER_DATA_AREA',NULL,29,1)

--PREM
INSERT INTO [columnDefinition] VALUES(21,'PREM_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(21,'PREM_TYPE_CD',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(21,'CIS_DIVISION',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(21,'LL_ID',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(21,'KEY_SW',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(21,'KEY_ID',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(21,'OK_TO_ENTER_SW',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(21,'MR_INSTR_CD',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(21,'MR_INSTR_DETAILS',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(21,'MR_WARN_CD',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(21,'TREND_AREA_CD',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS1',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS2',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS3',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS4',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(21,'MAIL_ADDR_SW',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(21,'CITY',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(21,'NUM1',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(21,'NUM2',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(21,'COUNTY',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(21,'POSTAL',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(21,'HOUSE_TYPE',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(21,'GEO_CODE',NULL,23,1)
INSERT INTO [columnDefinition] VALUES(21,'IN_CITY_LIMIT',NULL,24,1)
INSERT INTO [columnDefinition] VALUES(21,'STATE',NULL,25,1)
INSERT INTO [columnDefinition] VALUES(21,'COUNTRY',NULL,26,1)
INSERT INTO [columnDefinition] VALUES(21,'VERSION',NULL,27,1)
INSERT INTO [columnDefinition] VALUES(21,'ADDRESS1_UPR',NULL,28,1)
INSERT INTO [columnDefinition] VALUES(21,'CITY_UPR',NULL,29,1)
INSERT INTO [columnDefinition] VALUES(21,'TIME_ZONE_CD',NULL,30,1)
INSERT INTO [columnDefinition] VALUES(21,'LS_SL_FLG',NULL,31,1)
INSERT INTO [columnDefinition] VALUES(21,'LS_SL_DESCR',NULL,32,1)
INSERT INTO [columnDefinition] VALUES(21,'PRNT_PREM_ID',NULL,33,1)
INSERT INTO [columnDefinition] VALUES(21,'PREM_DATA_AREA',NULL,34,1)

--RS
INSERT INTO [columnDefinition] VALUES(22,'RS_CD',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(22,'CURRENCY_CD',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(22,'SVC_TYPE_CD',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(22,'FREQ_CD',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(22,'ALLOW_EST_SW',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(22,'VERSION',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(22,'NO_PRO_REF_FLG',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(22,'ALLOW_PRO_SW',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(22,'RS_TYPE_FLG',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(22,'RS_VERSION_FLG',NULL,10,1)

--SA
INSERT INTO [columnDefinition] VALUES(23,'SA_ID',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(23,'PROP_DCL_RSN_CD',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(23,'PROP_SA_ID',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(23,'CIS_DIVISION',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(23,'SA_TYPE_CD',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(23,'START_OPT_CD',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(23,'START_DT',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(23,'SA_STATUS_FLG',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(23,'ACCT_ID',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(23,'END_DT',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(23,'OLD_ACCT_ID',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(23,'CUST_READ_FLG',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(23,'ALLOW_EST_SW',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(23,'SIC_CD',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(23,'CHAR_PREM_ID',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(23,'TOT_TO_BILL_AMT',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(23,'CURRENCY_CD',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(23,'VERSION',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(23,'SA_REL_ID',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(23,'STRT_RSN_FLG',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(23,'STOP_RSN_FLG',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(23,'STRT_REQED_BY',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(23,'STOP_REQED_BY',NULL,23,1)
INSERT INTO [columnDefinition] VALUES(23,'HIGH_BILL_AMT',NULL,24,1)
INSERT INTO [columnDefinition] VALUES(23,'INT_CALC_DT',NULL,25,1)
INSERT INTO [columnDefinition] VALUES(23,'CIAC_REVIEW_DT',NULL,26,1)
INSERT INTO [columnDefinition] VALUES(23,'BUS_ACTIVITY_DESC',NULL,27,1)
INSERT INTO [columnDefinition] VALUES(23,'IB_SA_CUTOFF_TM',NULL,28,1)
INSERT INTO [columnDefinition] VALUES(23,'IB_BASE_TM_DAY_FLG',NULL,29,1)
INSERT INTO [columnDefinition] VALUES(23,'ENRL_ID',NULL,30,1)
INSERT INTO [columnDefinition] VALUES(23,'SPECIAL_USAGE_FLG',NULL,31,1)
INSERT INTO [columnDefinition] VALUES(23,'PROP_SA_STAT_FLG',NULL,32,1)
INSERT INTO [columnDefinition] VALUES(23,'NBR_PYMNT_PERIODS',NULL,33,1)
INSERT INTO [columnDefinition] VALUES(23,'NB_RULE_CD',NULL,34,1)
INSERT INTO [columnDefinition] VALUES(23,'EXPIRE_DT',NULL,35,1)
INSERT INTO [columnDefinition] VALUES(23,'RENEWAL_DT',NULL,36,1)
INSERT INTO [columnDefinition] VALUES(23,'NB_APAY_FLG',NULL,37,1)
INSERT INTO [columnDefinition] VALUES(23,'SA_DATA_AREA',NULL,38,1)

--SQI
INSERT INTO [columnDefinition] VALUES(24,'SQI_CD',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(24,'VERSION',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(24,'DECIMAL_POSITIONS',NULL,3,1)

--UOM
INSERT INTO [columnDefinition] VALUES(25,'UOM_CD',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(25,'SVC_TYPE_CD',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(25,'ALLOWED_ON_REG_SW',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(25,'MSR_PEAK_QTY_SW',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(25,'DECIMAL_POSITIONS',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(25,'VERSION',NULL,6,1)

--vwBillDate
INSERT INTO [columnDefinition] VALUES(26,'BillDate',NULL,1,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDATE_KEY',NULL,2,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDate_YYYY-MM',NULL,3,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDateKey',NULL,4,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDateNumeric_YYYY-MM',NULL,5,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayNumberinMonth',NULL,6,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayNumberinweek',NULL,7,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayNumberinyear',NULL,8,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayofWeek',NULL,9,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDayofWeekCode',NULL,10,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDaysinMonth',NULL,11,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDaysinQuarter',NULL,12,1)
INSERT INTO [columnDefinition] VALUES(26,'BillDaysInYear',NULL,13,1)
INSERT INTO [columnDefinition] VALUES(26,'BillMonth',NULL,14,1)
INSERT INTO [columnDefinition] VALUES(26,'BillMonthCode',NULL,15,1)
INSERT INTO [columnDefinition] VALUES(26,'BillMonthEndDate',NULL,16,1)
INSERT INTO [columnDefinition] VALUES(26,'BillMonthNumber',NULL,17,1)
INSERT INTO [columnDefinition] VALUES(26,'BillQuarter',NULL,18,1)
INSERT INTO [columnDefinition] VALUES(26,'BillQuarterCode',NULL,19,1)
INSERT INTO [columnDefinition] VALUES(26,'BillQuarterEndDate',NULL,20,1)
INSERT INTO [columnDefinition] VALUES(26,'BillQuarterNumber',NULL,21,1)
INSERT INTO [columnDefinition] VALUES(26,'BillSeason',NULL,22,1)
INSERT INTO [columnDefinition] VALUES(26,'BillWeekEndDate',NULL,23,1)
INSERT INTO [columnDefinition] VALUES(26,'BillWeekNumber',NULL,24,1)
INSERT INTO [columnDefinition] VALUES(26,'BillWorkDayCode',NULL,25,1)
INSERT INTO [columnDefinition] VALUES(26,'BillWorkDayIndicator',NULL,26,1)
INSERT INTO [columnDefinition] VALUES(26,'BillYear',NULL,27,1)
INSERT INTO [columnDefinition] VALUES(26,'BillYearEndDate',NULL,28,1)

INSERT INTO [State] VALUES ('Warning','Test with some warnings',1)
INSERT INTO [State] VALUES ('Failed','Test with errors, need atention',1)
INSERT INTO [State] VALUES ('OK!','Test Successfully completed',1)

INSERT INTO TestType VALUES ('Statistical') --1
INSERT INTO TestType VALUES ('Business')  --2
INSERT INTO TestType VALUES ('Unit')  --3
INSERT INTO TestType VALUES ('ETL')  --4

INSERT INTO ResultType VALUES('CDC Count','Count CCB reflected on CDC')
INSERT INTO ResultType VALUES('DTWH Count','Count on Data Warehouse')
INSERT INTO ResultType VALUES('Aver Count CDC','Average Count calculated on CDC')
INSERT INTO ResultType VALUES('Max Hist Count','Maximun Historical Count')


INSERT INTO Test VALUES (2,'Bills Generated On Weekend','Check that no bills generated on weekend','SELECT * FROM (SELECT B.BILL_DATE_KEY, CASE WHEN (B.UDDGEN1 BETWEEN C.StartDate AND C.EndDate) THEN 1 ELSE 0 END AS validFiscalYear FROM dwadm2.CF_BILLED_USAGE B INNER JOIN dwadm2.vw_CD_FISCAL_CAL C ON B.FISCAL_CAL_KEY=C.FISCAL_CAL_KEY WHERE (B.DATA_LOAD_DTTM BETWEEN @startDate AND @endDate)) T',1)							--testID=1
INSERT INTO TestParameter VALUES (1,204)
INSERT INTO TestParameter VALUES (1,469)
INSERT INTO TestParameter VALUES (1,219)

INSERT INTO Test VALUES (2,'Bills Generated On Wrong Fiscal Year','Check that no bills generated on a wrong fiscal year','',1)	--testID=2
INSERT INTO TestParameter VALUES (2,215)
INSERT INTO TestParameter VALUES (2,469)
INSERT INTO TestParameter VALUES (2,219)

INSERT INTO Test VALUES (1,'Count Distinct Bills Against The Historical Maximun','Contrast the historical maximum with the current count to detect errors','',1) --testID=3
INSERT INTO TestParameter VALUES (3,194)
INSERT INTO TestParameter VALUES (3,219)

INSERT INTO Test VALUES (3,'Distinct Accounts','Compare Distinct ACCT count between CCB and DTWH','',1)							--testID=4
INSERT INTO TestParameter VALUES (4,3)
INSERT INTO TestParameter VALUES (4,220)
INSERT INTO TestParameter VALUES (4,4)

INSERT INTO Test VALUES (3,'New Accounts','Compare New ACCT count between CCB and DTWH','',1)									--testID=5
INSERT INTO TestParameter VALUES (5,220)
INSERT INTO TestParameter VALUES (5,3)
INSERT INTO TestParameter VALUES (5,4)

INSERT INTO Test VALUES (3,'Updated Accounts','Compare count of Updated ACCT between CCB and DTWH','',1)							--testID=6
INSERT INTO TestParameter VALUES (6,220)
INSERT INTO TestParameter VALUES (6,3)
INSERT INTO TestParameter VALUES (6,4)

INSERT INTO Test VALUES (1,'Maximun Historical Accounts','Compare Distinct ACCT count with Historical Max Value count','',1)				--testID=7
INSERT INTO TestParameter VALUES (7,220)
INSERT INTO TestParameter VALUES (7,3)
INSERT INTO TestParameter VALUES (7,4)

INSERT INTO Test VALUES (1,'Statistical Average Accounts','Compare Average ACCT count with the last week average','',1)				--testID=8
INSERT INTO TestParameter VALUES (8,220)
INSERT INTO TestParameter VALUES (8,3)
INSERT INTO TestParameter VALUES (8,4)

INSERT INTO Test VALUES (3,'Distinct Persons','Compare count of Distinct PER between CCB and DTWH','',1)							--testID=9
INSERT INTO TestParameter VALUES (9,78)
INSERT INTO TestParameter VALUES (9,348)
INSERT INTO TestParameter VALUES (9,74)

INSERT INTO Test VALUES (3,'New Persons','Compare New PER count between CCB and DTWH','',1)									--testID=10
INSERT INTO TestParameter VALUES (10,78)
INSERT INTO TestParameter VALUES (10,348)
INSERT INTO TestParameter VALUES (10,74)

INSERT INTO Test VALUES (3,'Updated Persons','Compare count of Updated PER between CCB and DTWH','',1)							--testID=11
INSERT INTO TestParameter VALUES (11,78)
INSERT INTO TestParameter VALUES (11,348)
INSERT INTO TestParameter VALUES (11,74)

INSERT INTO Test VALUES (1,'Maximun Historical Persons','Compare Distinct PER count with Historical Max Value count','',1)				--testID=12
INSERT INTO TestParameter VALUES (12,78)
INSERT INTO TestParameter VALUES (12,348)
INSERT INTO TestParameter VALUES (12,74)

INSERT INTO Test VALUES (1,'Statistical Average Persons','Compare Average PER count with the last week average','',1)				--testID=13
INSERT INTO TestParameter VALUES (13,78)
INSERT INTO TestParameter VALUES (13,348)
INSERT INTO TestParameter VALUES (13,74)

INSERT INTO Test VALUES (3,'Distinct Premises','Compare count of Distinct PREM between CCB and DTWH','',1)						--testID=14
INSERT INTO TestParameter VALUES (14,96)
INSERT INTO TestParameter VALUES (14,377)
INSERT INTO TestParameter VALUES (14,94)

INSERT INTO Test VALUES (3,'New Premises','Compare New PREM count between CCB and DTWH','',1)									--testID=15
INSERT INTO TestParameter VALUES (15,96)
INSERT INTO TestParameter VALUES (15,377)
INSERT INTO TestParameter VALUES (15,94)

INSERT INTO Test VALUES (3,'Updated Premises','Compare count of Updated PREM between CCB and DTWH','',1)							--testID=16
INSERT INTO TestParameter VALUES (16,96)
INSERT INTO TestParameter VALUES (16,377)
INSERT INTO TestParameter VALUES (16,94)

INSERT INTO Test VALUES (1,'Maximun Historical Premises','Compare Distinct PREM count with Historical Max Value count','',1)				--testID=17
INSERT INTO TestParameter VALUES (17,96)
INSERT INTO TestParameter VALUES (17,377)
INSERT INTO TestParameter VALUES (17,94)

INSERT INTO Test VALUES (1,'Statistical Average Premises','Compare Average PREM count with the last week average','',1)				--testID=18
INSERT INTO TestParameter VALUES (18,96)
INSERT INTO TestParameter VALUES (18,377)
INSERT INTO TestParameter VALUES (18,94)

INSERT INTO Test VALUES (3,'Distinct Service Agreements','Compare count of Distinct SA between CCB and DTWH','',1)				--testID=19
INSERT INTO TestParameter VALUES (19,141)
INSERT INTO TestParameter VALUES (19,421)
INSERT INTO TestParameter VALUES (19,140)

INSERT INTO Test VALUES (3,'New Service Agreements','Compare New SA count between CCB and DTWH','',1)						--testID=20
INSERT INTO TestParameter VALUES (20,141)
INSERT INTO TestParameter VALUES (20,421)
INSERT INTO TestParameter VALUES (20,140)

INSERT INTO Test VALUES (3,'Updated Service Agreements','Compare count of Updated SA between CCB and DTWH','',1)					--testID=21
INSERT INTO TestParameter VALUES (21,141)
INSERT INTO TestParameter VALUES (21,421)
INSERT INTO TestParameter VALUES (21,140)

INSERT INTO Test VALUES (1,'Maximun Historical Service Agreements','Compare Distinct SA count with Historical Max Value count','',1)		--testID=22
INSERT INTO TestParameter VALUES (22,141)
INSERT INTO TestParameter VALUES (22,421)
INSERT INTO TestParameter VALUES (22,140)

INSERT INTO Test VALUES (1,'Statistical Average Service Agreements','Compare Average SA count with the last week average','',1)	--testID=23
INSERT INTO TestParameter VALUES (23,141)
INSERT INTO TestParameter VALUES (23,421)
INSERT INTO TestParameter VALUES (23,219)

INSERT INTO Test VALUES (3,'Distinct FT','Compare Distinct FT count between CCB and DTWH','',1)								--testID=24
INSERT INTO TestParameter VALUES (24,196)
INSERT INTO TestParameter VALUES (24,319)
INSERT INTO TestParameter VALUES (24,219)

INSERT INTO Test VALUES (3,'New FT','Compare New FT count between CCB and DTWH','',1)												--testID=25
INSERT INTO TestParameter VALUES (25,196)
INSERT INTO TestParameter VALUES (25,319)
INSERT INTO TestParameter VALUES (25,219)

INSERT INTO Test VALUES (3,'Updated FT','Compare count of Updated FT between CCB and DTWH','',1)									--testID=26
INSERT INTO TestParameter VALUES (26,196)
INSERT INTO TestParameter VALUES (26,319)
INSERT INTO TestParameter VALUES (26,219)

INSERT INTO Test VALUES (1,'Maximun Historical FT','Compare Distinct FT count with Historical Max Value count','',1)						--testID=27
INSERT INTO TestParameter VALUES (27,196)
INSERT INTO TestParameter VALUES (27,319)
INSERT INTO TestParameter VALUES (27,219)

INSERT INTO Test VALUES (1,'Statistical Average FT','Compare Average FT count with the last week average','',1)					--testID=28
INSERT INTO TestParameter VALUES (28,196)
INSERT INTO TestParameter VALUES (28,319)
INSERT INTO TestParameter VALUES (28,219)

INSERT INTO Test VALUES (3,'Distinct SQI','Compare Distinct SQI count between CCB and DTWH','',1)								--testID=29
INSERT INTO TestParameter VALUES (29,196)
INSERT INTO TestParameter VALUES (29,319)
INSERT INTO TestParameter VALUES (29,169)

INSERT INTO Test VALUES (3,'New SQI','Compare New SQI count between CCB and DTWH','',1)											--testID=30
INSERT INTO TestParameter VALUES (30,196)
INSERT INTO TestParameter VALUES (30,319)
INSERT INTO TestParameter VALUES (30,219)

INSERT INTO Test VALUES (3,'Updated SQI','Compare count of Updated SQI between CCB and DTWH','',1)								--testID=31
INSERT INTO TestParameter VALUES (31,196)
INSERT INTO TestParameter VALUES (31,319)
INSERT INTO TestParameter VALUES (31,169)

INSERT INTO Test VALUES (1,'Maximun Historical SQI','Compare Distinct SQI count with Historical Max Value count','',1)					--testID=32
INSERT INTO TestParameter VALUES (32,196)
INSERT INTO TestParameter VALUES (32,319)
INSERT INTO TestParameter VALUES (32,169)

INSERT INTO Test VALUES (1,'Statistical Average SQI','Compare Average SQI count with the last week average','',1)					--testID=33
INSERT INTO TestParameter VALUES (33,196)
INSERT INTO TestParameter VALUES (33,319)
INSERT INTO TestParameter VALUES (33,469)

INSERT INTO Test VALUES (3,'Distinct BSEG','Compare Distinct BSEG_ID count between CCB and DTWH','',1)					--testID=34
INSERT INTO TestParameter VALUES (34,195)
INSERT INTO TestParameter VALUES (34,243)
INSERT INTO TestParameter VALUES (34,219)


/*
DELETE FROM ResultDetail
DBCC CHECKIDENT (ResultDetail, RESEED,0)
GO
DELETE FROM Result
DBCC CHECKIDENT (Result, RESEED,0)
GO 
DELETE FROM ResultStat
DBCC CHECKIDENT (ResultStat, RESEED,0)
*/
