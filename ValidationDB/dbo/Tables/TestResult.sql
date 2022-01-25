CREATE TABLE TestResult
(
	testResultID BIGINT IDENTITY(1,1) PRIMARY KEY,
	stateID BIGINT FOREIGN KEY REFERENCES [State](stateID),
	testID BIGINT FOREIGN KEY REFERENCES Test(testID),	
	result VARCHAR(3000) ,
	iniEvalDate datetime ,
	endEvalDate datetime ,
	countCDC BIGINT ,
	countDTW BIGINT ,
	queryCDC VARCHAR(3000),
	queryDTW VARCHAR(3000),
	effectDate DATETIME,
	isActive BIT
)