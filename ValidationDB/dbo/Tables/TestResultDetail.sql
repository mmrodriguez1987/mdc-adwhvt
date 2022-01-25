CREATE TABLE TestResultDetail
(
	ID BIGINT IDENTITY(1,1)  PRIMARY KEY,
	testResultID BIGINT FOREIGN KEY REFERENCES TestResult(testResultID),
	affected_key_array VARCHAR(5000) ,
	affected_key_name VARCHAR(100) ,
	dbName VARCHAR(100)
)