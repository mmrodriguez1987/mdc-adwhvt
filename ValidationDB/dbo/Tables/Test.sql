CREATE TABLE Test
(
	testID BIGINT IDENTITY(1,1) PRIMARY KEY,
	firstColumnID BIGINT FOREIGN KEY REFERENCES [Column](columnID),	
	secondColumnID BIGINT FOREIGN KEY REFERENCES [Column](columnID),	
	thirdColumnID BIGINT FOREIGN KEY REFERENCES [Column](columnID),
	testName VARCHAR(500),
	testDescription VARCHAR(500),	
	isBusinessRule BIT,
	isStatisticalRule BIT,
	isUnilateral BIT,
	isETLRule BIT,
	isActive BIT
)