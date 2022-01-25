/*CREATE TABLES*/
CREATE TABLE [Database] 
(
	databaseID BIGINT IDENTITY(1,1) PRIMARY KEY,
	dbName VARCHAR(50),
	dbDescription VARCHAR(500),
	isActive BIT
)