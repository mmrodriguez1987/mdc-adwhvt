CREATE TABLE Entity 
(
	entityID BIGINT IDENTITY(1,1) PRIMARY KEY,
	databaseID BIGINT FOREIGN KEY REFERENCES [Database](databaseID),
	entityFormalName VARCHAR(50),
	entityShortName VARCHAR(50),
	typeEntity CHAR(1),
	isActive BIT
)