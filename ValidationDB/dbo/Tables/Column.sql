CREATE TABLE [Column]
(
	columnID BIGINT IDENTITY(1,1) PRIMARY KEY,
	entityID BIGINT FOREIGN KEY REFERENCES Entity(entityID),
	columnName VARCHAR(50),
	ordinalPosition INT,
	isActive BIT
)