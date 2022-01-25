CREATE TABLE [State]
(
	stateID BIGINT IDENTITY(1,1) PRIMARY KEY,
	stateName VARCHAR(50) ,
	stateMessage VARCHAR(300) ,
	isActive BIT
)