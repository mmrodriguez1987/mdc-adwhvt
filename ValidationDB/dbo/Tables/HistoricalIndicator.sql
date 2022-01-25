CREATE TABLE HistoricalIndicator
(                                                                                                                                                              
histIndicatorID BIGINT IDENTITY(1,1) PRIMARY KEY,
coolumnID BIGINT FOREIGN KEY REFERENCES [Column](columnID),
distinctCountVal BIGINT,
newCountVal BIGINT,
updatedCountVal BIGINT,
maxVal BIGINT,
minVal BIGINT,
calculatedDate DATETIME,
isActive BIT
)