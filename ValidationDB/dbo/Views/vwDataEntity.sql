CREATE VIEW vwDataEntity
AS
SELECT  DB.dbName,t.entityID, T.entityFormalName,T.entityShortName,T.typeEntity,C.columnID,  C.columnName,C.ordinalPosition FROM 
[Database] DB 
INNER JOIN Entity T ON DB.databaseID=T.databaseID
INNER JOIN [Column] C ON T.entityID=C.entityID