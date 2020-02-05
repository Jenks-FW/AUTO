SELECT *
FROM Staging.dbo.indx_granularity_item
ORDER BY id DESC

SELECT TOP 1000 *
FROM Warehouse.dbo.LMC_Auto

SELECT TOP 1000 *
FROM Warehouse.dbo.LMC_Cap
WHERE Country = 'USA'


SELECT TOP 1000 *
FROM Staging.dbo.indx_index_data
WHERE index_id = 134
SELECT TOP 1000 *
FROM Staging.dbo.indx_granularity_item
WHERE granularity_level_id IN (1, 19, 39)
AND [granularity1] = 'USA'
ORDER BY granularity1

SELECT TOP 1000 * --sum(c.[PRODUCTION VOLUME])
FROM Warehouse.dbo.LMC_Auto AS a
INNER JOIN Warehouse.dbo.LMC_Cap AS c
ON a.PLANT = c.Plant_Name 
AND a.MANUFACTURER = c.Manufacturer 
AND a.COUNTRY = c.Country
AND a.[YEAR] = c.[Year]
WHERE c.Country = 'USA' 


SELECT DISTINCT Plant_Name, City, [Location] 
FROM Warehouse.dbo.LMC_Cap
LEFT JOIN Warehouse.dbo.KMA
ON
WHERE Country = 'USA'



SELECT *
FROM Warehouse.dbo.indx_index_data_BRAD_TEST

 
SELECT DATA_TIMESTAMP, 
       SUM(DATA_VALUE) AS [Total USA Production]
FROM staging.dbo.indx_index_data D
     INNER JOIN staging.dbo.indx_granularity_item G ON G.ID = D.granularity_item_id
                                                       AND G.granularity_level_id = 3
WHERE index_id = 134
GROUP BY data_timestamp
ORDER BY data_timestamp;

-- Auto.USA ticker vs sum(Auto)
SELECT * FROM Staging.dbo.indx_index_data WHERE index_id = 134 AND granularity_item_id = 1 ORDER BY data_timestamp DESC
SELECT DATA_TIMESTAMP, 
       SUM(DATA_VALUE)
FROM staging.dbo.indx_index_data D
     INNER JOIN staging.dbo.indx_granularity_item G ON G.ID = D.granularity_item_id
                                                       AND G.granularity_level_id = 3
WHERE index_id = 134
GROUP BY data_timestamp
ORDER BY data_timestamp DESC;

