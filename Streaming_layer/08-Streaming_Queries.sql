-- Query #1

SELECT productId, SUM(quantity) AS total_quantity_sold
FROM qcompany.log_Events
WHERE eventType = 'purchase'
GROUP BY productId
ORDER BY total_quantity_sold DESC
LIMIT 10

-- Query #2


SELECT productId, COUNT(*) AS views
FROM qcompany.log_Events
WHERE eventType = 'productView'
GROUP BY productId
ORDER BY views DESC
LIMIT 10