#7. Find the number of categories that include at least one item with a bid of more than $100.

WITH Cate AS(
WITH auctions AS (
	SELECT ItemID
	FROM Bids
	WHERE Amount > 100
)
SELECT CategoryName
FROM auctions a, Category c
WHERE a.ItemID = c. ItemID
)
SELECT COUNT(DISTINCT CategoryName)
FROM Cate;