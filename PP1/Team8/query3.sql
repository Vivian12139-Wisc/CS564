#Find the number of auctions belonging to exactly four categories.
WITH auction AS(
	SELECT COUNT(*) as numofAuctions
	FROM Category
	GROUP BY ItemID
)
SELECT COUNT(numOfAuctions)
FROM auction
WHERE numOfAuctions = 4;