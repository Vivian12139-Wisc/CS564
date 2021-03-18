WITH user AS (
	SELECT UserID
	FROM Seller
	WHERE Location = 'New York'
	UNION
	SELECT UserID
	FROM Bidder
	WHERE Location = 'New York'
)
SELECT COUNT(DISTINCT UserID)
FROM user;