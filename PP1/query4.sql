SELECT ItemID
FROM items
WHERE Currently = (SELECT MAX(Currently) FROM items);

