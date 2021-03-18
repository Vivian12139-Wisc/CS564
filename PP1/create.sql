drop table if exists items;
create table items(
ItemID INTEGER NOT NULL,
Name TEXT,
Currently FLOAT,
Buy_Price FLOAT,
First_Bid FLOAT,
Number_of_Bids INTEGER,
Started TEXT,
Ends TEXT,
Description TEXT,
SellerID TEXT,
PRIMARY KEY(ItemID),
FOREIGN KEY(SellerID) REFERENCES Seller(UserID)
);

drop table if exists Seller;
create table Seller(
UserID TEXT NOT NULL,
Location TEXT,
Country TEXT,
Rating INTEGER,
PRIMARY KEY(UserID)
); 

drop table if exists Category;
create table Category(
ItemID INTEGER NOT NULL,
CategoryName TEXT,
PRIMARY KEY(ItemID, CategoryName),
FOREIGN KEY(ItemID) REFERENCES items(ItemID)
);

drop table if exists Bids;
create table Bids(
ItemID INTEGER NOT NULL,
BidderUserID TEXT NOT NULL,
Time TEXT,
Amount FLOAT,
PRIMARY KEY(ItemID, BidderUserID, Time),
FOREIGN KEY(ItemID) REFERENCES items(ItemID),
FOREIGN KEY(BidderUserID) REFERENCES Bidder(UserID)
);

drop table if exists Bidder;
create table Bidder(
UserID TEXT NOT NULL,
Location TEXT,
Country TEXT,
Rating INTEGER,
PRIMARY KEY(UserID)
);