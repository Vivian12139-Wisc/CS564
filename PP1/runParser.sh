python my_parser.py ebay_data/items-*.json &&
sort -u sellerTable.dat > seller.dat &&
sort -u itemTable.dat > items.dat &&
sort -u bidderTable.dat > bidder.dat &&
sort -u bidTable.dat > bid.dat &&
sort -u categoryTable.dat > category.dat