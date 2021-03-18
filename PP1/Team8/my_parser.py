
"""
FILE: skeleton_parser.py
------------------
Author: Firas Abuzaid (fabuzaid@stanford.edu)
Author: Perth Charernwattanagul (puch@stanford.edu)
Modified: 04/21/2014

Skeleton parser for CS564 programming project 1. Has useful imports and
functions for parsing, including:

1) Directory handling -- the parser takes a list of eBay json files
and opens each file inside of a loop. You just need to fill in the rest.
2) Dollar value conversions -- the json files store dollar value amounts in
a string like $3,453.23 -- we provide a function to convert it to a string
like XXXXX.xx.
3) Date/time conversions -- the json files store dates/ times in the form
Mon-DD-YY HH:MM:SS -- we wrote a function (transformDttm) that converts to the
for YYYY-MM-DD HH:MM:SS, which will sort chronologically in SQL.

Your job is to implement the parseJson function, which is invoked on each file by
the main function. We create the initial Python dictionary object of items for
you; the rest is up to you!
Happy parsing!
"""

import sys
from json import loads
from re import sub

columnSeparator = "|"

itemTable = ""
catTable = ""
sellerTable = ""
bidderTable = ""
bidTable = ""

# Dictionary of months used for date transformation
MONTHS = {'Jan':'01','Feb':'02','Mar':'03','Apr':'04','May':'05','Jun':'06',\
        'Jul':'07','Aug':'08','Sep':'09','Oct':'10','Nov':'11','Dec':'12'}

"""
Returns true if a file ends in .json
"""
def isJson(f):
    return len(f) > 5 and f[-5:] == '.json'

"""
Converts month to a number, e.g. 'Dec' to '12'
"""
def transformMonth(mon):
    if mon in MONTHS:
        return MONTHS[mon]
    else:
        return mon

"""
Transforms a timestamp from Mon-DD-YY HH:MM:SS to YYYY-MM-DD HH:MM:SS
"""
def transformDttm(dttm):
    dttm = dttm.strip().split(' ')
    dt = dttm[0].split('-')
    date = '20' + dt[2] + '-'
    date += transformMonth(dt[0]) + '-' + dt[1]
    return date + ' ' + dttm[1]

"""
Transform a dollar value amount from a string like $3,453.23 to XXXXX.xx
"""

def transformDollar(money):
    if money == None or len(money) == 0:
        return money
    return sub(r'[^\d.]', '', money)

"""
Parses a single json file. Currently, there's a loop that iterates over each
item in the data set. Your job is to extend this functionality to create all
of the necessary SQL tables for your database.
"""

            
def parse_items(item):
    line = ""

    if 'ItemID'in item and item['ItemID'] != None:
        item['ItemID']= item['ItemID'].replace('"', '""')
        line += item['ItemID']
        line = line + columnSeparator
    else :
        line = line + 'null'
        line = line + columnSeparator

    if 'Name' in item and item['Name'] != None:
        item['Name']= item['Name'].replace('"', '""')
        line += '\"' + item['Name'] + '\"'
        line += columnSeparator
    else:
        line += 'null'
        line += columnSeparator

    if 'Currently' in item and item['Currently'] != None:
        item['Currently']= item['Currently'].replace('"', '""')
        line += '\"' + transformDollar(item['Currently']) + '\"'
        line += columnSeparator
    else:
        line += 'null'
        line += columnSeparator

    if 'Buy_Price' in item and item['Buy_Price'] != None:
        item['Buy_Price']= item['Buy_Price'].replace('"', '""')
        line += '\"' + transformDollar(item['Buy_Price']) + '\"'
        line += columnSeparator
    else:
        line += 'null'
        line += columnSeparator

    if 'First_Bid' in item and item['First_Bid'] != None:
        item['First_Bid']= item['First_Bid'].replace('"', '""')
        line += '"' + transformDollar(item['First_Bid']) + '"'
        line += columnSeparator
    else:
        line += 'null'
        line += columnSeparator

    if 'Number_of_Bids' in item and item['Number_of_Bids'] != None:
        item['Number_of_Bids']= item['Number_of_Bids'].replace('"', '""')
        line += item['Number_of_Bids']
        line += columnSeparator
    else:
        line += 'null'
        line += columnSeparator

    if 'Started' in item and item['Started'] != None:
        item['Started']= item['Started'].replace('"', '""')
        line += '"' + transformDttm(item['Started']) + '"'
        line += columnSeparator
    else:
        line += 'null'
        line += columnSeparator

    if 'Ends' in item and item['Ends'] != None:
        item['Ends']= item['Ends'].replace('"', '""')
        line += '"' + transformDttm(item['Ends']) + '"'
        line += columnSeparator
    else:
        line += 'null'
        line += columnSeparator

    if 'Description' in item and item['Description'] != None:
        item['Description'] = item['Description'].replace('\"', '""')
        line += '"' + item['Description'] + '"'
        line += columnSeparator
    else:
        line += 'null'
        line += columnSeparator

    if 'Seller' in item and item['Seller'] != None:
        item['Seller']['UserID']= item['Seller']['UserID'].replace('"', '""')
        line += '"' + item['Seller']['UserID'] + '"'

    else:
        line += 'null'
        
    line += '\n'
    return line

def categoryTable(item):
    line = ""
    if 'ItemID' in item and item['ItemID'] != None :
        if('Category' in item and item['Category'] != None):
            i = 0
            for ele in item['Category']:
                line += item['ItemID']
                line += columnSeparator
                item['Category'][i] = item['Category'][i].replace('"', '""')
                line += '"' + item['Category'][i] + '"'
                line += '\n'
                i += 1
        else:
            for i in item['Category']:
                line += item['ItemID']
                line += columnSeparator
                line += 'null'
                line += '\n'
    return line


def sTable(item):
    line = ""
    if 'Seller' in item and item['Seller'] != None:
        line += '"' + item['Seller']['UserID'] + '"'
        line += columnSeparator
        if 'Location' in item and item['Location'] != None:
            item['Location']= item['Location'].replace('"', '""')
            line += '"' + item['Location'] + '"'
        else:
            line += 'null'
        line += columnSeparator
        if 'Country' in item and item['Country'] != None:
            item['Country']= item['Country'].replace('"', '""')
            line += '"' + item['Country'] + '"'
        else:
            line += 'null'
        line += columnSeparator
        if 'Rating' in item['Seller'] and item['Seller']['Rating'] != None: 
            line += item['Seller']['Rating']
        else: 
            line += 'null'
        line += '\n'
    return line


def bTable(item):
    line = ""
    
    if 'Bids' in item and item['Bids'] != None:
        for bidder in item['Bids']:
            #print(bidder)
            bidder['Bid']['Bidder']['UserID']= bidder['Bid']['Bidder']['UserID'].replace('"', '""')
            line += '\"' + bidder['Bid']['Bidder']['UserID'] + '\"'
            line += columnSeparator
            if 'Location' in bidder['Bid']['Bidder'] and bidder['Bid']['Bidder']['Location'] != None:
                bidder['Bid']['Bidder']['Location']= bidder['Bid']['Bidder']['Location'].replace('"', '""')
                line += '"' + bidder['Bid']['Bidder']['Location'] + '"'
            else:
                line += 'null'

            line += columnSeparator

            if 'Country' in bidder['Bid']['Bidder'] and bidder['Bid']['Bidder']['Country'] != None:
                bidder['Bid']['Bidder']['Country']= bidder['Bid']['Bidder']['Country'].replace('"', '""')
                line += '"' + bidder['Bid']['Bidder']['Country'] + '"'
            else:
                line += 'null'

            line += columnSeparator

            if 'Rating' in bidder['Bid']['Bidder'] and bidder['Bid']['Bidder']['Rating'] != None:
                line += bidder['Bid']['Bidder']['Rating']
            else:
                line += 'null'

            line += '\n'
    return line

def parse_bid(item):
    line = "" 
    if 'Bids' in item and item['Bids'] != None:
        for bid_i in item['Bids']:
            line += item['ItemID']
            line += columnSeparator

            line += '"' + bid_i['Bid']['Bidder']['UserID'] + '"'
            line += columnSeparator
            
            if bid_i['Bid']['Time'] != None:

                line += '"' + transformDttm(bid_i['Bid']['Time']) + '"'
            else:
                line += 'null'
            line += columnSeparator
            if bid_i['Bid']['Amount'] != None:
                bid_i['Bid']['Amount']= bid_i['Bid']['Amount'].replace('"', '""')
                line += '"' + transformDollar(bid_i['Bid']['Amount']) + '"'
            else: line += 'null'
            line += '\n'

    return line

def parseJson(json_file):
    global itemTable, catTable, bidderTable, bidTable, sellerTable
    with open(json_file, 'r') as f:
        items = loads(f.read())['Items'] # creates a Python dictionary of Items for the supplied json file

        for item in items:
            """
            TODO: traverse the items dictionary to extract information from the
            given `json_file' and generate the necessary .dat files to generate
            the SQL tables based on your relation design
            """

            itemTable += parse_items(item)
            catTable += categoryTable(item)
            bidderTable += bTable(item)
            sellerTable += sTable(item)
            bidTable += parse_bid(item)

def output():
    f1 = open("itemTable.dat", "a")
    f2 = open("categoryTable.dat", "a")
    f3 = open("bidderTable.dat", "a")
    f4 = open("bidTable.dat", "a")
    f5 = open("sellerTable.dat", "a")
    f1.write(itemTable)
    f2.write(catTable)
    f3.write(bidderTable)
    f4.write(bidTable)
    f5.write(sellerTable)
    f1.close()
    f2.close()
    f3.close()
    f4.close()
    f5.close()

"""
Loops through each json files provided on the command line and passes each file
to the parser
"""
def main(argv):
    if len(argv) < 2:
        print >> sys.stderr, 'Usage: python skeleton_json_parser.py <path to json files>'
        sys.exit(1)
    # loops over all .json files in the argument
    for f in argv[1:]:
        if isJson(f):
            parseJson(f)
            print("Success parsing " + f)
    output()

if __name__ == '__main__':
    main(sys.argv)
