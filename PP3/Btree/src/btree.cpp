/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
/**
 * The constructor first checks if the specified index file exists. And index file 
 * name is constructed by concatenating the relational name with the offset of the 
 * attribute over which the index is built.
 * 
 * @param relationName The name of the relation on which to build the index. The 
 * constructor should scan this relation (using FileScan) and insert entries for 
 * all the tuples in this relation into the index. You can insert an entry one-by-one, 
 * i.e., don’t worry about implementing a bottom-up bulkloading index construction 
 * mecha- nism.
 * @param outIndexName The name of the index file; determine this name in the 
 * constructor as shown above, and return the name.
 * @param bufMgrIn The instance of the global buffer manager.
 * @param attrByteOffset The byte offset of the attribute in the tuple on which to 
 * build the index.
 * @param attrType The data type of the attribute we are indexing.
 **/
BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
	bufMgr = bufMgrIn;
	leafOccupancy = INTARRAYLEAFSIZE;
  	nodeOccupancy = INTARRAYNONLEAFSIZE;
	this -> attrByteOffset = attrByteOffset;
	this -> attributeType = attrType;
	scanExecuting = false;
	
	std :: ostringstream idxStr ;
	idxStr << relationName <<  '.' << attrByteOffset ;
	outIndexName = idxStr.str() ; // indexName is the name of the index file
	
	try 
	{
		file = new BlobFile(outIndexName, false); // file exists
		Page *metaPage; //header page
		headerPageNum = file->getFirstPageNo();
		bufMgr->readPage(file, headerPageNum, metaPage);
    	IndexMetaInfo *meta = (IndexMetaInfo *)metaPage;
		rootPageNum = meta->rootPageNo;

		//check if match
		if(relationName != meta->relationName || attrType != meta->attrType 
			|| attrByteOffset != meta->attrByteOffset) 
		{
				throw BadIndexInfoException(outIndexName);
	  	}

		bufMgr->unPinPage(file, headerPageNum, false);

	} catch (FileNotFoundException e) 
	{
		file = new BlobFile(outIndexName, true); //no file exists,then create a file
		Page *metaPage;

		// copy the info to header page
		IndexMetaInfo *meta = (IndexMetaInfo *)metaPage;
		relationName.copy(meta->relationName, 20, 0);
  		meta->attrByteOffset = attrByteOffset;
  		meta->attrType = attrType;

		// create root node
		LeafNodeInt *root;
		bufMgr->allocPage(file, meta->rootPageNo, (Page *&)root);
		root->rightSibPageNo = 0;
		depth = 0;
		
		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, meta->rootPageNo, true);

		FileScan FileScan(relationName, bufMgr);
		try 
		{
			RecordId id;
			while (true)
			{
				FileScan.scanNext(id);
				std::string rstr = FileScan.getRecord();
				const char* record = rstr.c_str();
				int key = *((int*)(record + attrByteOffset));
				insertEntry(&key + attrByteOffset, id);
			}
			
		} catch (EndOfFileException e) {}
	}
	
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) 
{

}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
/**
 * This method is used to begin a “filtered scan” of the index. For example, if 
 * the method is called using arguments (1,GT,100,LTE), then the scan should seek 
 * all entries greater than 1 and less than or equal to 100.
 * 
 * @param lowValParm The low value to be tested.
 * @param lowOpParm The operation to be used in testing the low range. You should 
 * only support GT and GTE here; any- thing else should throw BadOpcodesException. 
 * Note that the Operator enumeration is defined in btree.h.
 * @param highValParm The high value to be tested.
 * @param highOpParm The operation to be used in testing the high range. You should 
 * only support LT and LTE here; anything else should throw BadOpcodesException.
 **/
void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
	if(lowOpParm != GT && lowOpParm != GTE) 
	{
		throw BadOpcodesException();
	}

	if(highOpParm != LT && highOpParm != LTE) 
	{
		throw BadOpcodesException();
	}

	lowValInt = *((int *)lowValParm);
  	highValInt = *((int *)highValParm);
	
	if (lowValInt > highValInt) 
	{
		throw BadScanrangeException();
	}
	
	lowOp = lowOpParm;
	highOp = highOpParm;

	if(scanExecuting) 
	{
		endScan();
	}

	bufMgr->readPage(file, rootPageNum, currentPageData);
	currentPageNum = rootPageNum;

	//if root is a nonleaf
	if(depth != 1) {
		NonLeafNodeInt* currentNode = (NonLeafNodeInt *) currentPageData;
		bool isfound = false;
		while (!isfound)
		{
			currentNode = (NonLeafNodeInt *) currentPageData;
			if(currentNode->level == 1) // found the leaf
			{
				isfound = true;
			}
			PageId numOfNextPage;
      		findNextNonleafNode(currentNode, numOfNextPage, lowValInt);
      		// Unpin
      		bufMgr->unPinPage(file, currentPageNum, false);
      		currentPageNum = numOfNextPage;
      		// read nextPage
      		bufMgr->readPage(file, currentPageNum, currentPageData);
		}
		
	}

	// current node is leaf, find smallest one
	bool found = false;
	while (!found)
	{
		LeafNodeInt* currNode = (LeafNodeInt *) currentPageData;
		if(currNode->ridArray[0].page_number == 0) //check if node is empty
		{
			throw NoSuchKeyFoundException();
		}

		// iterate through the leaf
		for(int i = 0; i < leafOccupancy; i++)
		{
			int key = currNode->keyArray[i];
			// check key if in the range
			if(check_key(key)) {
				found = true;
				nextEntry = i;
				scanExecuting = true;
				break;
			} else if((highOp == LT and key >= highValInt) || (highOp == LTE and key > highValInt))
			{
				throw NoSuchKeyFoundException();
			}

			//if didn't find matching leaf, go to sibpage
			if(i == leafOccupancy -1) 
			{
				bufMgr->unPinPage(file, currentPageNum, false);
				if(currNode->rightSibPageNo == 0)
				{
					throw NoSuchKeyFoundException();
				}
				currentPageNum = currNode->rightSibPageNo;
				bufMgr->readPage(file, currentPageNum, currentPageData);
			}
			
		}

	}
}

/**
 * Helper: find the next non-leaf node
 **/
const void BTreeIndex::findNextNonleafNode(NonLeafNodeInt *currentPage, PageId &nextPageId, int key)
{
	int nodeOccu = nodeOccupancy;
	while (nodeOccu > 0 && currentPage->pageNoArray[nodeOccu-1] >= key)
	{
		nodeOccu--;
	}
	while (nodeOccu >= 0 && currentPage->pageNoArray[nodeOccu] == 0)
	{
		nodeOccu--;
	}
	
	nextPageId = currentPage->pageNoArray[nodeOccu];
}

// helper function for scanNext, used to check if a key is in required range
const bool BTreeIndex::check_key(int key)
{
    if (lowOp == GT && key <= lowValInt)
    {
        return false;
    }
    if (lowOp == GTE && key < lowValInt)
    {
        return false;
    }
    if (highOp == LT && key >= highValInt)
    {
        return false;
    }
    if (highOp == LTE && key > highValInt)
    {
        return false;
    }
    return true;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------
  /**
	 * Fetch the record id of the next index entry that matches the scan.
	 * Return the next record from current page being scanned. If current page has 
	 * been scanned to its entirety, move on to the right sibling of current page, 
	 * if any exists, to start scanning that page. Make sure to unpin any pages that 
	 * are no longer required.
	 * 
   	 * @param outRid	RecordId of next record found that satisfies the scan criteria 
		* returned in this
	 * @throws ScanNotInitializedException If no scan has been initialized.
	 * @throws IndexScanCompletedException If no more records, satisfying the scan 
	 * criteria, are left to be scanned.
	**/
void BTreeIndex::scanNext(RecordId& outRid) 
{
    // check if scan has been initialized
    if (scanExecuting)
    {
        // to read this page, cast it to leaf node struct
        LeafNodeInt* curr = (LeafNodeInt*)currentPageData;
        // fetch next key and Rid, inc nextEntry
        int key = curr->keyArray[nextEntry];
        if (check_key(key) == false)
        {
            throw IndexScanCompletedException();
        }
        outRid = curr->ridArray[nextEntry];
        nextEntry = nextEntry + 1;
        // check if current page has been scanned to its entirety
        // page_number: Number of page containing this record.
        // leafOccupancy: Number of keys in leaf node, depending upon the type of key.
        if (outRid.page_number == 0 || nextEntry >= leafOccupancy)
        {
            // if there's no more sibling, ie. records, throw exception
            if (curr->rightSibPageNo == 0)
            {
                throw IndexScanCompletedException();
            }
            
            // otherwise, unpin current page and read right sibling page
            bufMgr->unPinPage(file,currentPageNum,false);
            currentPageNum = curr->rightSibPageNo;
            bufMgr->readPage(file,currentPageNum,currentPageData);
            LeafNodeInt* curr = (LeafNodeInt*)currentPageData;
            
            // reset nextEntry index to zero, and fetch key and Rid again
            nextEntry = 0;
            key = curr->keyArray[nextEntry];
            if (check_key(key) == false)
            {
                throw IndexScanCompletedException();
            }
            outRid = curr->ridArray[nextEntry];
            nextEntry = nextEntry + 1;
        }
    }
    else
    {
        // scan not initialized, throw exception
        throw ScanNotInitializedException();
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
  /**
	 * Terminate the current scan. Unpin any pinned pages. Reset scan specific variables.
	 * @throws ScanNotInitializedException If no scan has been initialized.
	**/
void BTreeIndex::endScan() 
{   
    if (scanExecuting)
    {
        // terminate current scan
        scanExecuting = false;
        // unpin any pinned pages
        bufMgr->unPinPage(file, currentPageNum, false);
        // reset scan variables
        nextEntry = -1;
        currentPageData = nullptr;
    }
    else
    {
        // no scan has been initialized, throw exception
        throw ScanNotInitializedException();
    }   
}

}
