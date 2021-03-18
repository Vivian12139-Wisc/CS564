/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * Team Memebers:
 * Name: Runheng Lei; Id: 9077507797
 * Purpose of file: 
 * 
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
        bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

  int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}

/**
 * Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table
 */
BufMgr::~BufMgr() {
    // Traverse Buffer pool and flush all dirty pages to disk
    for (FrameId i = 0; i < numBufs; i++)
    {
        BufDesc cur = bufDescTable[i];
        if (cur.dirty) 
        {
            // flushFile(cur.file);
            cur.file->writePage(bufPool[i]);
        }
    }
    // deallocate
    delete [] bufPool;
    delete [] bufDescTable;
    bufPool = NULL;
	  bufDescTable = NULL;
    hashTable = NULL;
}

/**
 * Advance clock to next frame in the buffer pool
 */
void BufMgr::advanceClock()
{
    clockHand = (clockHand + 1) % numBufs;
}

/**
 * Allocate a free frame.  
 *
 * @param frame   	Frame reference, frame ID of allocated frame returned via this variable
 * @throws BufferExceededException if all buffer frames are pinned
 */
void BufMgr::allocBuf(FrameId & frame) 
{
    for (FrameId i = 0; i < 2 * numBufs; i++)
    {
        // advance clockhand first
        advanceClock();
        BufDesc *cur_buf = &bufDescTable[clockHand];
        // check all requirments for allocation in the order shown in the flowchart
        if (cur_buf->valid) 
        {
            if (cur_buf->refbit)
            {
                // if refbit is set, clear refbit
                cur_buf->refbit = false;
                continue;
            }
            else {
                if (cur_buf->pinCnt > 0)
                {
                    // page pinned, not suitable for allocation
                    continue;
                }
                else
                {
                    if (cur_buf->dirty) 
                    {
                        // if dirty bit is set, write page to disk
                        // flushFile(cur_buf->file);
                        cur_buf->file->writePage(bufPool[clockHand]);
                    }
                }
            }
            // if the buffer frame allocated has a valid page in it, remove the entry from the hash table.
            hashTable->remove(cur_buf->file, cur_buf->pageNo);
        }
        // now we have a available frame to be allocated
        frame = cur_buf->frameNo;
        return;
    }
    // if no such buffer is found to be allocated, throw exception
    throw BufferExceededException();
}

/**
 * Reads the given page from the file into a frame and returns the pointer to page.
 * If the requested page is already present in the buffer pool pointer to that frame is returned
 * otherwise a new frame is allocated from the buffer pool for reading the page.
 *
 * @param file   	File object
 * @param PageNo  	Page number in the file to be read
 * @param page  	Reference to page pointer. Used to fetch the Page object in which requested page from file is read in.
 */	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;

	/**
	 * Case 1: Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and then 
	 * call the method file->readPage() to read the page from disk into the buffer pool frame. Next, 
	 * insert the page into the hashtable. Finally, invoke Set() on the frame to set it up properly. 
	 * Set() will leave the pinCnt for the page set to 1. Return a pointer to the frame containing the 
	 * page via the page parameter.
	 * 
	 * Case 2: Page is in the buffer pool. In this case set the appropriate refbit, increment the pinCnt 
	 * for the page, and then return a pointer to the frame containing the page via the page parameter.
	 */

    try
    {
        // Case 2
        hashTable->lookup(file, pageNo, frameNo);
        bufDescTable[frameNo].refbit = true;
        bufDescTable[frameNo].pinCnt += 1;
        page = &(bufPool[frameNo]);
    }
    catch (HashNotFoundException& e)
    {
        // Case 1
        allocBuf(frameNo);
        bufPool[frameNo] = file->readPage(pageNo);
        hashTable->insert(file, pageNo, frameNo);
        bufDescTable[frameNo].Set(file, pageNo);
        page = &(bufPool[frameNo]);
    }
}

/**
 * Unpin a page from memory since it is no longer required for it to remain in memory.
 *
 * @param file   	File object
 * @param PageNo  Page number
 * @param dirty		True if the page to be unpinned needs to be marked dirty	
 * @throws  PageNotPinnedException If the page is not already pinned
 */
void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
    try
    {
        // use hashTable to get the frameNo of the frame containing (file, PageNo)
        FrameId frameNo;
        hashTable->lookup(file, pageNo, frameNo);
        BufDesc *cur_buf = &bufDescTable[frameNo];
        // decrement the pinCnt and throw exception if pinCnt is already zero
        if (cur_buf->pinCnt != 0)
        {
            cur_buf->pinCnt = cur_buf->pinCnt - 1;
        }
        else 
        {
            throw PageNotPinnedException(file->filename(), cur_buf->pageNo, cur_buf->frameNo);
        }
        // if dirty is true, sets the dirty bit
        if (dirty == true)
        {
            cur_buf->dirty = true;
        }
    }
    catch (HashNotFoundException& e)
    {
        // do nothing if page is not found in the hash table lookup
    }
}

/**
 * Allocates a new, empty page in the file and returns the Page object.
 * The newly allocated page is also assigned a frame in the buffer pool.
 *
 * @param file   	File object
 * @param PageNo  Page number. The number assigned to the page in the file is returned via this reference.
 * @param page  	Reference to page pointer. The newly allocated in-memory Page object is returned via this reference.
 */
void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
    // allocate an empty page
    Page alloc_page = file->allocatePage();
    // obtain a buffer pool frame
    FrameId frameNo;
    allocBuf(frameNo);
    // insert an entry into the hash table
    pageNo = alloc_page.page_number();
    hashTable->insert(file, pageNo, frameNo);
    // invoke Set() on the frame to set it up
    bufDescTable[frameNo].Set(file, pageNo);
    // return a pointer to the buffer frame allocated for the page
    bufPool[frameNo] = alloc_page;
    page = &bufPool[frameNo];
}

/**
 * Writes out all dirty pages of the file to disk.
 * All the frames assigned to the file need to be unpinned from buffer pool before this function can be successfully called.
 * Otherwise Error returned.
 *
 * @param file   	File object
 * @throws  PagePinnedException If any page of the file is pinned in the buffer pool 
 * @throws BadBufferException If any frame allocated to the file is found to be invalid
 */
void BufMgr::flushFile(const File* file) 
{
    // scan through bufTable for pages belonging to the file
    for (FrameId i = 0; i < numBufs; i++)
    {
        BufDesc *cur_buf = &bufDescTable[i];
        if (cur_buf->file == file)
        {
            // detect exception
            if (cur_buf->pinCnt != 0)
            {
                throw PagePinnedException(file->filename(), cur_buf->pageNo, i);
            }
            if (cur_buf->valid == false)
            {
                throw BadBufferException(cur_buf->frameNo, cur_buf->dirty, cur_buf->valid, cur_buf->refbit);
            }
            // if page is dirty, flush page to disk and flip the dirty bit
            if (cur_buf->dirty == true)
            {
                cur_buf->file->writePage(bufPool[i]);
                cur_buf->dirty = false;
            }
            // remove the page from hashtable
            hashTable->remove(cur_buf->file, cur_buf->pageNo); 
            // invoke the clear method of BufDesc for the page frame
            cur_buf->Clear();
        }
    }
}

/**
 * Delete page from file and also from buffer pool if present.
 * Since the page is entirely deleted from file, its unnecessary to see if the page is dirty.
 *
 * @param file   	File object
 * @param PageNo  Page number
 */
void BufMgr::disposePage(File* file, const PageId PageNo)
{
    // delete page from file
    file->deletePage(PageNo);     
    try
    {
        // make sure this page is in the buffer pool
        FrameId frameNo;
        hashTable->lookup(file, PageNo, frameNo);
        // remove the entry from hash table
        hashTable->remove(file, PageNo);
        // free the buffer frame
        bufDescTable[frameNo].Clear();
    }
    catch (HashNotFoundException& e)
    {
    }  
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
  {
      tmpbuf = &(bufDescTable[i]);
      std::cout << "FrameNo:" << i << " ";
      tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
