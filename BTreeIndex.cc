/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 * @author Siran "Simon" Shen
 * @date 11/14/2015
 */
 
#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <cstring>
#include <iostream>

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
: rootPid(-1), treeHeight(0) {}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
  RC rc;
  if ((rc = pf.open(indexname, mode)) < 0)
    return rc;

  char buffer[PageFile::PAGE_SIZE];
  
  if (pf.endPid() == 0) {
    memcpy(buffer, &rootPid, sizeof(PageId));
    memcpy(buffer + sizeof(PageId), &treeHeight, sizeof(int));
    
    if ((rc = pf.write(0, buffer)) < 0)
      return rc;
  }
  else {
    if ((rc = pf.read(0, buffer)) < 0)
      return rc;
    
    memcpy(&rootPid, buffer, sizeof(PageId));
    memcpy(&treeHeight, buffer + sizeof(PageId), sizeof(int));
  }
  
  return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
  char buffer[PageFile::PAGE_SIZE];
  memcpy(buffer, &rootPid, sizeof(PageId));
  memcpy(buffer + sizeof(PageId), &treeHeight, sizeof(int));
  
  RC rc;
  if ((rc = pf.write(0, buffer)) < 0)
    return rc;
  return pf.close();
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
  RC rc;
  
  if (!treeHeight) { // Tree is empty
    BTLeafNode node;
    node.insert(key, rid);
    rootPid = pf.endPid();
    
    if ((rc = node.write(rootPid, pf)) < 0)
      return rc;
    treeHeight = 1;
  }
  else {
    int    keyUp = -1;     // The key to be added to parent node
    PageId newNodeId = -1; // The new pageId after splitting
    if ((rc = insertHelper(key, rid, rootPid, 1, keyUp, newNodeId)) < 0)
      return rc;
    
    if (newNodeId != -1) {
      BTNonLeafNode newRoot;
      newRoot.initializeRoot(rootPid, keyUp, newNodeId);
      
      rootPid = pf.endPid();
      if ((rc = newRoot.write(rootPid, pf)) < 0)
        return rc;
      treeHeight++;
    }
  }
  return 0;
}

RC BTreeIndex::insertHelper(int key, const RecordId& rid, PageId nodeId, int level, int& keyUp, PageId& newNodeId) {
  if (level < 0)
    return -1;

  RC rc;
  
  if (level == treeHeight) { // Reaching the leaf node
    BTLeafNode node;
    if ((rc = node.read(nodeId, pf)) < 0)
      return rc;

    if (node.insert(key, rid) == RC_NODE_FULL) {
      BTLeafNode sibling;
      if ((rc = node.insertAndSplit(key, rid, sibling, keyUp)) < 0)
        return rc;

      newNodeId = pf.endPid();
      sibling.setNextNodePtr(node.getNextNodePtr());
      node.setNextNodePtr(newNodeId);
	  cout << "New ID: " << newNodeId << " and Key: " << keyUp << endl;
      if ((rc = sibling.write(newNodeId, pf)) < 0)
        return rc;
    }
    
    if ((rc = node.write(nodeId, pf)) < 0)
      return rc;
  }
  else { // This is a nonleaf node
    BTNonLeafNode node;
    if ((rc = node.read(nodeId, pf)) < 0)
      return rc;

    PageId childId;
    node.locateChildPtr(key, childId);
    if ((rc = insertHelper(key, rid, childId, level + 1, keyUp, newNodeId)) < 0)
      return rc;

    if (newNodeId != -1) { // split in the child node
	  cout << "New child ID: " << newNodeId << " and Key: " << keyUp << endl;
      if (node.insert(keyUp, newNodeId) == RC_NODE_FULL) {
        BTNonLeafNode sibling;
        if ((rc = node.insertAndSplit(keyUp, newNodeId, sibling, keyUp)) < 0)
          return rc;

        newNodeId = pf.endPid(); // update the ID
        if ((rc = sibling.write(newNodeId, pf)) < 0)
          return rc;
      }
      else { // Clean up if no split
        keyUp = -1;
        newNodeId = -1;
      }
	  
      if ((rc = node.write(nodeId, pf)) < 0)
        return rc;
    }
  }
  
  return 0;
}

/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
  BTLeafNode    leafNode;
  BTNonLeafNode nonLeafNode;
  
  RC     rc;
  PageId pid = rootPid;
  int    eid;
  
  for (int i = 1; i < treeHeight; i++) {
    if ((rc = nonLeafNode.read(pid, pf)) < 0)
      return rc;

    nonLeafNode.locateChildPtr(searchKey, pid);
  }
  
  if ((rc = leafNode.read(pid, pf)) < 0)
    return rc;
  
  rc = leafNode.locate(searchKey, eid);
  cursor.pid = pid;
  cursor.eid = eid;
  return rc;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
  RC rc;
  
  BTLeafNode node;  
  if ((rc = node.read(cursor.pid, pf)) < 0)
    return rc;

  if ((rc = node.readEntry(cursor.eid, key, rid)) < 0)
    return rc;

  if (++cursor.eid >= BTLeafNode::ENTRIES_PER_PAGE) {
    cursor.pid = node.getNextNodePtr();
    cursor.eid = 0;
  }
  return 0;
}

void BTreeIndex::printTree(PageId pid, int level) {
  if (pid == -1)
	pid = rootPid;
  if (level == treeHeight) {
	BTLeafNode leaf;
	leaf.read(pid, pf);
	int count = leaf.getKeyCount(), key;
	RecordId rid;
	for (int i = 0; i < count; i++) {
	  leaf.readEntry(i, key, rid);
	  cout << key << " ";
	}
	cout << endl;
  }
  else {
	BTNonLeafNode nonleaf;
	nonleaf.read(pid, pf);
	nonleaf.printKeys();
	vector<PageId> ptrs;
	nonleaf.getChildPtrs(ptrs);
	for (int i = 0; i < ptrs.size(); i++)
	  printTree(ptrs[i], level + 1);
  }  
}