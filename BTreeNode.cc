#include "BTreeNode.h"
#include <cstring>
#include <iostream>

using namespace std;

BTLeafNode::BTLeafNode()
{
  memset(buffer, 0, PageFile::PAGE_SIZE);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ return pf.read(pid, buffer); }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{ return pf.write(pid, buffer); }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{
  int count;

  // the first four bytes of a page contains # records in the node
  memcpy(&count, buffer, sizeof(int));
  return count;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{
  int count = getKeyCount();
  
  if (count == ENTRIES_PER_PAGE)
    return RC_NODE_FULL;
  
  int eid;
  locate(key, eid); // We assume no duplicate keys
  
  char *ptr = buffer + sizeof(int) + eid*ENTRY_SIZE;
  if (eid != count) {
    memmove(ptr + ENTRY_SIZE, ptr, (count - eid) * ENTRY_SIZE); // shift right
  }
  // store the recordId and key
  memcpy(ptr, &rid, sizeof(RecordId));
  memcpy(ptr + sizeof(RecordId), &key, sizeof(int));
  
  count++;
  memcpy(buffer, &count, sizeof(int)); // update the count
  return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{
  if (sibling.getKeyCount() > 0)
    return -1;

  const int half = (ENTRIES_PER_PAGE + 1) / 2;
  int eid, split = half;
  char copy[half * ENTRY_SIZE];
  locate(key, eid); // We assume no duplicate keys
  
  if (eid < half) // Ensure that the left one has (n+1)/2 keys
    split--;

  //int ikey;
  RecordId irid;
  /*for (int i = split; i < ENTRIES_PER_PAGE; i++) {
    readEntry(i, ikey, irid);
    sibling.insert(ikey, irid);
  }*/
  memcpy(copy, buffer + sizeof(int) + split*ENTRY_SIZE, (ENTRIES_PER_PAGE - split)*ENTRY_SIZE);
  sibling.splitFromSibling(ENTRIES_PER_PAGE - split, copy);
  
  memcpy(buffer, &split, sizeof(int)); // update the counter
  
  if (split < half)
    insert(key, rid);
  else
    sibling.insert(key, rid);

  sibling.readEntry(0, siblingKey, irid);
  return 0;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{
  int count = getKeyCount(), key, i;
  char *ptr = buffer + sizeof(int) + sizeof(RecordId);
  for (i = 0; i < count; i++, ptr += ENTRY_SIZE) {
    memcpy(&key, ptr, sizeof(int));
    if (key == searchKey) {
      eid = i;
      return 0;
    }
    if (key > searchKey) break;
  }
  eid = i;
  return RC_NO_SUCH_RECORD;
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
  if (eid < 0 || eid >= getKeyCount())
    return RC_INVALID_RID;
  char *ptr = buffer + sizeof(int) + eid*ENTRY_SIZE;
  memcpy(&rid, ptr, sizeof(RecordId));  
  memcpy(&key, ptr + sizeof(RecordId), sizeof(int));
  return 0;
}

RC BTLeafNode::splitFromSibling(int count, char *copy) {
  if (getKeyCount() > 0)
    return -1;
  memcpy(buffer, &count, sizeof(int));
  memcpy(buffer + sizeof(int), copy, count * ENTRY_SIZE);
  return 0;
}

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{
  PageId next;
  memcpy(&next, buffer + PageFile::PAGE_SIZE - sizeof(PageId), sizeof(PageId));
  return next;
}

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node
 */
void BTLeafNode::setNextNodePtr(PageId pid)
{
  memcpy(buffer + PageFile::PAGE_SIZE - sizeof(PageId), &pid, sizeof(PageId));
}


BTNonLeafNode::BTNonLeafNode()
{
  memset(buffer, 0, PageFile::PAGE_SIZE);
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ return pf.read(pid, buffer); }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ return pf.write(pid, buffer); }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{
  int count;

  // the first four bytes of a page contains # records in the node
  memcpy(&count, buffer, sizeof(int));
  return count;
}


/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{
  int count = getKeyCount();
  
  if (count == KEYS_PER_PAGE)
    return RC_NODE_FULL;

  int eid;
  locate(key, eid);
  
  char *ptr = buffer + sizeof(int) + sizeof(PageId) + eid*ENTRY_SIZE;
  if (eid != count) {
    memmove(ptr + ENTRY_SIZE, ptr, (count - eid) * ENTRY_SIZE); // shift right
  }
  // store the key and pageId
  memcpy(ptr, &key, sizeof(int));
  memcpy(ptr + sizeof(int), &pid, sizeof(PageId));
  
  count++;
  memcpy(buffer, &count, sizeof(int)); // update the count
  return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{  
  if (sibling.getKeyCount() > 0)
    return -1;

  int eid, half = (KEYS_PER_PAGE + 1) / 2;
  const int sibSize = (KEYS_PER_PAGE/2)*ENTRY_SIZE;
  char copy[sibSize];
  locate(key, eid);
  
  memcpy(buffer, &half, sizeof(int)); // update the count
  
  PageId sibPid1;//, sibPid;
  //int sibKey;
  char *ptr = buffer + sizeof(int) + sizeof(PageId);
  if (eid == half) {
    midKey = key;
    sibPid1 = pid;
  } else if (eid < half) {
    half--;
    memcpy(&midKey, ptr + half*ENTRY_SIZE, sizeof(int));
    memcpy(&sibPid1, ptr + half*ENTRY_SIZE + sizeof(int), sizeof(PageId));
    
    char *tmp = ptr + eid * ENTRY_SIZE;
    memmove(tmp + ENTRY_SIZE, tmp, (half - eid)*ENTRY_SIZE);
    memcpy(tmp, &key, sizeof(int));
    memcpy(tmp + sizeof(int), &pid, sizeof(PageId));
    half++;
  } else {
    memcpy(&midKey, ptr + half*ENTRY_SIZE, sizeof(int));
    memcpy(&sibPid1, ptr + half*ENTRY_SIZE + sizeof(int), sizeof(PageId));
    
    memmove(ptr + half*ENTRY_SIZE, ptr + (half+1)*ENTRY_SIZE, (eid - half)*ENTRY_SIZE);
    memcpy(ptr + eid*ENTRY_SIZE, &key, sizeof(int));
    memcpy(ptr + eid*ENTRY_SIZE + sizeof(int), &pid, sizeof(PageId));    
  }
  
  ptr += half*ENTRY_SIZE;
  memcpy(copy, ptr, sibSize);
  sibling.splitFromSibling(KEYS_PER_PAGE/2, sibPid1, copy, sibSize);
  /*memcpy(&sibKey, ptr, sizeof(int));
  memcpy(&sibPid, ptr + sizeof(int), sizeof(PageId));
  ptr += ENTRY_SIZE;
  
  sibling.initializeRoot(sibPid1, sibKey, sibPid);
  for (int i = half + 1; i < KEYS_PER_PAGE; i++, ptr += ENTRY_SIZE) {
    memcpy(&sibKey, ptr, sizeof(int));
    memcpy(&sibPid, ptr + sizeof(int), sizeof(PageId));
    sibling.insert(sibKey, sibPid);
  }*/
  return 0;
}

void BTNonLeafNode::locate(int searchKey, int& eid)
{
  int count = getKeyCount(), key, i;
  char *ptr = buffer + sizeof(int) + sizeof(PageId);
  for (i = 0; i < count; i++, ptr += ENTRY_SIZE) {
    memcpy(&key, ptr, sizeof(int));
    if (key > searchKey) break;
  }
  eid = i;
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 */
void BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
  int count = getKeyCount(), key, i;
  char *ptr = buffer + sizeof(int) + sizeof(PageId);
  for (i = 0; i < count; i++, ptr += ENTRY_SIZE) {
    memcpy(&key, ptr, sizeof(int));
    if (key == searchKey) {
      memcpy(&pid, ptr + sizeof(int), sizeof(PageId));
      return;
    }
    if (key > searchKey) break;
  }
  memcpy(&pid, ptr - sizeof(PageId), sizeof(PageId));
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 */
void BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{  
  int n = 1;
  memcpy(buffer, &n, sizeof(int));
  memcpy(buffer + sizeof(int), &pid1, sizeof(PageId));
  memcpy(buffer + sizeof(int) + sizeof(PageId), &key, sizeof(int));
  memcpy(buffer + sizeof(int)*2 + sizeof(PageId), &pid2, sizeof(PageId));
}

RC BTNonLeafNode::splitFromSibling(int count, PageId sibPid1, char *data, int dsize) {
  if (getKeyCount() > 0)
    return -1;
  memcpy(buffer, &count, sizeof(int));
  memcpy(buffer + sizeof(int), &sibPid1, sizeof(PageId));
  memcpy(buffer + sizeof(int) + sizeof(PageId), data, dsize);
  return 0;
}

void BTNonLeafNode::printKeys() {
  int count = getKeyCount(), key;
  char *ptr = buffer + sizeof(int) + sizeof(PageId);
  for (int i = 0; i < count; i++, ptr += ENTRY_SIZE) {
	memcpy(&key, ptr, sizeof(int));
	cout << key << " ";
  }
  cout << endl;
}

void BTNonLeafNode::getChildPtrs(vector<PageId>& ptrs) {
  int count = getKeyCount();
  PageId pid;
  char *ptr = buffer + sizeof(int);
  for (int i = 0; i <= count; i++, ptr += ENTRY_SIZE) {
	memcpy(&pid, ptr, sizeof(PageId));
	ptrs.push_back(pid);
  }
}