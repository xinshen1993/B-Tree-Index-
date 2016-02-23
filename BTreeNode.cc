#include "BTreeNode.h"
#include <cstring>

using namespace std;

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf){
	// read the page containing the node
	RC rc;
	if ((rc = pf.read(pid,buffer)) < 0) return rc; 


	return 0; }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf){
	RC rc;
	if ((rc = pf.write(pid,buffer)) < 0) return rc; 

	return 0; }//if write beyond the last PageId, file is automatically expanded to include the page with given id

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount(){ 
	int count;
// the first four bytes of a page contains # records in the page
     memcpy(&count, buffer, sizeof(int));

	return count; }

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid) //why &here?
{   
// the first four bytes of a page contains # records in the page
	int count;// count is start from 0,1,2
    memcpy(&count,buffer,sizeof(int));
    if(count>=RECORDS_PER_NODE)
    	return RC_NODE_FULL;//retrun an error code if the node is full
    //point to just after the last (key,rid)
    char *backptr=buffer+sizeof(int)+(sizeof(int)+sizeof(RecordId))*count;//if count=0 *backptr just point to buffer+sizeof(int)
	//find the place where inserted(key,rid) should be
	char *ptr=buffer+sizeof(int);
	if(ptr==backptr) goto label;
	int keycompare;//used to store the key 
	memcpy(&keycompare,ptr,sizeof(int));
	while(keycompare<key){
		ptr=ptr+sizeof(int)+sizeof(RecordId);
		if(ptr==backptr){
			break;
		}
		memcpy(&keycompare,ptr,sizeof(int));
	}
	//move the (key,rid)s back to make room for the inserted one
	while(backptr!=ptr){
	memcpy(backptr,backptr-(sizeof(int)+sizeof(RecordId)),sizeof(int)+sizeof(RecordId));//!!! 可疑点1
	//memcpy(backptr,backptr-(sizeof(int)+sizeof(RecordId)),sizeof(int));
	//memcpy(backptr+sizeof(int),backptr-sizeof(RecordId),sizeof(RecordId));
	backptr=backptr-(sizeof(int)+sizeof(RecordId));
	}
	//insert the (key,rid)
	label:
    memcpy(ptr,&key,sizeof(int));
	memcpy(ptr+sizeof(int),&rid,sizeof(RecordId));
	//update the # counts 
	count=count+1;
	memcpy(buffer,&count,sizeof(int));

	return 0; }

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
                              BTLeafNode& sibling, int& siblingKey){
	//coppy the half part records to the sibling node;
	//memcpy(sibling.buffer+sizeof(int),buffer+sizeof(int)+(sizeof(int)+sizeof(RecordId))*(RECORDS_PER_NODE - RECORDS_PER_NODE/2),(sizeof(int)+sizeof(RecordId))*(RECORDS_PER_NODE/2));
	//set the half part of the first buffer to be 0 exempt the pageid part
	//int siblingcount=(RECORDS_PER_NODE+1)/2;
	
	int finalcount=RECORDS_PER_NODE+1-(RECORDS_PER_NODE+1)/2;//after splitting, the first node should have these amount records;
	int compare;//used to store key
	int comparecount=0;//this used to make sure whether the inserted record should be in the first one or in the second one
	int i=0;//used to get the certain key
	memcpy(&compare,buffer+sizeof(int)+(sizeof(int)+sizeof(RecordId))*i,sizeof(int));
	while(key>compare){
			comparecount=comparecount+1;
			i=i+1;
			memcpy(&compare,buffer+sizeof(int)+(sizeof(int)+sizeof(RecordId))*i,sizeof(int));
		}
	
	if(comparecount<finalcount){//if this holds. means that the inserted record should be in the first part
		memcpy(sibling.buffer+sizeof(int),buffer+sizeof(int)+(sizeof(int)+sizeof(RecordId))*(RECORDS_PER_NODE/2),(sizeof(int)+sizeof(RecordId))*(RECORDS_PER_NODE-RECORDS_PER_NODE/2));//this will happen if inseted is in first part
	    int siblingcount=RECORDS_PER_NODE-RECORDS_PER_NODE/2;//mark that we spliting the node first and insert the key to the certain part after that, so this is what will havppen in this situation
	    int originalcount=RECORDS_PER_NODE/2;//here we did not care about the other elments reside in original node
	    memcpy(sibling.buffer,&siblingcount,sizeof(int));
	    memcpy(buffer,&originalcount,sizeof(int));//now we can make sure that we can insert without splitting in the first part... thais part might be improved by using the result of the former
	    BTLeafNode::insert(key, rid);//insert the node itself

	}
	else{//if the inserted record should be in the second part
		memcpy(sibling.buffer+sizeof(int),buffer+sizeof(int)+(sizeof(int)+sizeof(RecordId))*(1+RECORDS_PER_NODE/2),(sizeof(int)+sizeof(RecordId))*(RECORDS_PER_NODE-RECORDS_PER_NODE/2-1));//this will happen if inseted is in second part
		int siblingcount=RECORDS_PER_NODE-RECORDS_PER_NODE/2-1;//similar analysis
	    int originalcount=RECORDS_PER_NODE/2+1;
	    memcpy(sibling.buffer,&siblingcount,sizeof(int));
	    memcpy(buffer,&originalcount,sizeof(int));
	    sibling.insert(key, rid);//now we can make sure we can insert without splitting in the first part
	}
	memcpy(&siblingKey,sibling.buffer+sizeof(int),sizeof(int));//get the first key from the new sibling node

	//memcpy(sibling.buffer,&siblingcount,sizeof(int));//set the account of the sibling node
	sibling.setNextNodePtr(getNextNodePtr());//set sibling next Pagepid to the original node next Pageid
	//memcpy(buffer,&count,sizeof(int));//updating the count of the original node
    //?leave the second half part of the original node as it used to be?is that okay?
    
    //set the original node next Pageid= simbling
	//PageFile pf;//?why can directly use this?
    //BTLeafNode::setNextNodePtr(BTreeIndex::pf.endPid());//is this right?how to deal with endpid after this
	//the first key in the sibling node after split
    //memcpy(&siblingKey,sibling.buffer+sizeof(int),sizeof(int));


		return 0; }

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
	int count;
	int keycompare;
	memcpy(&count,buffer,sizeof(int));
    for(int i=0;i<=count-1;i++){
    	memcpy(&keycompare,buffer+sizeof(int)+(sizeof(int)+sizeof(RecordId))*i,sizeof(int));
    	if(searchKey==keycompare){
    		eid=i;// the first is eid=0!?
    		return 0;
    	}
    	else if(searchKey<keycompare){
    		eid=i;//immediately “behind” the largest key smaller than searchKey.
    		return RC_NO_SUCH_RECORD;
    	}
    }
    	eid=count;//what if node if full and is the largest? 
    	return -1015;//this use to distinguish if not found in node and also largest than other keys in node
    }

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{   //get the key and rid using eid
    memcpy(&key,buffer+sizeof(int)+(sizeof(int)+sizeof(RecordId))*eid,sizeof(int));
    memcpy(&rid,buffer+sizeof(int)+(sizeof(int)+sizeof(RecordId))*eid+sizeof(int),sizeof(RecordId));

	return 0; }

/*
 * Return the pid of the next slibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr(){
	//get the next node ptr from the back
	PageId Next_Node_PageId;
	memcpy(&Next_Node_PageId,buffer+PageFile::PAGE_SIZE-sizeof(PageId),sizeof(PageId));
	return Next_Node_PageId;
 }

/*
 * Set the pid of the next slibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{   //!!!why here use RC?
	memcpy(buffer+PageFile::PAGE_SIZE-sizeof(PageId),&pid,sizeof(PageId));//！！！可疑点2

	return 0; }

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 	RC rc;
    // read the page containing the node
	if ((rc = pf.read(pid,buffer)) < 0) return rc; 
	return 0; }
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{   RC rc;
	if ((rc = pf.write(pid,buffer)) < 0) return rc; 
	return 0; }

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ int count;
// the first four bytes of a page contains # records in the page
     memcpy(&count, buffer,sizeof(int));

	return count; }

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ //?how to deal with the first pageid?
	
    int count;//initial value? count is 0,1,2,3...
    memcpy(&count,buffer,sizeof(int));
    if(count>=RECORDS_PER_NonLeaf) 
    	return RC_NODE_FULL;//Return an error code if the node is full.
    char *backptr=buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*count;
    char *ptr=buffer+sizeof(int)+sizeof(PageId);
    int compare;
    for(int i=0;i<=count-1;i++){	
    	memcpy(&compare,buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i,sizeof(int));
    	if(compare>=key){//if we find the key that is just larger than the key we want to sert, means we find the place it should be
    		ptr=ptr+(sizeof(int)+sizeof(PageId))*i;
    		break;//after record the place, we break the for code part 可疑点3！！！
    	}
    	if(i==count-1)
    		ptr=ptr+(sizeof(int)+sizeof(PageId))*count;//if reach the last key and the break doesnot work means the key we want to insert is the lagest
}
	while(backptr!=ptr){//make room for the (key pageid) we want to insert
		memcpy(backptr,backptr-(sizeof(int)+sizeof(PageId)),sizeof(int)+sizeof(PageId));
		//memcpy(backptr,backptr-(sizeof(int)+sizeof(PageId)),sizeof(int));
		//memcpy(backptr+sizeof(int),backptr-sizeof(PageId),sizeof(PageId));
		backptr=backptr-(sizeof(int)+sizeof(PageId));
	}
//insert the (key,pageid)
    memcpy(ptr,&key,sizeof(int));
	memcpy(ptr+sizeof(int),&pid,sizeof(PageId));
	//update the # counts 
	count=count+1;
	memcpy(buffer,&count,sizeof(int));

	return 0; }

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
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)//how to deal with RC? need to check later
{ 	
    int finalcount=RECORDS_PER_NonLeaf+1-(RECORDS_PER_NonLeaf+1)/2;//after splitting,before up the midkey, the first part should have these amount records;
	int compare;//used to store key
	int comparecount=0;//this used to make sure the inserted record should be in the first one or in the second one
	int i=0;//used to get the certain key
	memcpy(&compare,buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i,sizeof(int));
	while(key>compare){
			comparecount=comparecount+1;
			i=i+1;
			memcpy(&compare,buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i,sizeof(int));
		}
	
	if(comparecount<finalcount){//if this holds. means that the inserted record should be in the first part
		memcpy(sibling.buffer+sizeof(int)+sizeof(PageId),buffer+sizeof(int)+sizeof(PageId)+SizeN_RECORD*(RECORDS_PER_NonLeaf/2),SizeN_RECORD*(RECORDS_PER_NonLeaf-RECORDS_PER_NonLeaf/2));//this will happen if inseted is in first part
	    int siblingcount=RECORDS_PER_NonLeaf-RECORDS_PER_NonLeaf/2;//mark that we spliting the node first and insert the key to the certain part after that, so this is what will happen in this situation
	    int originalcount=RECORDS_PER_NonLeaf/2;
	    memcpy(sibling.buffer,&siblingcount,sizeof(int));
	    memcpy(buffer,&originalcount,sizeof(int));//now we can make sure that we can insert without splitting in the first part... thais part might be improved by using the result of the former
	    BTNonLeafNode::insert(key, pid);
	    memcpy(&midKey,buffer+sizeof(int)+sizeof(PageId)+SizeN_RECORD*(originalcount),sizeof(int));//this will happen if inseted is in first part
	    PageId pidnew;//get the last pig of the first part
	    memcpy(&pidnew,buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*(originalcount)+sizeof(int),sizeof(PageId));
	    memcpy(buffer,&originalcount,sizeof(int));//after we bring a key and a pageid we decrease the # the first part
	    memcpy(sibling.buffer+sizeof(int),&pidnew,sizeof(PageId));//the last pig of the first part before becomes the fist pig in the sibling.buffer

	}
	else{//if the inserted record should be in the second part
		memcpy(sibling.buffer+sizeof(int)+sizeof(PageId),buffer+sizeof(int)+sizeof(PageId)+SizeN_RECORD*(1+RECORDS_PER_NonLeaf/2),SizeN_RECORD*(RECORDS_PER_NonLeaf-RECORDS_PER_NonLeaf/2-1));//this will happen if inseted is in second part
		int siblingcount=RECORDS_PER_NonLeaf-RECORDS_PER_NonLeaf/2-1;//similar analysis with former
	    int originalcount=RECORDS_PER_NonLeaf/2+1;
	    memcpy(sibling.buffer,&siblingcount,sizeof(int));
	    memcpy(buffer,&originalcount,sizeof(int));//now we can make sure we can insert whithout splitting
	    sibling.insert(key, pid);
	    memcpy(&midKey,buffer+sizeof(int)+sizeof(PageId)+SizeN_RECORD*(originalcount-1),sizeof(int));//this will happen if inseted is in first part
	    PageId pidnew;
	    memcpy(&pidnew,buffer+sizeof(int)+sizeof(PageId)+SizeN_RECORD*(originalcount-1)+sizeof(int),sizeof(PageId));//take away the last pig of the first 
	    originalcount=originalcount-1;
	    memcpy(buffer,&originalcount,sizeof(int));//the first part,we get away the last (key,pageid)
	    memcpy(sibling.buffer+sizeof(int),&pidnew,sizeof(PageId));//the last pig of the first part before becomes the fist pig in the sibling.buffer
	}


		return 0; }

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 	
	int count;
	int keycompare;
	memcpy(&count,buffer,sizeof(int));
    for(int i=0;i<=count-1;i++){
    	memcpy(&keycompare,buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i,sizeof(int));
    	if(searchKey<keycompare){//if the key follow is just larger than serchkey,means the left pig is the one we want
    	memcpy(&pid,buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i-sizeof(PageId),sizeof(PageId));
    		return 0;//this one is also hold for the first pig, and we onely need to do something else for the last pig
    	}
    	if(i==count-1){//deal with the last pig
    		memcpy(&pid,buffer+sizeof(int)+sizeof(PageId)+(sizeof(int)+sizeof(PageId))*i+sizeof(int),sizeof(PageId));
    		return 0;
    	}
    }
 }//what is the error code?

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{  int countinitial=1;
   memcpy(buffer,&countinitial,sizeof(int));
   memcpy(buffer+sizeof(int),&pid1,sizeof(PageId));
   memcpy(buffer+sizeof(int)+sizeof(PageId),&key,sizeof(int));
   memcpy(buffer+sizeof(int)+sizeof(PageId)+sizeof(int),&pid2,sizeof(PageId));
	
	return 0; }
