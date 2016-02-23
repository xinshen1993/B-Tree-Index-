/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include "BTreeIndex.h"
#include "BTreeNode.h"
#include <cstring>
#include <setjmp.h>
using namespace std;
static jmp_buf buf;//we will use setjmp to jum between functions
int recursive_midKey;
int recursive_i;
PageId pid_next;
PageId recursive_endpid;
/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    rootPid = -1;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)//not finished
{	RC rc;
	if((rc=pf.open(indexname,mode))<0) return rc;
    //we have to get the treeheight and rootpid,so we must first stor these two variables into the disk, how?
    char page[PageFile::PAGE_SIZE];
    PageId storepage=0;//we store the rootpid treeheight in 0
    //if(pf.endPid()==0){
    //need to be implement here	

    //}
    //else{
    if(pf.endPid()!=0){
    pf.read(storepage,page);
    memcpy(&rootPid,page,sizeof(PageId));  // the PageId of the root node
    memcpy(&treeHeight,page+sizeof(PageId),sizeof(int)); /// the height of the tree
	}	
	return 0; }

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{	
	RC rc;
	//before closing store the treeheight and rootpid
	//char page[PageFile::PAGE_SIZE];
    //PageId storepage=0;//we store the rootpid treeheight in page0
    //pf.read(storepage,page);
    //memcpy(page,&rootPid,sizeof(PageId));  // the PageId of the root node
    //memcpy(page+sizeof(PageId),&treeHeight,sizeof(int)); /// the height of the tree
	//pf.write(storepage,page);//write back to the disk
	Update_rootpid_treeheight(rootPid,treeHeight);
	
	if((rc=pf.close())<0) return rc;
	rootPid=-1;
	treeHeight=0;	

    return 0;
}

void BTreeIndex::Update_rootpid_treeheight(PageId i, int j){//??????here i is rootpid, j is treeheight
	char page[PageFile::PAGE_SIZE];
    PageId storepage=0;//we store the rootpid treeheight in page0
	pf.read(storepage,page);
    memcpy(page,&i,sizeof(PageId));  // the PageId of the root node
    memcpy(page+sizeof(PageId),&j,sizeof(int)); /// the height of the tree
	pf.write(storepage,page);//write back to the disk
}
/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)//not so sure about this
{	RC rc;
	IndexCursor cursor;
	BTLeafNode bt;
	//deal with the first insert
	if(rootPid<0){//not so sure about this part
		/*BTLeafNode node1; // There is something wrong with this part, however, i havenot figure it out yet
		int count=0;
		if(pf.endPid()==0)
		    memcpy(node1.buffer,&count,sizeof(int));
	    else{
	    	node1.read(1,pf);
	    	memcpy(&count,node1.buffer,sizeof(int));
	    }
		if(count>=BTLeafNode::RECORDS_PER_NODE)
			{
				int siblingKey;
				BTLeafNode sibling;
				node1.insertAndSplit(key,rid,sibling,siblingKey);
				sibling.write(2,pf);
				node1.setNextNodePtr(2);
				BTNonLeafNode root;
				root.initializeRoot(1,siblingKey,2);
				root.write(3,pf);
				rootPid=3;//page0 we use to store rootpid and treeheight so here is 3
		        treeHeight=2;
				Update_rootpid_treeheight(rootPid,treeHeight);

			}
		else{
			    node1.insert(key,rid);
			    node1.write(1,pf);
			}


		return 0;*/

		BTNonLeafNode node;
		BTLeafNode node1,node2;
		PageId pid1,pid2=0;
		node.initializeRoot(pid1,key,pid2);
		node.write(pf.endPid()+1,pf);//since pf.endPid()=0 at this point and we want to store some information in 0 so we skip the page0 to page 1
		node1.write(pf.endPid(),pf);//at this time pf.endpid=2 since we extend pig to 0 1 at last step .page2
		node2.write(pf.endPid(),pf);//page3
		pid1=pf.endPid()-2;//because we have to put root in 0 page4-2=page2
		pid2=pf.endPid()-1;//is this okay???page4-1=page3
		memcpy(node.buffer+sizeof(int),&pid1,sizeof(PageId));
		memcpy(node.buffer+sizeof(int)+sizeof(PageId)+sizeof(int),&pid2,sizeof(PageId));
		node1.setNextNodePtr(pid2);//set the fist leaf node's next pid
		int i=0;
		memcpy(node1.buffer,&i,sizeof(int));//copy the key rid to the second node
		memcpy(node2.buffer,&i,sizeof(int));//copy the key rid to the second node
		//memcpy(node2.buffer+sizeof(int),&key,sizeof(int));
		//memcpy(node2.buffer+sizeof(int)+sizeof(int),&rid,sizeof(RecordId));
		//we can make the leaves void and let the next part do it.
		//store three nodes information in disk
		node.write(1,pf);//page1 rootpid
		node1.write(pid1,pf);//page2
		node2.write(pid2,pf);//page3
		rootPid=1;//page0 we use to store rootpid and treeheight so here is 1
		treeHeight=2;
		Update_rootpid_treeheight(rootPid, treeHeight);
	}
	//deal with the first insert_end
    if((rc=locate(key,cursor))<0){//we would not insert duplicates, so this always holds
    	char page[PageFile::PAGE_SIZE];
    	int count;
    	pf.read(cursor.pid,page);
    	memcpy(&count,page,sizeof(int));//get the count of the node to see if there is space or not
    	if(count>=BTLeafNode::RECORDS_PER_NODE){	
    		recursive_endpid=pf.endPid();//state first before
    		int siblingKey;
    		BTLeafNode sibling;
    		bt.read(cursor.pid,pf);//get this leafnode
    		bt.insertAndSplit(key,rid,sibling,siblingKey);
    		bt.setNextNodePtr(recursive_endpid);//set the original's next pid
    		bt.write(cursor.pid,pf);//write back the original node after splitting
    		sibling.write(recursive_endpid,pf);//now we will get information about sibling(siblingkey,endpid)
    	//recursive_insert
    		recursive_midKey=siblingKey;//state first before
    		recursive_i=1;//state first before
    		//PageId pid_next;
    		recursive_insert(rootPid,key);
    	//recursive_inesert_end
    	}
    	else{
    	   
    		bt.read(cursor.pid,pf);//get this leafnode
    		bt.insert(key,rid);
    		bt.write(cursor.pid,pf);//write back to the disk
    	}
    }
    if(setjmp(buf))
    	return 0;
    return 0;//will this influence other?
}
//midKey
//treeHeight
//i=1
//BTN siblingN
//BTN node
//pid_xiayige,pid
//rootnode.pid;//node‘s pid
void BTreeIndex::recursive_insert(PageId pid, int recursive_key){//use to insert level by level I think this works okay
	BTNonLeafNode node;//is this okay? next recursive'node is different with this?
	node.read(pid,pf);	
	while(recursive_i<=treeHeight-2)
		{++recursive_i;
		 node.locateChildPtr(recursive_key,pid_next);//find the place the key we want to insert should be
		 recursive_insert(pid_next,recursive_key); 
		}
if((node.insert(recursive_midKey,recursive_endpid))==0) {//midkey and endpid are from before
	node.write(pid,pf);
	//goto RETURN;//is this okay?
	longjmp(buf,1);
}
else{ 
	int key_x=recursive_midKey;//before transfered
	//PageId page_x=endpid;
	BTNonLeafNode siblingN;
	node.insertAndSplit(key_x,recursive_endpid,siblingN,recursive_midKey);
	node.write(pid,pf);//what is this node? the original node before spliting
	siblingN.write(pf.endPid(),pf);//write this new node to the end
	recursive_endpid=pf.endPid()-1;//this will be the siblingN's pid?
	if(pid==rootPid){//if this is root means there should be a new root
       BTNonLeafNode rootnew;
       rootnew.initializeRoot(pid,recursive_midKey,recursive_endpid);//we create a new root
       rootnew.write(pf.endPid(),pf);
       rootPid=pf.endPid()-1;//will this be the new rootPid?
       treeHeight=treeHeight+1;
       Update_rootpid_treeheight(rootPid,treeHeight);
	}

}
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
{	RC rc;
	BTNonLeafNode btn;
	BTLeafNode bt;
	PageId pid;
	int i=1;
    btn.read(rootPid,pf);//get the rootnode
    while(i<=treeHeight-1){//for nonleafnode recall locateChildPtr//??we have to get the treeheight first
    btn.locateChildPtr(searchKey, pid);
    btn.read(pid,pf);//when treeheight=2 might be conflict however, we do not use the btn then
    i++;
	}//at last pid is the leafnode we want
	cursor.pid=pid;
	bt.read(pid,pf);//get this leafnode
	if((rc=bt.locate(searchKey,cursor.eid))<0){
	if(rc==-1015)
		return -1015;// this use to distinguish if not found in node and also largest than other keys in node
	else
	return RC_NO_SUCH_RECORD;//if not found this key, we send back error,however,we still get eid
	}
	else
		return 0;
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)//there is a &after IndexCursor because we want to change the cursor itself
{
    RC rc;
    int count;//used go to the next node
    char page[PageFile::PAGE_SIZE];
    char *ptr;
    rc = pf.read(cursor.pid, page);//read the contain of the specific leafnode
    memcpy(&count,page,sizeof(int));//error fixed here
    if(count==0){//deal with the special case when the left one node is zero
    memcpy(&cursor.pid,page+PageFile::PAGE_SIZE-sizeof(PageId),sizeof(PageId));        
    cursor.eid=0;//if this eid is that last one in one node,then we have to move to the next node to satisfy 0?
    rc = pf.read(cursor.pid, page);
    }
	ptr=page+sizeof(int)+(sizeof(int)+sizeof(RecordId))*cursor.eid;//find the place the key we want to find
	memcpy(&key,ptr,sizeof(int));
	memcpy(&rid,ptr+sizeof(int),sizeof(RecordId));
    memcpy(&count,page,sizeof(int));//error fixed here
    //PageId last;//deal with the last leave, even if it is the end we do not change to next page; for key<verybig
    //memcpy(&last,page+PageFile::PAGE_SIZE-sizeof(PageId),sizeof(PageId));
    if(cursor.eid>=(count-1) ){
    memcpy(&cursor.pid,page+PageFile::PAGE_SIZE-sizeof(PageId),sizeof(PageId));        
    cursor.eid=0;//if this eid is that last one in one node,then we have to move to the next node to satisfy 0?
    }
    else ++cursor.eid;
    return 0;
}
