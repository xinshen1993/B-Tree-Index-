/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <algorithm>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc, rc1;
  int    key;     
  string value;
  int    count;
  int    diff;
  int keycondition=0;//store the number of key conditions
  int numeq=0;//this used to decide using index or not when concerning equal
  int numne=0;//store the number of NE in conditions
  IndexCursor cursor,cursor1;//used when we read index, as smallest or largest bounds
  BTreeIndex Bindex;
  vector<SelCond> cond1(cond);//cond cannot be changed however cond1 can 
  vector<int> nevalue;//to store the values in not equal conditions


  // open the table file test part
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }
   for(unsigned i = 0; i < cond.size(); i++){//how many key conditions 
    if(cond[i].attr==1) keycondition++;
   }

   for(unsigned i = 0; i < cond.size(); i++) { //in some conditions we will skip index to directly scan entire table
      if(cond[i].attr!=1) goto scantable;//if there is conditions about value, we just scan the entire table
      if(cond[i].comp==SelCond::NE) {nevalue.push_back(atoi(cond[i].value)); numne++;}//store ne values in nevalue
      }
   if(keycondition==numne and numne!=0 and attr!=4 and attr!=1) goto scantable;//if only ne conditions exist and we not only select key or condition, we should scan entir table
   for(unsigned i = 0; i < cond.size(); i++) {//deal with more equal conditions this is just in case, seldom happen in reality
      if(cond[i].comp==SelCond::EQ) ++numeq;
    }
    if(numeq>=2) goto scantable;
    if(numeq==1 and cond.size()!=1) goto scantable;//just in case, doesnot matter, would not happen in reality


   //index part
  if ((rc = Bindex.open(table + ".idx", 'r')) < 0){ // it is okay if there is not index, just scan entire table
     goto scantable;
  }
  
  if(cond.size()==0 or keycondition==numne ){//the count part without condition or only have ne and has left(not goto scantable )
  if(attr==4){//if select count(*) without conditions or only have nes we always use index 
  int count=0;
  IndexCursor l_cursor;
  l_cursor.pid=2;
  l_cursor.eid=0;
 while(1){
    Bindex.readForward(l_cursor, key, rid);//after doing this ltcursor is to the next eid,maybe the next node
    if(std::find(nevalue.begin(),nevalue.end(),key)!=nevalue.end()) goto continue1;
      ++count;
    continue1:if(l_cursor.pid==0 and l_cursor.eid==0) break;//all the elements in leaf node have been count
 }  
     fprintf(stdout, "%d\n", count);
     goto exit_select;
  }
  
  if(attr==1){//if select key without conditions or only have nes we always use index
      IndexCursor l_cursor;
      l_cursor.pid=2;
      l_cursor.eid=0;
    while(1){
      Bindex.readForward(l_cursor, key, rid);//after doing this ltcursor is to the next eid,maybe the next node
      if(std::find(nevalue.begin(),nevalue.end(),key)!=nevalue.end()) goto continue2;
      fprintf(stdout, "%d\n", key);
      continue2:if(l_cursor.pid==0 and l_cursor.eid==0) break;//all the elements in leaf node have been count
 }  
      goto exit_select;
  }

  if(attr!=4 and attr!=1) goto scantable;//if there is no condition and we donot select count(*) or key, then just scan entire table 
}

  //index part: only one condition
  if(cond.size()==1){
    if(cond[0].comp==SelCond::EQ){//only one EQ condition
      int key;
      if((rc=Bindex.locate(atoi(cond[0].value), cursor))<0) return rc;
      Bindex.readForward(cursor, key, rid);
       switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);//if we only need keys we do not have to read file
      break;
    case 2:  // SELECT value
      rf.read(rid, key, value);
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      rf.read(rid, key, value);
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    case 4: // SELECT count(*) just in case, would not happen in reality
      fprintf(stdout, "%d\n", 1);
      break;
    }
    goto exit_select;
    }

//index part:only one condition
ltle1:
 if(cond1[0].attr==1 and (cond1[0].comp==SelCond::LT or cond1[0].comp==SelCond::LE)){//only LT or LE
  int key; 
  int count=0;
  rc=Bindex.locate(atoi(cond1[0].value), cursor);
  IndexCursor ltcursor;
  ltcursor.pid=2;
  ltcursor.eid=0;//the first information stored in leafnode is at page2eid0
  
  while(1){// this is because the righter leafnod could probably have lesser page because of split
    if(ltcursor.pid==cursor.pid and ltcursor.eid==cursor.eid) break;
    Bindex.readForward(ltcursor, key, rid);//after doing this ltcursor is to the next eid,maybe the next node
       if(std::find(nevalue.begin(),nevalue.end(),key)!=nevalue.end()) goto continue3;
       switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      rf.read(rid, key, value);
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      rf.read(rid, key, value);
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break; }
      ++count;
     continue3: if(ltcursor.pid==0 and ltcursor.eid==0) break;
 }  
   if(cond1[0].comp==SelCond::LE and ltcursor.pid!=0){//if ltcursor.pid=0, already last leaf we stop doing this 
    Bindex.readForward(ltcursor, key, rid);//after doing this ltcursor is to the next eid,maybe the next node
    if(key>atoi(cond1[0].value)) goto afterskip;//there is situation if only 41 42 45 exists we want key<=44, we will get 45 doing former line, have to avoid this
    if(std::find(nevalue.begin(),nevalue.end(),key)!=nevalue.end()) goto afterskip;   
       switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      rf.read(rid, key, value);
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      rf.read(rid, key, value);
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break; }
      ++count;
 }
    afterskip:
    if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
   goto exit_select;
}
//index part:only one condition
gtge1:
 if(cond1[0].attr==1 and (cond1[0].comp==SelCond::GT or  cond1[0].comp==SelCond::GE)){//only one condition > or >=
  int key; 
  int count=0;
  rc=Bindex.locate(atoi(cond1[0].value), cursor);
  IndexCursor ltcursor;
  ltcursor.pid=cursor.pid;
  ltcursor.eid=cursor.eid;//the first information we begin
  if(cond1[0].comp==SelCond::GT and rc==0)//if GT we have to skip the first one
  Bindex.readForward(ltcursor, key, rid);
  
  while(1){
    if(ltcursor.pid==0 and ltcursor.eid==0) break;//at last we get to (0,0),and also use to make sure after skip first one formerline whether we get to the end
    Bindex.readForward(ltcursor, key, rid);//after doing this ltcursor is to the next eid,maybe the next node
    if (rc==-1015 and ltcursor.pid==0) break;//used to distinguish the two different 0,0!!! which necessary if condi.value extent the most largest in BTree
    if(std::find(nevalue.begin(),nevalue.end(),key)!=nevalue.end()) continue;
       switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      rf.read(rid, key, value);
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      rf.read(rid, key, value);
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break; }
      ++count;
 }  
    if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
   goto exit_select;
}

}
  //index part: more than or equal to 2 conditions
  if(cond.size()>=2){
 // get the right key we need
  vector<int> lt,le,gt,ge;
  vector<int>::const_iterator itlt, itle,itgt,itge; 
  for(unsigned i=0;i<cond.size();i++){
    if(cond[i].comp==SelCond::LT) lt.push_back(atoi(cond[i].value));
  }
  if(lt.size()!=0) itlt=min_element(lt.begin(),lt.end()); 
  for(unsigned i=0;i<cond.size();i++){
    if(cond[i].comp==SelCond::LE) le.push_back(atoi(cond[i].value));
  }
  if(le.size()!=0) itle=min_element(le.begin(),le.end());
  for(unsigned i=0;i<cond.size();i++){
    if(cond[i].comp==SelCond::GT) gt.push_back(atoi(cond[i].value));
  }
  if(gt.size()!=0) itgt=max_element(gt.begin(),gt.end());
  for(unsigned i=0;i<cond.size();i++){
    if(cond[i].comp==SelCond::GE) ge.push_back(atoi(cond[i].value));
  }
  if(ge.size()!=0) itge=max_element(ge.begin(),ge.end());

 if(lt.size()==0 and le.size()==0){//only> or >=
  int i,j;
  if(gt.size()==0){j=*itge;char text[32];sprintf(text, "%d",j); cond1[0].comp=SelCond::GE;cond1[0].value=text;}
  else if(ge.size()==0){i=*itgt;char text[32];sprintf(text,"%d",i);cond1[0].comp=SelCond::GT;cond1[0].value=text;}
  else{
  i=*itgt;
  j=*itge;
  if(i>=j) {char text[32];sprintf(text,"%d",i);cond1[0].comp=SelCond::GT;cond1[0].value=text;}
  else {char text[32];sprintf(text, "%d",j); cond1[0].comp=SelCond::GE;cond1[0].value=text;}
  }
  goto gtge1;// after dealing with only> or>= we can go to the former only one condition part to do
 }
 else if(gt.size()==0 and ge.size()==0){//only < or<=
  int i,j;
  if(le.size()==0){i=*itlt;char text[32];sprintf(text,"%d",i);cond1[0].comp=SelCond::LT;cond1[0].value=text;}
  else if(lt.size()==0){j=*itle;char text[32];sprintf(text, "%d",j); cond1[0].comp=SelCond::LE;cond1[0].value=text;}
  else{
  i=*itlt;
  j=*itle;
  if(i<=j) {char text[32];sprintf(text,"%d",i);cond1[0].comp=SelCond::LT;cond1[0].value=text;}
  else {char text[32];sprintf(text, "%d",j); cond1[0].comp=SelCond::LE;cond1[0].value=text;}
  }
  goto ltle1;// after dealing with only< or<= we can go to the former only one condition part to do
 }
 else {// all possible need to consider
  int i,j;
  char text[32];
  char text1[32];//why if put outside is okay but inside not?
  char text2[32];
  char text3[32];
  char text4[32];
  char text5[32];
  if(ge.size()==0){
    i=*itgt;
    sprintf(text,"%d",i);
    cond1[0].comp=SelCond::GT;
    cond1[0].value=text;
  }
 if(gt.size()==0){
    j=*itge;
    sprintf(text1,"%d",j);
    cond1[0].comp=SelCond::GE;
    cond1[0].value=text1;
  } 
  if(ge.size()!=0 and gt.size()!=0){
    i=*itgt;
    j=*itge;
    if(i>=j) {sprintf(text2,"%d",i);cond1[0].comp=SelCond::GT;cond1[0].value=text2;}
    else {sprintf(text2, "%d",j); cond1[0].comp=SelCond::GE;cond1[0].value=text2;}
  }
  
  if(le.size()==0){
    j=*itlt;
    sprintf(text3,"%d",j);
    cond1[1].comp=SelCond::LT;
    cond1[1].value=text3;
  }
  if(lt.size()==0){
    i=*itle;
    sprintf(text4,"%d",i);
    cond1[1].comp=SelCond::LE;
    cond1[1].value=text4;
  }
  if(lt.size()!=0 and le.size()!=0){
    i=*itlt;
    j=*itle;
    if(i<=j) {sprintf(text5,"%d",i);cond1[1].comp=SelCond::LT;cond1[1].value=text5;}
    else {sprintf(text5, "%d",j); cond1[1].comp=SelCond::LE;cond1[1].value=text5;}
  }
 
 }
 //get the right key we need partend

  if(cond1[0].attr==1 and (cond1[0].comp==SelCond::GE or cond1[0].comp==SelCond::GT) and cond1[1].attr==1 and (cond1[1].comp==SelCond::LT or cond1[1].comp==SelCond::LE)) {
  int key; 
  int count=0;
  rc=Bindex.locate(atoi(cond1[0].value), cursor);//cursor lowest 
  rc1=Bindex.locate(atoi(cond1[1].value), cursor1);//cursor1 largest
  IndexCursor l_cursor;
  l_cursor.pid=cursor.pid;
  l_cursor.eid=cursor.eid;//the first information stored in leafnode is at
  if(cond1[0].comp==SelCond::GT and rc==0) 
  Bindex.readForward(l_cursor, key, rid);//the first is delete if GT is hold
  while(1){
    if(l_cursor.pid==cursor1.pid and l_cursor.eid==cursor1.eid) break;
    Bindex.readForward(l_cursor, key, rid);//after doing this ltcursor is to the next eid,maybe the next node
    if (rc==-1015 and l_cursor.pid==0) break;
    if(std::find(nevalue.begin(),nevalue.end(),key)!=nevalue.end()) goto continue4;
       switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      rf.read(rid, key, value);
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      rf.read(rid, key, value);
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break; }
      ++count;
      continue4: if(l_cursor.pid==0 and l_cursor.eid==0) break;

 }
    if(cond1[1].comp==SelCond::LE and l_cursor.pid!=0){//if already last leaf we stop doing this 
    Bindex.readForward(l_cursor, key, rid);//after doing this ltcursor is to the next eid,maybe the next node
    if(key>atoi(cond1[1].value)) goto afterskip1;
    if(std::find(nevalue.begin(),nevalue.end(),key)!=nevalue.end()) goto afterskip1;
    //rf.read(rid, key, value);
       switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      rf.read(rid, key, value);
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      rf.read(rid, key, value);
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break; }
      ++count;
 }
    afterskip1:
    if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
   goto exit_select;
}
}
  //index part end

  // scan the table file from the beginning
scantable:
  rid.pid = rid.sid = 0;
  count = 0;
  while (rid < rf.endRid()) {
    // read the tuple
    if ((rc = rf.read(rid, key, value)) < 0) {
      fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
      goto exit_select;
    }

    // check the conditions on the tuple
    for (unsigned i = 0; i < cond.size(); i++) {
      // compute the difference between the tuple value and the condition value
      switch (cond[i].attr) {
      case 1:
	diff = key - atoi(cond[i].value);
	break;
      case 2:
	diff = strcmp(value.c_str(), cond[i].value);
	break;
      }

      // skip the tuple if any condition is not met
      switch (cond[i].comp) {
      case SelCond::EQ:
	if (diff != 0) goto next_tuple;
	break;
      case SelCond::NE:
	if (diff == 0) goto next_tuple;
	break;
      case SelCond::GT:
	if (diff <= 0) goto next_tuple;
	break;
      case SelCond::LT:
	if (diff >= 0) goto next_tuple;
	break;
      case SelCond::GE:
	if (diff < 0) goto next_tuple;
	break;
      case SelCond::LE:
	if (diff > 0) goto next_tuple;
	break;
      }
    }

    // the condition is met for the tuple. 
    // increase matching tuple counter
    count++;

    // print the tuple 
    switch (attr) {
    case 1:  // SELECT key
      fprintf(stdout, "%d\n", key);
      break;
    case 2:  // SELECT value
      fprintf(stdout, "%s\n", value.c_str());
      break;
    case 3:  // SELECT *
      fprintf(stdout, "%d '%s'\n", key, value.c_str());
      break;
    }

    // move to the next tuple
    next_tuple:
    ++rid;
  }

  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
  RecordFile rf; //Record File to store the table
  RC     rc;
  RecordId   rid;  // record cursor for table scanning
  int    key;     
  string value;
  string::size_type loc;
  loc = loadfile.find(".", 0);
  string loadfile_delete_suffix(loadfile,0,loc);//delete the loadfile suffix, reserve 0 to the former element befor.

  //here is the index part
  BTreeIndex Bindex;//already defaut constructor
  if(index)
  {
    Bindex.open(loadfile_delete_suffix+".idx", 'w');
    //Bindex.rootPid=-1; 
  }
  //index part end
  
    if ((rc=rf.open(loadfile_delete_suffix + ".tbl", 'w')) < 0) {
      fprintf(stderr, "Error: while constructing table %s\n", loadfile_delete_suffix.c_str());
      return rc;
    }//construct a .tbl to store the data
  ifstream in(loadfile.c_str());//only C-style is okay!construct an ifstream and open the given file
  string line;
  while(getline(in,line)){
    SqlEngine::parseLoadLine(line,key,value);//get line from loadfile and convert it to key and value
    
    if ((rc=rf.append(key,value,rid)) < 0){
      fprintf(stderr, "Error: while appending a tuple to table %s\n", loadfile_delete_suffix.c_str());
      return rc;
    }
  //here is the index part faulse!!!
      if(index){
           Bindex.insert(key, rid);
      }

      //index part end
  }
  if(index){
           Bindex.close();
      }
  rf.close();
  in.close();
  /* your code here */

  return 0;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
