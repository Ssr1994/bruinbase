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
#include <climits>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);

static int cnt = 1;

RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC processRange(vector<SelCond>& newCond, const SelCond& cond, long& lo, long& hi) {
  long val = atol(cond.value);
  switch (cond.comp) {
  case SelCond::EQ:
    if (val < lo || val > hi) return -1;
    lo = hi = val;
    break;
  case SelCond::NE:
    if (val > lo && val < hi) {
      newCond.push_back(cond);
	  return -2;
	}
    else if (val == lo && val == hi) // conflict with point query
      return -1;
    else if (val == lo)
      lo++;
    else if (val == hi)
      hi--;
    break;
  case SelCond::GT:
    val++;
  case SelCond::GE:
    if (val > hi) return -1;
    if (val > lo) lo = val;
    break;
  case SelCond::LT:
    val--;
  case SelCond::LE:
    if (val < lo) return -1;
    if (val < hi) hi = val;
    break;
  }
  return 0;
}

bool checkConditions(const vector<SelCond>& cond, RecordId& rid, int key, string& value) {
  int diff;

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
	  if (diff != 0) return false;
	case SelCond::NE:
	  if (diff == 0) return false;
	case SelCond::GT:
	  if (diff <= 0) return false;
	case SelCond::LT:
	  if (diff >= 0) return false;
	case SelCond::GE:
	  if (diff < 0) return false;
	case SelCond::LE:
	  if (diff > 0) return false;
	}
  }
  return true;
}

void printTuple(int attr, int key, string& value) {
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
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile  rf;   // RecordFile containing the table
  RecordId    rid;  // record cursor for table scanning
  BTreeIndex  tree;
  IndexCursor cur;
  
  RC     rc;
  int    key;     
  string value;
  int    count = 0;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  // open the index file  
  if (tree.open(table + ".idx", 'r') == 0) {
    vector<SelCond> newCond;
    SelCond temp;
    temp.attr = 1;
	int NEonKey = 0; // number of NE condition(s) on key
    long lo = LONG_MIN, hi = LONG_MAX; // using long to avoid overflow
    for (unsigned i = 0; i < cond.size(); i++) {
      switch (cond[i].attr) {
      case 1:
        rc = processRange(newCond, cond[i], lo, hi);
        if (rc == -1)
          return 0; // range is invalid
		if (rc == -2)
		  NEonKey++;
        break;
      case 2:
        newCond.push_back(cond[i]);
        break;
      }
    }
    
    if (lo == LONG_MIN && hi == LONG_MAX)
      goto scan_table; // No range or point query on key
    
    int loKey = (int) lo, hiKey = (int) hi;
      
    rc = tree.locate(loKey, cur);
    if (rc != 0 && rc != RC_NO_SUCH_RECORD) {
      fprintf(stderr, "Error: while reading a tuple from index %s\n", table.c_str());
      goto exit_select;
    }
    
    if (rc == 0 && newCond.empty() && attr == 1) {
      while (cur.pid > 0 && (rc = tree.readForward(cur, key, rid)) == 0) {
        if (key > hiKey) break;
        count++;
        printTuple(attr, key, value);
      }
    }
    else if (rc == 0 && NEonKey == newCond.size() && attr == 1) {
      while (cur.pid > 0 && (rc = tree.readForward(cur, key, rid)) == 0) {
        if (key > hiKey) break;
        
		for (unsigned i = 0; i < newCond.size(); i++)
		  if (key == atoi(newCond[i].value)) continue;

        count++;
        printTuple(attr, key, value);
      }
    }
    else if (rc == 0) {
      while (cur.pid > 0 && (rc = tree.readForward(cur, key, rid)) == 0) {        
        if (key > hiKey) break;
        
        if ((rc = rf.read(rid, key, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          goto exit_select;
        }
      
        if (checkConditions(newCond, rid, key, value)) {
          count++;
          printTuple(attr, key, value);          
        }
      }
	}
	
	if (rc < 0 && rc != RC_NO_SUCH_RECORD) {
      fprintf(stderr, "Error: while reading a tuple from index %s\n", table.c_str());
      goto exit_select;
    }
  }
  else {
    scan_table:
    // scan the table file from the beginning
    rid.pid = rid.sid = 0;
    while (rid < rf.endRid()) {
      // read the tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      // check the conditions on the tuple
      if (!checkConditions(cond, rid, key, value))
		goto next_tuple;

      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;

      // print the tuple 
      printTuple(attr, key, value);

      // move to the next tuple
      next_tuple:
      ++rid;
    }
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
  /* your code here */
  RecordFile rf;   // RecordFile containing the table
  BTreeIndex tree;
  
  RC       rc;
  int      key;     
  string   value;
  string   line;
  RecordId rid;
  
  // open the loadfile
  ifstream fileToLoad (loadfile.c_str());
  if (fileToLoad.fail()) {
    fprintf(stderr, "Error: opening %s\n", loadfile.c_str());
    return RC_FILE_OPEN_FAILED;
  }
  
  // open the table file
  string tableName = table + ".tbl";
  if ((rc = rf.open(tableName, 'w')) < 0) {
    fprintf(stderr, "Error: opening %s\n", tableName.c_str());
    goto exit_load;
  }
  
  if (index) {
    string indexName = table + ".idx";
    if ((rc = tree.open(indexName, 'w')) < 0) {
      fprintf(stderr, "Error: opening %s\n", indexName.c_str());
      goto exit_load;
    }    
  }
  
  while (getline(fileToLoad, line)) {
    if ((rc = parseLoadLine(line, key, value)) < 0) {
      fprintf(stderr, "Error: while reading a line from %s\n", loadfile.c_str());
      goto exit_load;
    }
    
    if ((rc = rf.append(key, value, rid)) < 0) {
      fprintf(stderr, "Error: while inserting a tuple into table %s\n", table.c_str());
      goto exit_load;
    }
    
    if (index) {
      if ((rc = tree.insert(key, rid)) < 0) {
        fprintf(stderr, "Error: while inserting into index %s\n", table.c_str());
        goto exit_load;
      }
    }
  }  
  rc = 0;
  
  exit_load:
  tree.close();
  rf.close();
  fileToLoad.close();
  return rc;
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
