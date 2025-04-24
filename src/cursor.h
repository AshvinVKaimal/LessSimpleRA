// #include"bufferManager.h"
// /**
//  * @brief The cursor is an important component of the system. To read from a
//  * table, you need to initialize a cursor. The cursor reads rows from a page one
//  * at a time.
//  *
//  */
// class Cursor{
//     public:
//     Page page;
//     int pageIndex;
//     string tableName;
//     int pagePointer;

//     public:
//     Cursor(string tableName, int pageIndex);
//     vector<int> getNext();
//     void nextPage(int pageIndex);
// };



// file: cursor.h
#ifndef CURSOR_H
#define CURSOR_H

#include "bufferManager.h"
#include <string>
#include <vector>
using namespace std;

/**
 * @brief The cursor is an important component of the system.
 * To read from a table, you need to initialize a cursor. 
 * The cursor reads rows from a page one at a time.
 */
class Cursor {
public:
    Page page;
    int pageIndex;
    string tableName;
    int pagePointer;

    Cursor(string tableName, int pageIndex);
    vector<int> getNext();
    void nextPage(int pageIndex);
};

#endif // CURSOR_H
