#include "global.h"
#include "cursor.h"         // Include the header file for the Cursor class
#include "bufferManager.h"  // Include BufferManager definition
#include "tableCatalogue.h" // Include TableCatalogue definition
#include "table.h"          // Include the full definition of the Table class

using namespace std; // Make std namespace accessible

Cursor::Cursor(string tableName, int pageIndex)
{
    logger.log("Cursor::Cursor");
    this->page = bufferManager.getPage(tableName, pageIndex);
    this->pagePointer = 0;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
}

/**
 * @brief This function reads the next row from the page. The index of the
 * current row read from the page is indicated by the pagePointer(points to row
 * in page the cursor is pointing to).
 *
 * @return vector<int>
 */
vector<int> Cursor::getNext()
{
    logger.log("Cursor::geNext");
    vector<int> result = this->page.getRow(this->pagePointer);
    this->pagePointer++;
    if (result.empty())
    {
        // Check if the table exists before trying to get it
        Table *tablePtr = tableCatalogue.getTable(this->tableName);
        if (tablePtr != nullptr)
        {
            tablePtr->getNextPage(this);
            // Check if pagePointer was reset AND a valid page was loaded
            // Accessing page.pageIndex is now valid as it's public and int
            if (!this->pagePointer && this->page.pageIndex != -1)
            {
                result = this->page.getRow(this->pagePointer);
                this->pagePointer++;
            }
            else if (this->page.pageIndex == -1)
            {
                // Handle case where getNextPage failed to load a valid page
                // result remains empty
                logger.log("Cursor::getNext: getNextPage failed to load a valid next page.");
            }
            // If pagePointer was not reset, it means getNextPage didn't load a new page (likely end of table)
        }
        else
        {
            // Handle case where table doesn't exist
            // result remains empty
            logger.log("Cursor::getNext ERROR: Table " + this->tableName + " not found.");
        }
    }
    return result;
}
/**
 * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
 * reading from the new page.
 *
 * @param pageIndex
 */
void Cursor::nextPage(int pageIndex)
{
    logger.log("Cursor::nextPage");
    // Check if the pageIndex is valid before attempting to get the page
    Table *tablePtr = tableCatalogue.getTable(this->tableName);
    if (tablePtr != nullptr && pageIndex >= 0 && pageIndex < tablePtr->blockCount)
    {
        this->page = bufferManager.getPage(this->tableName, pageIndex);
        this->pageIndex = pageIndex;
        this->pagePointer = 0;
    }
    else
    {
        // Handle invalid pageIndex or table not found
        logger.log("Cursor::nextPage ERROR: Invalid page index or table not found.");
        // Optionally reset cursor state to indicate failure
        this->page = Page(); // Reset to a default/empty page (which has pageIndex -1)
        this->pageIndex = -1;
        this->pagePointer = 0;
    }
}