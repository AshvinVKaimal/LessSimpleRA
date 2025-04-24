#ifndef BUFFERMANAGER_H
#define BUFFERMANAGER_H

#include <string>
#include <vector>
#include <deque>
#include <fstream>  // Include for file operations used in matrix functions
#include <sstream>  // Include for stringstream used in matrix functions
#include "logger.h" // Include Logger for logging capabilities
#include "page.h"   // Include the full definition of Page

// Forward declarations to avoid circular dependencies
class Table; // Needed for deleteTablePages parameter type in tableCatalogue access

/**
 * @brief The BufferManager is responsible for reading pages to the main memory.
 * Recall that large files are broken and stored as blocks in the hard disk. The
 * minimum amount of memory that can be read from the disk is a block of data.
 * The buffer manager stores pages read from the disk in a pool of buffer frames
 * (memory blocks in main memory). To manage the buffer frames the buffer manager
 * uses eviction policies. Revisit the description of the buffer manager in the
 * course slides for more details.
 *
 */
class BufferManager
{
    std::deque<Page> pages; // Use std::deque for efficient front removal (LRU)
    size_t maxSize;         // Maximum number of pages the buffer can hold

public:
    BufferManager();
    ~BufferManager(); // Declare destructor

    Page getPage(const std::string &tableName, int pageIndex);
    bool inPool(const std::string &pageName);
    Page *findPage(const std::string &pageName);                  // Declaration for findPage
    void insertIntoPool(Page &page);                              // Declaration for insertIntoPool
    void writeAllPages();                                         // Declaration for writeAllPages
    void deletePage(const std::string &tableName, int pageIndex); // Declaration for deletePage
    void deleteTablePages(const std::string &tableName);          // Declaration for deleteTablePages
    void clearPoolForTable(const std::string &tableName);         // Declaration for clearPoolForTable

    // Add method specifically for writing index pages (serialized vector<int>)
    void writeIndexPage(const std::string &pageName, const std::vector<int> &pageData);

    // Matrix specific functions - Keep declarations consistent with definitions
    std::vector<std::vector<int>> getBlock(const std::string &matrixName, int rowBlockIndex, int colBlockIndex);
    void writeBlock(const std::string &matrixName, int rowBlockIndex, int colBlockIndex, const std::vector<std::vector<int>> &blockData);
    std::vector<std::vector<int>> loadMatrix(const std::string &matrixName, int &dimension);
    void writeMatrix(const std::string &matrixName, const std::vector<std::vector<int>> &matrixData, int dimension);
    void writeBlock(const std::string &matrixName, int pageIndex, const std::vector<int> &rowData); // Overload for single row write? Ensure this is intended.
};

#endif // BUFFERMANAGER_H