#ifndef PAGE_H
#define PAGE_H

#include <string>
#include <vector>
#include "logger.h" // Include Logger for logging capabilities

// DO NOT USE "using namespace std;" in header files
// Use std:: prefix instead.

/**
 * @brief The Page object is the main memory representation of a physical page
 * (equivalent to a block). The page class and the page.h header file are at the
 * bottom of the dependency tree when compiling files.
 *<p>
 * Do NOT modify the Page class. If you find that modifications
 * are necessary, you may do so by posting the change you want to make on Moodle
 * or Teams with justification and gaining approval from the TAs.
 *</p>
 */

class Page
{

    std::string tableName;
    std::string pageName;
    int columnCount;
    long long int rowCount; // Changed to long long int for potentially large tables
    std::vector<std::vector<int>> rows;

public:
    int pageIndex; // Changed type to int and made public

    Page();
    Page(const std::string &tableName, int pageIndex);                                                   // Use const& for string
    Page(const std::string &tableName, int pageIndex, std::vector<std::vector<int>> rows, int rowCount); // Use const& for string
    std::vector<int> getRow(int rowIndex);
    void writePage();

    // Added getter methods for private members if needed elsewhere
    std::string getTableName() const { return tableName; }
    std::string getPageName() const { return pageName; }
    int getColumnCount() const { return columnCount; }
    long long int getRowCount() const { return rowCount; } // Updated return type
};

#endif // PAGE_H