#include "global.h"
#include "page.h"           // Include the header file for the Page class
#include "tableCatalogue.h" // Include TableCatalogue definition
#include "table.h"          // Include the full definition of the Table class
#include <vector>
#include <string>
#include <fstream>
#include <iostream> // For cerr

using namespace std; // Make std namespace accessible (acceptable in .cpp)

/**
 * @brief Construct a new Page object. Never used as part of the code
 *
 */
Page::Page()
{
    this->pageName = "";
    this->tableName = "";
    this->pageIndex = -1; // Initialize int pageIndex
    this->rowCount = 0;
    this->columnCount = 0;
    this->rows.clear();
}

/**
 * @brief Construct a new Page:: Page object given the table name and page
 * index. When tables are loaded they are broken up into blocks of BLOCK_SIZE
 * and each block is stored in a different file named
 * "<tablename>_Page<pageindex>". For example, If the Page being loaded is of
 * table "R" and the pageIndex is 2 then the file name is "R_Page2". The page
 * loads the rows (or tuples) into a vector of rows (where each row is a vector
 * of integers).
 *
 * @param tableName
 * @param pageIndex
 */
Page::Page(const string &tableName, int pageIndex) // Use const&
{
    logger.log("Page::Page");
    this->tableName = tableName;
    this->pageIndex = pageIndex; // Assign int pageIndex
    // Use to_string to convert the int pageIndex for the filename
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);
    Table *tablePtr = tableCatalogue.getTable(tableName);

    // Check if the table exists and the pageIndex is valid
    if (tablePtr == nullptr || pageIndex < 0 || pageIndex >= tablePtr->blockCount)
    {
        logger.log("Page::Page ERROR: Table '" + tableName + "' not found or invalid page index " + to_string(pageIndex) + ".");
        cerr << "Page::Page ERROR: Table '" << tableName << "' not found or invalid page index " << pageIndex << "." << endl;
        // Initialize to a safe state
        this->pageName = "";
        this->tableName = "";
        this->pageIndex = -1; // Set int pageIndex to -1
        this->rowCount = 0;
        this->columnCount = 0;
        this->rows.clear();
        return; // Exit constructor early
    }

    Table table = *tablePtr; // Dereference the pointer safely
    this->columnCount = table.columnCount;
    // Use BLOCK_SIZE directly if maxRowsPerBlock isn't reliably set elsewhere
    // uint maxRowCount = table.maxRowsPerBlock;
    // Avoid potential division by zero if columnCount is 0 (though unlikely for a valid table)
    // uint maxRowCount = (this->columnCount > 0) ? (BLOCK_SIZE * 1000 / (sizeof(int) * this->columnCount)) : 0;
    // if (maxRowCount == 0) maxRowCount = 1; // Ensure at least 1 row can be potentially stored if block size is very small

    // Get the expected row count for this specific block from the table object
    if (pageIndex < (int)table.rowsPerBlockCount.size())
    {
        this->rowCount = table.rowsPerBlockCount[pageIndex]; // Set rowCount based on catalogue info
        // Reserve space for efficiency, using the known row count for this block
        this->rows.reserve(this->rowCount);
    }
    else
    {
        logger.log("Page::Page ERROR: pageIndex " + to_string(pageIndex) + " out of bounds for rowsPerBlockCount vector (size " + to_string(table.rowsPerBlockCount.size()) + ").");
        cerr << "Page::Page ERROR: pageIndex " << pageIndex << " out of bounds for rowsPerBlockCount vector (size " << table.rowsPerBlockCount.size() << ")." << endl;
        // Initialize to a safe state
        this->pageName = "";
        this->tableName = "";
        this->pageIndex = -1;
        this->rowCount = 0;
        this->columnCount = 0;
        this->rows.clear();
        return; // Exit constructor early
    }

    // If rowCount for this page is 0, no need to read the file
    if (this->rowCount == 0)
    {
        logger.log("Page::Page: Page " + this->pageName + " has 0 rows according to catalogue. Skipping read.");
        // Ensure rows vector is empty
        this->rows.clear();
        // No need to open/close file
        return;
    }

    ifstream fin(pageName, ios::in);
    if (!fin)
    {
        logger.log("Page::Page ERROR: Cannot open page file " + pageName);
        cerr << "Page::Page ERROR: Cannot open page file " << pageName << endl;
        // Initialize to a safe state
        this->pageName = "";
        this->tableName = "";
        this->pageIndex = -1; // Set int pageIndex to -1
        this->rowCount = 0;
        this->columnCount = 0;
        this->rows.clear();
        return; // Exit constructor early
    }

    int number;
    // Read exactly rowCount rows
    for (long long int rowCounter = 0; rowCounter < this->rowCount; rowCounter++)
    {
        vector<int> currentRow; // Read into a temporary vector
        // Reserve space based on columnCount if it's known and > 0
        if (this->columnCount > 0)
        {
            currentRow.reserve(this->columnCount);
        }
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!(fin >> number))
            { // Check if read was successful
                logger.log("Page::Page ERROR: Failed to read data from " + pageName + " at row " + to_string(rowCounter) + ", col " + to_string(columnCounter) + ". File might be corrupted or shorter than expected.");
                cerr << "Page::Page ERROR: Failed to read data from " + pageName << " at row " << rowCounter << ", col " << columnCounter << endl;
                fin.close();
                // Initialize to a safe state or handle partial read
                // Mark how many rows were actually read before the error
                this->rowCount = rowCounter;
                // Clear partially read rows to avoid inconsistent state? Or keep them?
                // Keeping partially read data might be problematic. Clearing seems safer.
                this->rows.clear();
                this->pageIndex = -1; // Indicate failure by setting pageIndex to -1
                return;
            }
            currentRow.push_back(number);
        }
        // Check if the row read has the expected number of columns
        if ((int)currentRow.size() != this->columnCount)
        {
            logger.log("Page::Page WARNING: Row " + to_string(rowCounter) + " in " + pageName + " has incorrect column count (" + to_string(currentRow.size()) + "). Expected " + to_string(this->columnCount));
            // Decide how to handle: skip row, pad, error out?
            // For now, let's add it as is, but log a warning. The writePage logic might need adjustment.
        }
        this->rows.push_back(currentRow); // Add the fully read row
    }

    // After the loop, check if there's more data in the file than expected (rowCount rows)
    // This could indicate corruption or an issue with rowCount tracking.
    if (fin >> number)
    {
        logger.log("Page::Page WARNING: File " + pageName + " contains more data than expected (" + to_string(this->rowCount) + " rows).");
        cerr << "Page::Page WARNING: File " + pageName + " contains more data than expected (" << this->rowCount << " rows)." << endl;
        // Read and discard the rest? Or just log? For now, just log.
    }

    fin.close();

    // Final check: Verify that the number of rows read matches the expected rowCount
    if (this->rows.size() != (size_t)this->rowCount)
    {
        logger.log("Page::Page WARNING: Number of rows read (" + to_string(this->rows.size()) + ") does not match expected rowCount (" + to_string(this->rowCount) + ") for file " + pageName + ". Adjusting rowCount.");
        cerr << "Page::Page WARNING: Number of rows read (" << this->rows.size() << ") does not match expected rowCount (" << this->rowCount << ") for file " << pageName << ". Adjusting rowCount." << endl;
        this->rowCount = this->rows.size(); // Adjust rowCount to actual number of rows read
        // This discrepancy might indicate an issue elsewhere (e.g., table metadata update)
    }
}

/**
 * @brief Get row from page indexed by rowIndex
 *
 * @param rowIndex
 * @return vector<int>
 */
vector<int> Page::getRow(int rowIndex)
{
    // logger.log("Page::getRow"); // Logging this for every row access might be too verbose
    // Use unsigned comparison for safety with vector index
    // Also check against the actual size of the rows vector, not just rowCount
    if (rowIndex >= 0 && (size_t)rowIndex < this->rows.size())
    {
        // Return a copy of the row
        return this->rows[rowIndex];
    }

    // Log if the index is out of bounds based on the actual vector size
    if (rowIndex < 0 || (size_t)rowIndex >= this->rows.size())
    {
        // logger.log("Page::getRow WARN: rowIndex " + to_string(rowIndex) + " out of bounds [0, " + to_string(this->rows.size()) + ").");
    }
    // This condition indicates an inconsistency between rowCount and rows.size() if it triggers
    // when the previous condition didn't.
    else if ((long long int)rowIndex >= this->rowCount)
    {
        logger.log("Page::getRow ERROR: rowIndex " + to_string(rowIndex) + " is within internal rows vector size (" + to_string(this->rows.size()) + ") but exceeds stored rowCount (" + to_string(this->rowCount) + ").");
        cerr << "Page::getRow ERROR: rowIndex " << rowIndex << " is within internal rows vector size (" << this->rows.size() << ") but exceeds stored rowCount (" << this->rowCount << ")." << endl;
    }

    return vector<int>(); // Return empty vector if index is invalid or out of bounds
}

Page::Page(const string &tableName, int pageIndex, vector<vector<int>> rows, int rowCount) // Use const&
{
    logger.log("Page::Page (Constructor with rows)");
    this->tableName = tableName;
    this->pageIndex = pageIndex;  // Assign int pageIndex
    this->rows = std::move(rows); // Use move semantics for efficiency
    this->rowCount = rowCount;
    // Ensure columnCount is set correctly, handle empty rows case
    this->columnCount = (this->rows.empty() || this->rows[0].empty()) ? 0 : this->rows[0].size();
    // Use to_string to convert the int pageIndex for the filename
    this->pageName = "../data/temp/" + this->tableName + "_Page" + to_string(pageIndex);

    // Add a check for consistency between rowCount and actual rows size
    if (this->rowCount != (long long int)this->rows.size())
    {
        logger.log("Page::Page(rows) WARNING: Provided rowCount (" + to_string(this->rowCount) + ") differs from actual rows vector size (" + to_string(this->rows.size()) + "). Using vector size.");
        this->rowCount = this->rows.size();
    }
}

/**
 * @brief writes current page contents to file.
 *
 */
void Page::writePage()
{
    logger.log("Page::writePage");
    // Check if pageName is valid before opening
    if (this->pageName.empty() || this->pageIndex < 0) // Also check if pageIndex is valid
    {
        logger.log("Page::writePage ERROR: Page name is empty or page index is invalid, cannot write.");
        cerr << "Page::writePage ERROR: Page name is empty or page index (" << this->pageIndex << ") is invalid, cannot write." << endl;
        return;
    }
    ofstream fout(this->pageName, ios::trunc); // Use trunc to overwrite existing file
    if (!fout)
    {
        logger.log("Page::writePage ERROR: Cannot open file for writing: " + this->pageName);
        cerr << "Page::writePage ERROR: Cannot open file for writing: " << this->pageName << endl;
        return;
    }
    // Ensure rowCount doesn't exceed the actual number of rows stored
    // Use the actual size of the rows vector for iteration safety
    size_t actualRowCount = this->rows.size();
    // Update the object's rowCount if it differs from the vector size before writing
    // This might indicate an earlier inconsistency.
    if (this->rowCount != (long long int)actualRowCount)
    {
        logger.log("Page::writePage WARNING: Internal rowCount (" + to_string(this->rowCount) + ") differs from actual rows vector size (" + to_string(actualRowCount) + ") before writing. Adjusting rowCount.");
        this->rowCount = actualRowCount;
        // TODO: Consider if TableCatalogue needs updating here if row counts change.
        // This depends on how table metadata is managed.
    }

    // Write exactly actualRowCount rows
    for (size_t rowCounter = 0; rowCounter < actualRowCount; rowCounter++)
    {
        // Check if the row itself has the expected number of columns
        if ((int)this->rows[rowCounter].size() != this->columnCount)
        {
            logger.log("Page::writePage WARNING: Row " + to_string(rowCounter) + " has incorrect column count (" + to_string(this->rows[rowCounter].size()) + ") vs expected (" + to_string(this->columnCount) + "). Writing available columns.");
            cerr << "Page::writePage WARNING: Row " << rowCounter << " has incorrect column count (" << this->rows[rowCounter].size() << ") vs expected (" << this->columnCount << "). Writing available columns." << endl;
            // Decide how to handle: skip row, pad, or write available columns?
            // Writing available columns:
            int colsToWrite = this->rows[rowCounter].size();
            for (int columnCounter = 0; columnCounter < colsToWrite; columnCounter++)
            {
                if (columnCounter != 0)
                    fout << " ";
                fout << this->rows[rowCounter][columnCounter];
            }
            // Optionally pad with default values (e.g., 0) up to columnCount if strict format needed
            // for (int k = colsToWrite; k < this->columnCount; ++k) {
            //     fout << " 0"; // Example padding
            // }
        }
        else
        {
            // Normal case: write exactly columnCount elements
            for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
            {
                if (columnCounter != 0)
                    fout << " ";
                fout << this->rows[rowCounter][columnCounter];
            }
        }
        fout << endl;
    }
    fout.close();
    // Check fstream state after closing
    if (!fout.good())
    {
        logger.log("Page::writePage ERROR: Error occurred during writing or closing file " + this->pageName);
        cerr << "Page::writePage ERROR: Error occurred during writing or closing file " << this->pageName << endl;
    }
}
