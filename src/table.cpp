#include "global.h"
#include <fstream>      // Make sure fstream is included
#include <sstream>      // Make sure sstream is included
#include <algorithm>    // For remove_if, needed by extractColumnNames if using original approach
#include <cctype>       // For ::isspace if using original approach

/**
 * @brief Default constructor
 */
Table::Table()
{
    // logger needed here? ensure logger is available or pass it if needed
    logger.log("Table::Table (Default Constructor)");
    // Initialize members to safe defaults
    this->columnCount = 0;
    this->rowCount = 0;
    this->blockCount = 0;
    this->maxRowsPerBlock = 0;
    // multiColumnIndexData is default constructed (empty)
}

/**
 * @brief Construct a new Table:: Table object used in the case where the data
 * file is available and LOAD command has been called. This command should be
 * followed by calling the load function;
 *
 * @param tableName
 */
Table::Table(const string &tableName)
{
    logger.log("Table::Table (Load Constructor) for: " + tableName);
    this->sourceFileName = "../data/" + tableName + ".csv";
    this->tableName = tableName;
    // Initialize other members
    this->columnCount = 0;
    this->rowCount = 0;
    this->blockCount = 0;
    this->maxRowsPerBlock = 0;
    // multiColumnIndexData is default constructed (empty)
}

/**
 * @brief Construct a new Table:: Table object for assignment commands.
 *
 * @param tableName
 * @param columns
 */
Table::Table(const string &tableName, const vector<string> &columns)
{
    logger.log("Table::Table (New Table Constructor) for: " + tableName);
    this->sourceFileName = "../data/temp/" + tableName + ".csv"; // Ensure temp directory exists
    this->tableName = tableName;
    this->columns = columns; // Copy columns
    this->columnCount = columns.size();

    // Calculate maxRowsPerBlock safely
    if (this->columnCount > 0 && BLOCK_SIZE > 0) { // Ensure BLOCK_SIZE is defined and positive
        // Assuming BLOCK_SIZE is in KB, adjust calculation if it's blocks or bytes
        this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
        if (this->maxRowsPerBlock == 0) {
            logger.log("Table::Table WARNING: Calculated maxRowsPerBlock is 0 for " + tableName + ". Check BLOCK_SIZE and columnCount.");
            // Set a minimum? Or handle potential division by zero later?
            // For safety, let's set a minimum of 1 if columns exist.
             this->maxRowsPerBlock = 1;
        }
    } else {
        this->maxRowsPerBlock = 0; // No columns or zero block size
        logger.log("Table::Table WARNING: Cannot calculate maxRowsPerBlock for " + tableName + " (columnCount=" + to_string(this->columnCount) + ", BLOCK_SIZE=" + to_string(BLOCK_SIZE) + ").");
    }


    // Initialize other members
    this->rowCount = 0; // New table starts empty
    this->blockCount = 0;
    // multiColumnIndexData is default constructed (empty)


    // Write header row to the *temporary* file, truncating if it exists
    ofstream fout(this->sourceFileName, ios::trunc);
    if (fout.is_open()) {
        this->writeRow<string>(columns, fout);
        fout.close();
        logger.log("Table::Table: Wrote header to temporary file: " + this->sourceFileName);
    } else {
        cerr << "ERROR: Could not open temporary file '" << this->sourceFileName << "' to write header for new table '" << this->tableName << "'." << endl;
    }
}

/**
 * @brief The load function is used when the LOAD command is encountered. It
 * reads data from the source file, splits it into blocks and updates table
 * statistics.
 *
 * @return true if the table has been successfully loaded
 * @return false if an error occurred
 */
bool Table::load()
{
    logger.log("Table::load: Starting load for " + this->tableName + " from " + this->sourceFileName);
    fstream fin(this->sourceFileName, ios::in);
    if (!fin.is_open()) {
        logger.log("Table::load ERROR: Cannot open source file: " + this->sourceFileName);
        return false;
    }

    string firstLine;
    if (getline(fin, firstLine))
    {
        if (this->extractColumnNames(firstLine)) {
             // Rewind stream to beginning to pass to blockify
             fin.clear(); // Clear potential EOF flags if needed
             fin.seekg(0, ios::beg);
             logger.log("Table::load: Column names extracted, proceeding to blockify.");
             if (this->blockify()) { // blockify now uses the already open stream
                  fin.close(); // Close file after successful blockify
                  logger.log("Table::load: Blockify successful for " + this->tableName);
                  return true;
             } else {
                  logger.log("Table::load ERROR: Blockify failed for " + this->tableName);
             }
        } else {
             logger.log("Table::load ERROR: Failed to extract column names from header: " + firstLine);
        }
    } else {
        logger.log("Table::load ERROR: Cannot read header line from file: " + this->sourceFileName);
    }

    fin.close(); // Make sure to close if something failed
    return false;
}

/**
 * @brief Function extracts column names from the header line of the .csv data
 * file.
 *
 * @param line
 * @return true if column names successfully extracted (i.e. no column name
 * repeats)
 * @return false otherwise
 */
bool Table::extractColumnNames(const string &firstLine)
{
    logger.log("Table::extractColumnNames from header: " + firstLine);
    this->columns.clear(); // Clear any previous columns
    unordered_set<string> columnNames;
    string word;
    stringstream s(firstLine);

    while (getline(s, word, ','))
    {
        // Trim whitespace robustly
        size_t first = word.find_first_not_of(" \t\n\r\f\v");
        if (string::npos == first) // String is all whitespace
        {
             word = "";
        } else {
            size_t last = word.find_last_not_of(" \t\n\r\f\v");
            word = word.substr(first, (last - first + 1));
        }


        if (word.empty()) {
             cout << "ERROR: Empty column name detected in header." << endl;
             logger.log("Table::extractColumnNames ERROR: Empty column name detected.");
             return false;
         }
        if (columnNames.count(word)) {
             cout << "ERROR: Duplicate column name '" << word << "' detected." << endl;
              logger.log("Table::extractColumnNames ERROR: Duplicate column name '" + word + "'.");
             return false;
        }
        columnNames.insert(word);
        this->columns.emplace_back(word);
        logger.log("Table::extractColumnNames: Found column '" + word + "'");
    }

    this->columnCount = this->columns.size();
    if (this->columnCount == 0) {
         cout << "ERROR: No columns found in header." << endl;
          logger.log("Table::extractColumnNames ERROR: No columns extracted.");
         return false;
     }

    // Recalculate maxRowsPerBlock based on actual column count
    if (BLOCK_SIZE > 0) { // Check BLOCK_SIZE again
        this->maxRowsPerBlock = (uint)((BLOCK_SIZE * 1000) / (sizeof(int) * this->columnCount));
         if (this->maxRowsPerBlock == 0) {
             logger.log("Table::extractColumnNames WARNING: Calculated maxRowsPerBlock is 0. Setting to 1.");
             this->maxRowsPerBlock = 1; // Ensure at least 1 row per block if possible
         }
    } else {
        logger.log("Table::extractColumnNames ERROR: BLOCK_SIZE is not positive. Cannot calculate maxRowsPerBlock.");
        this->maxRowsPerBlock = 0; // Indicate failure
        return false; // Cannot proceed without block size
    }


    logger.log("Table::extractColumnNames successful. Column Count: " + to_string(this->columnCount) + ", Max Rows/Block: " + to_string(this->maxRowsPerBlock));
    return true;
}

/**
 * @brief This function splits all the rows and stores them in multiple files of
 * one block size. Assumes the file stream is already open and positioned after the header.
 *
 * @return true if successfully blockified
 * @return false otherwise
 */
bool Table::blockify()
{
    logger.log("Table::blockify starting for table " + this->tableName);
    // Reopen the file; load() might have closed it or stream state might be bad
    ifstream fin(this->sourceFileName, ios::in);
     if (!fin.is_open()) {
         logger.log("Table::blockify ERROR: Could not reopen source file " + this->sourceFileName);
         return false;
     }


    string line, word;
    // Reset table stats before blockifying
    this->rowCount = 0;
    this->blockCount = 0;
    this->rowsPerBlockCount.clear();
    // Ensure these are correctly sized AFTER columnCount is known
    if (this->columnCount > 0) {
        this->distinctValuesInColumns.assign(this->columnCount, unordered_set<int>());
        this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
    } else {
         logger.log("Table::blockify ERROR: columnCount is 0.");
         fin.close();
         return false;
    }


    // Ensure maxRowsPerBlock is positive before allocating page buffer
    if (this->maxRowsPerBlock == 0) {
         logger.log("Table::blockify ERROR: maxRowsPerBlock is zero. Check column count and BLOCK_SIZE.");
         fin.close();
         return false;
     }
    vector<vector<int> > rowsInPage(this->maxRowsPerBlock, vector<int>(this->columnCount));


    int pageRowCounter = 0; // How many rows currently in rowsInPage buffer
    long long lineNum = 1; // Start counting lines after header
    getline(fin, line); // Skip header line

    vector<int> currentRow(this->columnCount); // Use a temporary row vector

    while (getline(fin, line))
    {
        lineNum++;
        stringstream s(line);
        bool rowReadSuccess = true;
        for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
        {
            if (!getline(s, word, ',')) {
                logger.log("Table::blockify WARNING: Line " + to_string(lineNum) + ": Not enough columns. Expected " + to_string(this->columnCount) + ". Line: " + line);
                rowReadSuccess = false;
                break;
            }
            try {
                 // Trim whitespace before converting
                 size_t first = word.find_first_not_of(" \t\n\r\f\v");
                 if (string::npos == first) word = "";
                 else {
                     size_t last = word.find_last_not_of(" \t\n\r\f\v");
                     word = word.substr(first, (last - first + 1));
                 }

                 if (word.empty()) { // Handle empty strings after trim - decide if this is an error or represents 0/NULL
                     logger.log("Table::blockify WARNING: Line " + to_string(lineNum) + ", Column " + to_string(columnCounter) + ": Empty value after trim. Treating as 0.");
                     currentRow[columnCounter] = 0; // Or handle as error/NULL if needed
                 } else {
                     currentRow[columnCounter] = stoi(word);
                 }
                 // Only copy to page buffer if conversion was okay so far
                 if (rowReadSuccess) {
                    rowsInPage[pageRowCounter][columnCounter] = currentRow[columnCounter];
                 }
             } catch (const std::invalid_argument& ia) {
                 logger.log("Table::blockify ERROR: Line " + to_string(lineNum) + ": Invalid integer '" + word + "'.");
                 rowReadSuccess = false;
                 break;
             } catch (const std::out_of_range& oor) {
                 logger.log("Table::blockify ERROR: Line " + to_string(lineNum) + ": Integer out of range '" + word + "'.");
                 rowReadSuccess = false;
                 break;
             }
        }


        // Check for too many columns
        string remainingWord;
        if (rowReadSuccess && getline(s, remainingWord)) {
             // Trim remainingWord and check if it's empty
            size_t first = remainingWord.find_first_not_of(" \t\n\r\f\v");
             if (first != string::npos) { // Found non-whitespace characters
                logger.log("Table::blockify WARNING: Line " + to_string(lineNum) + ": Too many columns. Extra content: '" + remainingWord + "'. Line: " + line);
                rowReadSuccess = false;
            }
         }


        if (rowReadSuccess) {
            // Row is valid, update stats and add to page
            this->updateStatistics(currentRow);
            this->rowCount++;
            pageRowCounter++;


            if (pageRowCounter == this->maxRowsPerBlock)
            {
                logger.log("Table::blockify: Writing page " + to_string(this->blockCount) + " with " + to_string(pageRowCounter) + " rows.");
                bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageRowCounter);
                this->blockCount++;
                this->rowsPerBlockCount.emplace_back(pageRowCounter);
                pageRowCounter = 0; // Reset page buffer counter
            }
        } else {
             // Optional: Decide whether to skip the bad row or stop loading
             cout << "Skipping invalid row number " << lineNum << ": " << line << endl;
             // If you want to stop on error:
             // fin.close(); return false;
         }
    } // End while(getline)


    // Write any remaining rows in the last partially filled page
    if (pageRowCounter > 0)
    {
        logger.log("Table::blockify: Writing final partial page " + to_string(this->blockCount) + " with " + to_string(pageRowCounter) + " rows.");
        bufferManager.writePage(this->tableName, this->blockCount, rowsInPage, pageRowCounter);
        this->blockCount++;
        this->rowsPerBlockCount.emplace_back(pageRowCounter);
    }

    fin.close(); // Close the file stream


    if (this->rowCount == 0 && this->columnCount > 0) {
        logger.log("Table::blockify WARNING: Table loaded successfully but is empty.");
        // Not an error per se.
    }


    // Clear distinct value sets to save memory after blockification
    this->distinctValuesInColumns.clear();
    this->distinctValuesInColumns.shrink_to_fit();


    logger.log("Table::blockify completed for " + this->tableName + ". Final Row Count: " + to_string(this->rowCount) + ", Final Block Count: " + to_string(this->blockCount));
    return true;
}



/**
 * @brief Given a row of values, this function will update the statistics it
 * stores i.e. it updates the number of rows that are present in the column and
 * the number of distinct values present in each column. These statistics are to
 * be used during optimisation. Called during blockify.
 *
 * @param row
 */
void Table::updateStatistics(const vector<int> &row)
{
    // Ensure stats vectors are sized correctly (should be done by blockify start)
     if (this->distinctValuesInColumns.size() != this->columnCount || this->distinctValuesPerColumnCount.size() != this->columnCount) {
         // This indicates an earlier logic error
         logger.log("Table::updateStatistics ERROR: Stats vectors not initialized correctly.");
         // Attempt to resize, but this might hide the root cause
         this->distinctValuesInColumns.assign(this->columnCount, unordered_set<int>());
         this->distinctValuesPerColumnCount.assign(this->columnCount, 0);
         // return; // Or return to avoid potential crashes
     }


    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        // distinctValuesInColumns is guaranteed to be populated during blockify
        // Check if the value is already in the set for this column
        if (this->distinctValuesInColumns[columnCounter].find(row[columnCounter]) == this->distinctValuesInColumns[columnCounter].end())
        {
            // If not found, insert it and increment the distinct count
            this->distinctValuesInColumns[columnCounter].insert(row[columnCounter]);
            this->distinctValuesPerColumnCount[columnCounter]++;
        }
    }
}


/**
 * @brief Checks if the given column is present in this table.
 *
 * @param columnName
 * @return true
 * @return false
 */
bool Table::isColumn(const string &columnName) const
{
    // logger.log("Table::isColumn checking for '" + columnName + "'"); // Can be verbose
    for (auto const& col : this->columns) // Use const& for efficiency
    {
        if (col == columnName)
            return true;
    }
    return false;
}


/**
 * @brief Renames the column indicated by fromColumnName to toColumnName. It is
 * assumed that checks such as the existence of fromColumnName and the non prior
 * existence of toColumnName are done.
 *
 * @param fromColumnName
 * @param toColumnName
 */
void Table::renameColumn(const string &fromColumnName,
                         const string &toColumnName)
{
    logger.log("Table::renameColumn: Renaming '" + fromColumnName + "' to '" + toColumnName + "' in table '" + this->tableName + "'");
    bool changed = false;
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == fromColumnName)
        {
            // Update index data if this column was indexed
                        // Check if the new name affects the index
            if (this->multiColumnIndexData.count(fromColumnName)) {
                logger.log("Table::renameColumn: Updating index map key from '" + fromColumnName + "' to '" + toColumnName + "' using C++11 method.");
                // C++11 way: Copy the map, insert under new key, then erase old key.
                std::map<int, RowLocation> indexMapCopy = this->multiColumnIndexData[fromColumnName]; // Make a copy
                this->multiColumnIndexData[toColumnName] = std::move(indexMapCopy); // Move the copy to the new key
                this->multiColumnIndexData.erase(fromColumnName); // Erase the entry with the old key
                logger.log("Index map key updated.");
            }
            // Update the column name in the vector
            columns[columnCounter] = toColumnName;
            changed = true;
            logger.log("Table::renameColumn: Column renamed successfully in memory.");
            break;
        }
    }
    if (!changed) {
         logger.log("Table::renameColumn WARNING: Column '" + fromColumnName + "' not found to rename.");
     }
    // IMPORTANT: This only renames the column in the Table object's memory.
    // It does NOT update the header in the persistent CSV file automatically.
    // makePermanent() would need to be called to reflect this change persistently.
    return;
}


/**
 * @brief Print up to PRINT_COUNT rows of the table.
 */
void Table::print() const
{
    logger.log("Table::print for table " + this->tableName);
    // Use PRINT_COUNT from global scope
    uint count = min((long long)PRINT_COUNT, this->rowCount);

    // Print headings
    logger.log("Printing header...");
    this->writeRow(this->columns, cout);


    if (this->rowCount == 0) {
         cout << "...Table is empty..." << endl;
         printRowCount(0);
         return;
     }
    if (this->blockCount == 0) {
        cout << "WARNING: Table has " << this->rowCount << " rows but zero blocks. Cannot print rows." << endl;
        printRowCount(this->rowCount); // Report metadata count anyway
        return;
     }


    logger.log("Printing first " + to_string(count) + " rows (or fewer if table is smaller)...");
    Cursor cursor(this->tableName, 0);
    vector<int> row;
    uint rowsPrinted = 0;
    for (; rowsPrinted < count; rowsPrinted++)
    {
        row = cursor.getNext();
        if (row.empty()) {
             logger.log("Table::print: Cursor returned empty row after printing " + to_string(rowsPrinted) + " rows.");
             break; // Stop if cursor runs out of rows
        }
        this->writeRow(row, cout);
    }
    logger.log("Finished printing rows.");
    printRowCount(this->rowCount); // Print the total row count stored in metadata
}



/**
 * @brief This function advances the cursor to the next page if the current one
 * is exhausted. Called internally by cursor.getNext().
 *
 * @param cursor
 */
void Table::getNextPage(Cursor *cursor)
{
    // logger.log("Table::getNextPage called for cursor on page " + to_string(cursor->pageIndex)); // Verbose

    // Basic validation
    if (!cursor) {
         logger.log("Table::getNextPage ERROR: Null cursor passed.");
         return;
     }
    // Optional: Check table existence again? Catalogue check might be slow here.
    // if (!tableCatalogue.isTable(cursor->tableName)) { ... }


    // Check if there is a next page *available*
    if (cursor->pageIndex < (int)(this->blockCount) - 1) // Cast blockCount to int for comparison
    {
        int nextPageIdx = cursor->pageIndex + 1;
        logger.log("Table::getNextPage: Advancing cursor for table '" + cursor->tableName + "' to page " + to_string(nextPageIdx));
        cursor->nextPage(nextPageIdx);
    } else {
        // No more pages physically exist for this table
        logger.log("Table::getNextPage: No more pages for cursor on table '" + cursor->tableName + "' (already at last page " + to_string(cursor->pageIndex) + ").");
        // Cursor needs to handle this state - likely by getRow returning empty
    }
}

/**
 * @brief called when EXPORT command is invoked to write table data to a permanent
 * file in "../data/". Deletes old temporary source file if it exists.
 *
 */
void Table::makePermanent()
{
    logger.log("Table::makePermanent for table " + this->tableName);
    string newSourceFile = "../data/" + this->tableName + ".csv";


    // Check if already permanent or if the source file *is* the target file
    if (this->sourceFileName == newSourceFile) {
        // This implies it was either loaded directly from ../data/ or already exported.
        // Maybe rewrite to ensure consistency? Or just log and return.
        logger.log("Table::makePermanent: Table's source file is already the permanent location: " + newSourceFile + ". Ensuring consistency by rewriting.");
        // If we don't rewrite, uncomment below and comment out rest of function
        // return;
    }


    // Check if a temporary source file needs deleting later
    bool tempFileExists = (this->sourceFileName.find("../data/temp/") != string::npos);
    string oldSourceFile = this->sourceFileName;


    logger.log("Table::makePermanent: Writing data to permanent file: " + newSourceFile);
    ofstream fout(newSourceFile, ios::trunc); // Overwrite or create permanent file
    if (!fout.is_open()) {
        cerr << "ERROR: Could not open permanent file '" << newSourceFile << "' for writing." << endl;
        logger.log("Table::makePermanent ERROR: Failed to open output file.");
        return;
    }


    // Write headings
    this->writeRow(this->columns, fout);


    // Use cursor to read all rows from pages and write them
    Cursor cursor(this->tableName, 0);
    vector<int> row;
    long long rowsWritten = 0;
    for (; rowsWritten < this->rowCount; rowsWritten++)
    {
        row = cursor.getNext();
        if (row.empty()) {
             // This implies rowCount metadata is wrong or cursor failed.
             logger.log("Table::makePermanent WARNING: Cursor ended after " + to_string(rowsWritten) + " rows, but expected " + to_string(this->rowCount) + ".");
             break; // Stop writing
        }
        this->writeRow(row, fout);
    }
    fout.close();


    // Check if the number of rows written matches expected count
    if (rowsWritten != this->rowCount) {
         logger.log("Table::makePermanent WARNING: Number of rows written (" + to_string(rowsWritten) + ") does not match table rowCount (" + to_string(this->rowCount) + "). Metadata might be inaccurate.");
         // Optionally update rowCount here? Or just log.
         // this->rowCount = rowsWritten;
     }


    // Update the sourceFileName in the Table object *only after successful write*
    this->sourceFileName = newSourceFile;
    logger.log("Table::makePermanent: Successfully wrote " + to_string(rowsWritten) + " rows. Source file path updated.");


    // Delete the old temporary source file if it existed and is different from the new one
    if (tempFileExists && oldSourceFile != newSourceFile) {
        logger.log("Table::makePermanent: Deleting old temporary source file: " + oldSourceFile);
        if (remove(oldSourceFile.c_str()) != 0) {
            // Use perror for system call errors
            perror(("Table::makePermanent WARNING: Error deleting temporary file " + oldSourceFile).c_str());
             logger.log("Table::makePermanent WARNING: Error deleting temporary file (errno details above).");
        } else {
             logger.log("Table::makePermanent: Successfully deleted old temporary source file.");
        }
    }
    // Note: Page files in ../data/temp/ still exist. Export only guarantees the CSV is in ../data/
}


/**
 * @brief Function to check if table is considered permanent (source file is in ../data/)
 *
 * @return true if permanent
 * @return false otherwise
 */
bool Table::isPermanent() const
{
    // logger.log("Table::isPermanent checking path: " + this->sourceFileName); // Verbose
    // Check if sourceFileName starts with "../data/" and does NOT contain "/temp/"
    if (this->sourceFileName.rfind("../data/", 0) == 0 && this->sourceFileName.find("/temp/") == string::npos) {
        return true;
    }
    return false;
}


/**
 * @brief The unload function removes the table from memory and deletes
 * all associated temporary files (pages and temp source CSV).
 *
 */
void Table::unload(){
    logger.log("Table::unload starting for table: " + this->tableName);
    this->clearIndex(); // Clear in-memory index data first


    // Delete page files from temp directory
    logger.log("Deleting page files for table '" + this->tableName + "' from ../data/temp/");
    for (uint pageCounter = 0; pageCounter < this->blockCount; pageCounter++) { // Use uint for blockCount type
        // Construct page file name reliably
        string pageFileName = "../data/temp/" + this->tableName + "_Page" + to_string(pageCounter);
        // Use BufferManager's delete method which might handle logging/errors
        bufferManager.deleteFile(pageFileName); // Pass full path
    }


    // Delete the source CSV file *only if it's temporary*
    if (!this->sourceFileName.empty() && !isPermanent()) {
        logger.log("Deleting temporary source file: " + this->sourceFileName);
        bufferManager.deleteFile(this->sourceFileName);
    } else if (!this->sourceFileName.empty()) {
         logger.log("Keeping permanent source file: " + this->sourceFileName);
    } else {
         logger.log("Table::unload: No source file name recorded to delete/keep.");
    }
    logger.log("Table::unload finished for " + this->tableName);
    // The Table object itself will be deleted by TableCatalogue::deleteTable
}


/**
 * @brief Function that returns a cursor positioned at the start of this table (page 0).
 *
 * @return Cursor
 */
Cursor Table::getCursor() const
{
    logger.log("Table::getCursor for table " + this->tableName);
    // Check if table has blocks before creating cursor
     if (this->blockCount == 0) {
         logger.log("Table::getCursor WARNING: Table '" + this->tableName + "' has no blocks. Cursor will return empty rows.");
         // Create cursor anyway, its getNext() should handle empty table state.
     }
    Cursor cursor(this->tableName, 0); // Always start cursor at page 0
    return cursor;
}

/**
 * @brief Function that returns the zero-based index of column indicated by columnName
 *
 * @param columnName
 * @return int column index, or -1 if not found
 */
int Table::getColumnIndex(const string &columnName) const
{
    // logger.log("Table::getColumnIndex for '" + columnName + "' in table '" + this->tableName + "'"); // Verbose
    for (int columnCounter = 0; columnCounter < this->columnCount; columnCounter++)
    {
        if (this->columns[columnCounter] == columnName) {
            // logger.log("Table::getColumnIndex: Found '" + columnName + "' at index " + to_string(columnCounter));
            return columnCounter;
        }
    }
    logger.log("Table::getColumnIndex WARNING: Column '" + columnName + "' not found in table '" + this->tableName + "'.");
    return -1; // Return -1 to indicate column not found
}




// --- Indexing Method Implementations ---


// Clears all column indices stored in multiColumnIndexData
void Table::clearIndex() {
    if (!this->multiColumnIndexData.empty()) {
         logger.log("Clearing all (" + to_string(this->multiColumnIndexData.size()) + ") column indices for table '" + this->tableName + "'");
         this->multiColumnIndexData.clear();
    }
    // Also reset any flags if they were used (they are removed now)
    // this->indexed = false;
    // this->indexedColumn = "";
    // this->indexingStrategy = NOTHING;
}


// Checks if a *specific* column has an index map entry
bool Table::isIndexed(const string& columnName) const {
    bool indexed = this->multiColumnIndexData.count(columnName) > 0;
    // logger.log("Table::isIndexed check for column '" + columnName + "': " + (indexed ? "Yes" : "No")); // Verbose
    return indexed;
}

// Removed getIndexedColumn() and getIndexingStrategy() implementations as they are no longer class members