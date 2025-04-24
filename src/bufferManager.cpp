#include "global.h"
#include "bufferManager.h"  // Include the header file for the BufferManager class
#include "page.h"           // Include the Page class definition
#include "tableCatalogue.h" // Include TableCatalogue for accessing table info
#include "table.h"          // Include the full Table definition
#include <string>
#include <vector>
#include <deque>     // Use deque for efficient front removal
#include <algorithm> // For std::find_if, std::remove_if
#include <iostream>  // For cerr
#include <fstream>   // For file operations
#include <sstream>   // For stringstream
#include <cstdio>    // For remove()

using namespace std; // Make std namespace accessible

BufferManager::BufferManager()
{
    logger.log("BufferManager::BufferManager");
    // Initialize buffer size based on global BLOCK_COUNT
    this->maxSize = BLOCK_COUNT; // Initialize maxSize
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 * Uses LRU eviction policy.
 *
 * @param tableName
 * @param pageIndex
 * @return Page
 */
Page BufferManager::getPage(const string &tableName, int pageIndex) // Use const& for string
{
    logger.log("BufferManager::getPage");
    // Use to_string for converting int pageIndex
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);

    // Check if page is already in the buffer pool using its name
    auto it = find_if(pages.begin(), pages.end(),
                      [&](const Page &p)
                      { return p.getPageName() == pageName; }); // Use getter

    if (it != pages.end())
    {
        // Page found in buffer, move it to the back (most recently used)
        Page foundPage = *it;
        pages.erase(it);
        pages.push_back(foundPage);
        logger.log("BufferManager::getPage: Page " + pageName + " found in buffer.");
        return foundPage;
    }
    else
    {
        // Page not in buffer, need to load it
        logger.log("BufferManager::getPage: Page " + pageName + " not in buffer. Loading.");
        Page newPage(tableName, pageIndex); // Load the page

        // Check if the loaded page is valid (e.g., file existed, read successful)
        // A pageIndex of -1 often indicates an error during loading in the Page constructor
        if (newPage.pageIndex == -1) // Accessing public member pageIndex
        {
            logger.log("BufferManager::getPage: Failed to load page " + pageName + ". Returning empty page.");
            cerr << "BufferManager::getPage: Failed to load page " + pageName + "." << endl;
            return Page(); // Return a default/empty page to indicate failure
        }

        // Check if buffer is full before inserting
        if (pages.size() >= this->maxSize) // Use this->maxSize
        {
            // Buffer is full, evict the least recently used page (at the front)
            Page &evictedPage = pages.front();
            logger.log("BufferManager::getPage: Buffer full. Evicting page " + evictedPage.getPageName()); // Use getter
            // Write the evicted page back to disk if it has been modified (implement modification tracking if needed)
            // For now, we assume pages might need writing back. A 'dirty bit' would optimize this.
            evictedPage.writePage();
            pages.pop_front(); // Remove from the front (deque is efficient here)
        }

        // Insert the newly loaded page at the back (most recently used)
        pages.push_back(newPage);
        logger.log("BufferManager::getPage: Page " + pageName + " loaded and added to buffer.");
        return newPage;
    }
}

/**
 * @brief Checks to see if a page exists in the pool
 *
 * @param pageName
 * @return true
 * @return false
 */
bool BufferManager::inPool(const string &pageName) // Use const&
{
    logger.log("BufferManager::inPool");
    // Use find_if with the getter method
    auto it = find_if(pages.begin(), pages.end(),
                      [&](const Page &p)
                      { return p.getPageName() == pageName; }); // Use getter
    return it != pages.end();
}

/**
 * @brief Find a specific page in the buffer pool by its name.
 * Returns a pointer to the page if found, otherwise nullptr.
 * This allows modifying the page in the buffer directly.
 *
 * @param pageName
 * @return Page*
 */
Page *BufferManager::findPage(const string &pageName) // Use const&
{
    logger.log("BufferManager::findPage");
    auto it = find_if(pages.begin(), pages.end(),
                      [&](const Page &p)
                      { return p.getPageName() == pageName; }); // Use getter

    if (it != pages.end())
    {
        // Return pointer to the found page
        return &(*it);
    }
    return nullptr; // Page not found
}

/**
 * @brief Inserts page into the pool. Handles eviction if necessary.
 * Replaces existing page with the same name.
 *
 * @param page The page object to insert (passed by reference, will be copied into deque).
 */
void BufferManager::insertIntoPool(Page &page) // Pass by reference
{
    logger.log("BufferManager::insertIntoPool");
    string pageName = page.getPageName(); // Get page name using getter

    // First, check if a page with the same name already exists.
    auto it = find_if(pages.begin(), pages.end(),
                      [&](const Page &p)
                      { return p.getPageName() == pageName; }); // Use getter

    if (it != pages.end())
    {
        // Page already exists. Remove the old one.
        logger.log("BufferManager::insertIntoPool: Page " + pageName + " already exists. Replacing.");
        // Optionally write back the old page if it was modified
        // it->writePage(); // Uncomment if modification tracking is added
        pages.erase(it);
    }

    // Check if buffer is full before inserting
    if (pages.size() >= this->maxSize) // Use this->maxSize
    {
        // Buffer is full, evict the least recently used page (at the front)
        Page &evictedPage = pages.front();
        logger.log("BufferManager::insertIntoPool: Buffer full. Evicting page " + evictedPage.getPageName()); // Use getter
        // Write the evicted page back to disk if it has been modified
        // evictedPage.writePage(); // Uncomment if modification tracking is added
        pages.pop_front(); // Remove from the front
    }

    // Insert the new page at the back (most recently used)
    pages.push_back(page); // deque stores copies
    logger.log("BufferManager::insertIntoPool: Page " + pageName + " inserted.");
}

/**
 * @brief Writes all pages present in the pool buffer to disk.
 *        Useful for saving state before exiting or during checkpoints.
 */
void BufferManager::writeAllPages()
{
    logger.log("BufferManager::writeAllPages");
    logger.log("Writing " + to_string(pages.size()) + " pages to disk.");
    for (Page &page : pages) // Iterate by reference to call writePage()
    {
        // Check if the page has a valid name/index before writing
        if (!page.getPageName().empty() && page.pageIndex >= 0) // Access public member
        {
            // logger.log("Writing page: " + page.getPageName()); // Can be verbose
            page.writePage();
        }
        else
        {
            logger.log("BufferManager::writeAllPages: Skipping write for invalid/empty page.");
        }
    }
    logger.log("BufferManager::writeAllPages: Finished writing all pages.");
}

/**
 * @brief Deletes page from pool and disk space.
 *
 * @param tableName
 * @param pageIndex
 */
void BufferManager::deletePage(const string &tableName, int pageIndex) // Use const&
{
    logger.log("BufferManager::deletePage");
    // Use to_string for int pageIndex
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);

    // Remove the page from the buffer pool if it exists
    // Use erase-remove idiom
    auto it = remove_if(pages.begin(), pages.end(),
                        [&](const Page &p)
                        { return p.getPageName() == pageName; }); // Use getter

    if (it != pages.end())
    {
        pages.erase(it, pages.end()); // Erase the removed elements
        logger.log("BufferManager::deletePage: Page " + pageName + " removed from buffer.");
    }
    else
    {
        logger.log("BufferManager::deletePage: Page " + pageName + " not found in buffer.");
    }

    // Delete the corresponding file from disk
    if (remove(pageName.c_str()) != 0)
    {
        // Log an error if the file couldn't be deleted (it might not exist, which is okay)
        // Use perror for system errors
        // perror(("BufferManager::deletePage: Error deleting file " + pageName).c_str());
        logger.log("BufferManager::deletePage: Error deleting file " + pageName + ". It might not exist or permissions issue.");
    }
    else
    {
        logger.log("BufferManager::deletePage: File " + pageName + " deleted from disk.");
    }
}

/**
 * @brief Deletes all pages associated with a table from the buffer pool and disk.
 *
 * @param tableName
 */
void BufferManager::deleteTablePages(const string &tableName) // Use const&
{
    logger.log("BufferManager::deleteTablePages for table: " + tableName);
    string prefix = "../data/temp/" + tableName + "_Page";

    // Iterate through the buffer and remove pages belonging to the table
    // Using remove_if and erase idiom
    auto new_end = remove_if(pages.begin(), pages.end(),
                             [&](const Page &p)
                             {
                                 // Check if the page name starts with the table's page prefix
                                 return p.getPageName().rfind(prefix, 0) == 0; // Use getter
                             });

    int removedCount = distance(new_end, pages.end());
    if (new_end != pages.end())
    {
        pages.erase(new_end, pages.end()); // Erase the removed elements
    }

    if (removedCount > 0)
    {
        logger.log("BufferManager::deleteTablePages: Removed " + to_string(removedCount) + " pages of table " + tableName + " from buffer.");
    }
    else
    {
        logger.log("BufferManager::deleteTablePages: No pages of table " + tableName + " found in buffer.");
    }

    // Now, delete the actual files from disk. This requires knowing the page indices.
    // We need information from the TableCatalogue or Table object itself.
    Table *tablePtr = tableCatalogue.getTable(tableName); // Access global tableCatalogue
    if (tablePtr != nullptr)
    {
        // Accessing blockCount requires the full definition of Table, included via table.h
        int blockCount = tablePtr->blockCount;
        logger.log("BufferManager::deleteTablePages: Deleting " + to_string(blockCount) + " files for table " + tableName + " from disk.");
        for (int i = 0; i < blockCount; ++i)
        {
            string pageName = prefix + to_string(i);
            if (remove(pageName.c_str()) != 0)
            {
                // Log error, but continue trying to delete other pages
                // perror(("BufferManager::deleteTablePages: Error deleting file " + pageName).c_str());
                logger.log("BufferManager::deleteTablePages: Error deleting file " + pageName + ". It might not exist.");
            }
            else
            {
                // logger.log("BufferManager::deleteTablePages: File " + pageName + " deleted from disk."); // Can be verbose
            }
        }
        logger.log("BufferManager::deleteTablePages: Finished deleting files for table " + tableName + ".");
    }
    else
    {
        logger.log("BufferManager::deleteTablePages: Table " + tableName + " not found in catalogue. Cannot delete disk files systematically.");
        // Optionally, could try listing files in temp dir and deleting based on pattern, but less safe.
    }
}

// Destructor: Write all pages back to disk before the BufferManager is destroyed.
BufferManager::~BufferManager()
{
    logger.log("BufferManager::~BufferManager");
    writeAllPages();
    // The deque 'pages' will be automatically cleared when the object goes out of scope.
}

// --- Matrix Functions ---
// These seem less related to generic page buffering and more to matrix data handling.
// They operate directly on files named differently (_Block_ vs _Page).
// Consider if they belong in a Matrix class or MatrixCatalogue.

vector<vector<int>> BufferManager::getBlock(const string &matrixName, int rowBlockIndex, int colBlockIndex)
{
    // This function reads a specific block file, not interacting with the page buffer pool.
    logger.log("BufferManager::getBlock (Matrix Block Read)");
    string fileName = "../data/temp/" + matrixName + "_Block_" + to_string(rowBlockIndex) + "_" + to_string(colBlockIndex) + ".matrix";
    ifstream fin(fileName);
    // Assuming MATRIX_BLOCK_DIM is a global constant defining matrix block dimensions
    // Need error handling if MATRIX_BLOCK_DIM is not defined or accessible
    // For now, assume it's accessible and > 0
    if (MATRIX_BLOCK_DIM == 0) // Use MATRIX_BLOCK_DIM
    {
        logger.log("BufferManager::getBlock ERROR: MATRIX_BLOCK_DIM is zero.");
        cerr << "BufferManager::getBlock ERROR: MATRIX_BLOCK_DIM is zero." << endl;
        return {}; // Return empty vector
    }
    vector<vector<int>> blockData(MATRIX_BLOCK_DIM, vector<int>(MATRIX_BLOCK_DIM, 0)); // Initialize with 0 // Use MATRIX_BLOCK_DIM

    if (fin.is_open())
    {
        string line;
        int rowNum = 0;
        while (rowNum < (int)MATRIX_BLOCK_DIM && getline(fin, line)) // Use MATRIX_BLOCK_DIM
        {
            stringstream ss(line);
            int colNum = 0;
            while (colNum < (int)MATRIX_BLOCK_DIM && ss >> blockData[rowNum][colNum]) // Use MATRIX_BLOCK_DIM
            {
                colNum++;
            }
            // Optionally check if the correct number of columns were read
            if (colNum != (int)MATRIX_BLOCK_DIM) // Use MATRIX_BLOCK_DIM
            {
                logger.log("BufferManager::getBlock WARNING: Row " + to_string(rowNum) + " in " + fileName + " has incorrect column count (" + to_string(colNum) + "). Expected " + to_string(MATRIX_BLOCK_DIM)); // Use MATRIX_BLOCK_DIM
            }
            rowNum++;
        }
        // Optionally check if the correct number of rows were read
        if (rowNum != (int)MATRIX_BLOCK_DIM) // Use MATRIX_BLOCK_DIM
        {
            logger.log("BufferManager::getBlock WARNING: File " + fileName + " has incorrect row count (" + to_string(rowNum) + "). Expected " + to_string(MATRIX_BLOCK_DIM)); // Use MATRIX_BLOCK_DIM
        }
        fin.close();
    }
    else
    {
        logger.log("BufferManager::getBlock: Could not open file " + fileName);
        cerr << "BufferManager::getBlock: Could not open file " + fileName << endl;
        // Return the initialized (zero) block or handle error differently
    }

    return blockData;
}

void BufferManager::writeBlock(const string &matrixName, int rowBlockIndex, int colBlockIndex, const vector<vector<int>> &blockData)
{
    // This function writes a specific block file, not interacting with the page buffer pool.
    logger.log("BufferManager::writeBlock (Matrix Block Write)");
    string fileName = "../data/temp/" + matrixName + "_Block_" + to_string(rowBlockIndex) + "_" + to_string(colBlockIndex) + ".matrix";
    ofstream fout(fileName, ios::trunc);

    if (!fout)
    {
        logger.log("BufferManager::writeBlock ERROR: Cannot open file for writing: " + fileName);
        cerr << "BufferManager::writeBlock ERROR: Cannot open file for writing: " + fileName << endl;
        return;
    }

    for (const auto &row : blockData)
    {
        for (size_t i = 0; i < row.size(); ++i)
        {
            if (i != 0)
                fout << " ";
            fout << row[i];
        }
        fout << endl;
    }
    fout.close();
}

vector<vector<int>> BufferManager::loadMatrix(const string &matrixName, int &dimension)
{
    // This function reads matrix data from potentially multiple _Page files.
    // It doesn't seem to use the buffer pool mechanism.
    logger.log("BufferManager::loadMatrix (Direct File Read)");
    vector<vector<int>> matrixData;
    dimension = 0;
    int pageIndex = 0;

    while (true)
    {
        // Assumes matrix pages follow a specific naming convention
        string fileName = "../data/temp/" + matrixName + "_Page" + to_string(pageIndex) + ".matrix";
        ifstream fin(fileName);
        if (!fin.is_open())
        {
            if (pageIndex == 0)
            { // If first page doesn't exist, matrix likely doesn't exist
                logger.log("BufferManager::loadMatrix: File " + fileName + " not found. Matrix may not exist.");
                cerr << "BufferManager::loadMatrix: File " + fileName + " not found." << endl;
            }
            else
            {
                logger.log("BufferManager::loadMatrix: Reached end of pages at index " + to_string(pageIndex));
            }
            break; // Stop if a page file is not found
        }

        string line;
        while (getline(fin, line))
        {
            stringstream ss(line);
            vector<int> row;
            int value;
            while (ss >> value)
            {
                row.push_back(value);
            }
            if (!row.empty())
            { // Avoid adding empty rows if file has blank lines
                matrixData.push_back(row);
            }
        }
        fin.close();
        pageIndex++;
    }

    if (!matrixData.empty())
    {
        dimension = matrixData.size();
        // Basic check for square matrix if needed
        // if (dimension > 0 && (int)matrixData[0].size() != dimension) {
        //     logger.log("BufferManager::loadMatrix WARNING: Matrix " + matrixName + " is not square (" + to_string(dimension) + "x" + to_string(matrixData[0].size()) + ").");
        // }
    }
    else
    {
        logger.log("BufferManager::loadMatrix: No data loaded for matrix " + matrixName);
    }

    return matrixData;
}

void BufferManager::writeMatrix(const string &matrixName, const vector<vector<int>> &matrixData, int dimension)
{
    // This function writes matrix data to potentially multiple _Page files.
    // It doesn't seem to use the buffer pool mechanism.
    logger.log("BufferManager::writeMatrix (Direct File Write)");
    int pageIndex = 0;
    // Assuming MATRIX_BLOCK_DIM determines rows per page file? This seems inconsistent with Page class logic.
    // Let's assume pageSize means rows per file page here.
    int pageSize = MATRIX_BLOCK_DIM; // Using global MATRIX_BLOCK_DIM as page size? Revisit this logic. // Use MATRIX_BLOCK_DIM
    if (pageSize <= 0)
    {
        logger.log("BufferManager::writeMatrix ERROR: Invalid pageSize (MATRIX_BLOCK_DIM=" + to_string(pageSize) + ")"); // Use MATRIX_BLOCK_DIM
        cerr << "BufferManager::writeMatrix ERROR: Invalid pageSize." << endl;
        return;
    }

    if (dimension != (int)matrixData.size())
    {
        logger.log("BufferManager::writeMatrix WARNING: Provided dimension (" + to_string(dimension) + ") does not match matrix data size (" + to_string(matrixData.size()) + "). Using data size.");
        dimension = matrixData.size(); // Use actual size
    }

    for (int i = 0; i < dimension; i += pageSize)
    {
        string fileName = "../data/temp/" + matrixName + "_Page" + to_string(pageIndex++) + ".matrix";
        ofstream fout(fileName, ios::trunc);
        if (!fout)
        {
            logger.log("BufferManager::writeMatrix ERROR: Cannot open file for writing: " + fileName);
            cerr << "BufferManager::writeMatrix ERROR: Cannot open file for writing: " << fileName << endl;
            // Decide whether to stop or continue trying other pages
            continue; // Skip this page, try next
        }

        for (int j = i; j < i + pageSize && j < dimension; ++j)
        {
            // Check row bounds
            if (j >= (int)matrixData.size())
                break; // Should not happen if dimension is correct

            // Check column bounds if necessary (assuming square or consistent rows)
            // int expectedCols = (dimension > 0 && !matrixData.empty()) ? matrixData[0].size() : 0;

            for (size_t k = 0; k < matrixData[j].size(); ++k)
            {
                if (k != 0)
                    fout << " ";
                fout << matrixData[j][k];
            }
            fout << endl;
        }
        fout.close();
    }
}

// This overload seems specific. Is it intended to write a single row to a page file?
// It conflicts with the Page class's responsibility.
void BufferManager::writeBlock(const string &matrixName, int pageIndex, const vector<int> &rowData)
{
    logger.log("BufferManager::writeBlock (Single Row Write to Page " + to_string(pageIndex) + ")");
    string fileName = "../data/temp/" + matrixName + "_Page" + to_string(pageIndex) + ".matrix";
    // Using trunc mode will overwrite the entire file with just this row. Is this intended?
    ofstream fout(fileName, ios::trunc);

    if (!fout)
    {
        logger.log("BufferManager::writeBlock ERROR: Cannot open file for writing: " + fileName);
        cerr << "BufferManager::writeBlock ERROR: Cannot open file for writing: " << fileName << endl;
        return;
    }

    for (size_t i = 0; i < rowData.size(); ++i)
    {
        if (i != 0)
            fout << " ";
        fout << rowData[i];
    }
    fout << endl;
    fout.close();
}

/**
 * @brief Writes a serialized index page (vector<int>) directly to a file.
 * This bypasses the standard Page object abstraction and is intended specifically
 * for index structures that manage their own serialization format.
 *
 * @param pageName The full path and name of the index page file.
 * @param pageData The vector of integers representing the serialized node data.
 */
void BufferManager::writeIndexPage(const string &pageName, const vector<int> &pageData)
{
    logger.log("BufferManager::writeIndexPage writing to " + pageName);
    ofstream fout(pageName, ios::trunc | ios::out); // Overwrite existing file
    if (!fout)
    {
        logger.log("BufferManager::writeIndexPage ERROR: Cannot open file for writing: " + pageName);
        cerr << "BufferManager::writeIndexPage ERROR: Cannot open file for writing: " << pageName << endl;
        // Consider throwing an exception if this failure is critical
        return;
    }

    // Write the integers to the file, space-separated, one line per page (or handle formatting as needed)
    // Assuming the entire vector<int> represents the page content.
    // A simple approach is to write all ints on one line, though this might differ
    // from how regular data pages are formatted. Adjust if needed.
    for (size_t i = 0; i < pageData.size(); ++i)
    {
        if (i != 0)
            fout << " ";
        fout << pageData[i];
    }
    fout << endl; // Add a newline at the end

    fout.close();
    if (!fout.good())
    {
        logger.log("BufferManager::writeIndexPage ERROR: Error occurred during writing or closing file " + pageName);
        cerr << "BufferManager::writeIndexPage ERROR: Error occurred during writing or closing file " + pageName << endl;
        // Consider throwing an exception
    }
}

/**
 * @brief Removes all pages belonging to a specific table from the buffer pool.
 * Does NOT delete files from disk. Useful when table structure changes or is dropped.
 *
 * @param tableName The name of the table whose pages should be cleared from the buffer.
 */
void BufferManager::clearPoolForTable(const string &tableName) // Use const&
{
    logger.log("BufferManager::clearPoolForTable");
    // Construct the expected prefix for pages of the table.
    string prefix = "../data/temp/" + tableName + "_Page";
    // Remove from the deque pages whose pageName starts with prefix.
    // Use erase-remove idiom
    auto newEnd = remove_if(pages.begin(), pages.end(),
                            [&prefix](const Page &p) // Capture prefix by reference
                            {
                                // Check if pageName starts with prefix
                                return p.getPageName().rfind(prefix, 0) == 0; // Use getter
                            });
    if (newEnd != pages.end())
    {
        logger.log("BufferManager::clearPoolForTable: Clearing " + to_string(distance(newEnd, pages.end())) + " pages for table " + tableName + " from buffer.");
        pages.erase(newEnd, pages.end());
    }
    else
    {
        logger.log("BufferManager::clearPoolForTable: No pages found in buffer for table " + tableName);
    }
}
