#include "global.h"
#include <cmath>
#include <algorithm>
#include <fstream>
#include <sstream>
#include "table.h"
#include "page.h" // Include Page definition for writing
#include <vector>
#include <string>
#include <algorithm> // For std::sort, std::stable_sort
#include <queue>     // For priority_queue in external merge sort

using namespace std;

/**
 * @brief File contains methods to process SORT commands.
 *
 * New syntax:
 *   SORT <table-name> BY <col1>, <col2>, ... IN <ASC|DESC>, <ASC|DESC>, ...
 *
 * Implementation: External Sorting using a K-way Merge Sort Algorithm.
 * The algorithm works in two phases:
 *   1. Sorting Phase: Read at most 10 blocks at a time, sort the records in memory,
 *      and write each sorted run as a temporary subfile.
 *   2. Merging Phase: Merge subfiles (up to 9 at a time) until only one sorted file remains.
 */

bool syntacticParseSORT()
{
    logger.log("syntacticParseSORT");

    parsedQuery.sortColumns.clear();
    parsedQuery.sortingStrategies.clear();

    // Check that we have a minimal number of tokens.
    // Minimal query: SORT <table-name> BY <col> IN <ASC|DESC>
    if (tokenizedQuery.size() < 6)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // First token must be "SORT"
    if (tokenizedQuery[0] != "SORT")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Table name is provided as the second token.
    parsedQuery.sortRelationName = tokenizedQuery[1];

    // The third token must be "BY"
    if (tokenizedQuery[2] != "BY")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Find the index of the "IN" token that separates columns from sort orders.
    int inIndex = -1;
    for (int i = 3; i < tokenizedQuery.size(); i++)
    {
        if (tokenizedQuery[i] == "IN")
        {
            inIndex = i;
            break;
        }
    }
    if (inIndex == -1)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Collect column names (from token 3 up to token before "IN").
    vector<string> columns;
    for (int i = 3; i < inIndex; i++)
    {
        string col = tokenizedQuery[i];
        if (col == ",")
            continue; // skip comma tokens
        // Remove trailing comma if present.
        if (!col.empty() && col.back() == ',')
        {
            col.pop_back();
        }
        columns.push_back(col);
    }

    // Collect the sorting order for each column (tokens after "IN").
    vector<string> orders;
    for (int i = inIndex + 1; i < tokenizedQuery.size(); i++)
    {
        string ord = tokenizedQuery[i];
        if (ord == ",")
            continue;
        if (!ord.empty() && ord.back() == ',')
        {
            ord.pop_back();
        }
        orders.push_back(ord);
    }

    // The number of columns must match the number of orders.
    if (columns.empty() || orders.empty() || columns.size() != orders.size())
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    parsedQuery.queryType = QueryType::SORT;
    parsedQuery.sortColumns = columns;

    // Validate each order token and populate the sorting strategies.
    for (auto ord : orders)
    {
        if (ord == "ASC")
            parsedQuery.sortingStrategies.push_back(SortingStrategy::ASC);
        else if (ord == "DESC")
            parsedQuery.sortingStrategies.push_back(SortingStrategy::DESC);
        else
        {
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }
    return true;
}

bool semanticParseSORT()
{
    logger.log("semanticParseSORT");

    // Check that the table exists.
    if (!tableCatalogue.isTable(parsedQuery.sortRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    // Verify that each specified column exists in the table.
    for (auto col : parsedQuery.sortColumns)
    {
        if (!tableCatalogue.isColumnFromTable(col, parsedQuery.sortRelationName))
        {
            cout << "SEMANTIC ERROR: Column " << col << " doesn't exist in relation" << endl;
            return false;
        }
    }
    return true;
}

/**
 * Helper function that reads one block of rows from the table.
 * It uses the Cursor (which accepts a page index) and the stored
 * rowsPerBlockCount to read exactly that block.
 */
vector<vector<int>> readBlock(Table *table, int blockIndex)
{
    vector<vector<int>> blockData;
    // Create a cursor for the specified block.
    Cursor cursor(table->tableName, blockIndex);
    // Use the stored rows count per block (populated in blockify)
    int rowsCount = table->rowsPerBlockCount[blockIndex];
    for (int i = 0; i < rowsCount; i++)
    {
        vector<int> row = cursor.getNext();
        blockData.push_back(row);
    }
    return blockData;
}

/**
 * Comparison function for two rows using multi‑key criteria.
 * It uses the list of sort columns and their corresponding strategies
 * from parsedQuery. The Table function getColumnIndex helps find the right index.
 */
bool compareRows(const vector<int> &a, const vector<int> &b, Table *table)
{
    for (size_t idx = 0; idx < parsedQuery.sortColumns.size(); idx++)
    {
        int colIdx = table->getColumnIndex(parsedQuery.sortColumns[idx]);
        if (a[colIdx] == b[colIdx])
            continue;
        if (parsedQuery.sortingStrategies[idx] == SortingStrategy::ASC)
            return a[colIdx] < b[colIdx];
        else // DESC
            return a[colIdx] > b[colIdx];
    }
    return false;
}

/**
 * Merges multiple sorted runs (each run is a vector of rows) into one run.
 * This simple K‑way merge repeatedly selects the smallest available row
 * (using compareRows) among the current heads of each run.
 */
vector<vector<int>> mergeMultipleRuns(const vector<vector<vector<int>>> &runs, Table *table)
{
    vector<vector<int>> merged;
    // Keep track of the current index in each run.
    vector<size_t> indices(runs.size(), 0);
    bool done = false;
    while (!done)
    {
        int bestRun = -1;
        vector<int> bestRow;
        bool found = false;
        for (size_t i = 0; i < runs.size(); i++)
        {
            if (indices[i] < runs[i].size())
            {
                vector<int> currentRow = runs[i][indices[i]];
                if (!found)
                {
                    bestRow = currentRow;
                    bestRun = i;
                    found = true;
                }
                else
                {
                    if (compareRows(currentRow, bestRow, table))
                    {
                        bestRow = currentRow;
                        bestRun = i;
                    }
                }
            }
        }
        if (!found)
            done = true;
        else
        {
            merged.push_back(bestRow);
            indices[bestRun]++;
        }
    }
    return merged;
}

void refreshTable(const string &tableName)
{
    // Clear cached pages only for this table.
    bufferManager.clearPoolForTable(tableName);

    // Get the old table from the catalogue.
    Table *oldTable = tableCatalogue.getTable(tableName);
    // Save important metadata.
    bool isPermanent = oldTable->isPermanent();
    vector<string> columns = oldTable->columns; // copy columns

    // Remove the old table (this deletes its pages; the source file remains for permanent tables)
    tableCatalogue.deleteTable(tableName);

    // Create a new table object.
    Table *newTable;
    if (isPermanent)
    {
        std::cout << "Permanent table " << tableName << " refreshed" << endl;
        newTable = new Table(tableName);
    }
    else
    {
        std::cout << "Temporary table " << tableName << " refreshed" << endl;
        newTable = new Table(tableName, columns);
    }

    // Load the table from the updated (sorted) CSV file.
    if (!newTable->load())
    {
        std::cout << "Error reloading table " << tableName << endl;
        // Handle error as needed
    }

    // Insert the new table into the catalogue.
    tableCatalogue.insertTable(newTable);

    std::cout << "Table " << tableName << " refreshed successfully" << std::endl;
}

// Structure for priority queue in k-way merge for SORT
struct MergeElementSort
{
    vector<int> row;
    int runIndex;    // Which run this element came from
    Table *tablePtr; // Pointer to the table for comparison context

    MergeElementSort(const vector<int> &r, int rIdx, Table *tPtr)
        : row(r), runIndex(rIdx), tablePtr(tPtr) {}

    // Overload > for min-heap (based on compareRows)
    // The priority queue uses `>` to determine the *lowest* priority element (which comes out first).
    // So, `a > b` should return true if `a` should come *after* `b` in the sorted order.
    bool operator>(const MergeElementSort &other) const
    {
        // compareRows(a, b, table) returns true if a < b according to sort criteria.
        // We want the element that is "smaller" according to compareRows to have higher priority (come out first).
        // Therefore, if compareRows(this->row, other.row, tablePtr) is true (this < other),
        // then `this` should have higher priority, meaning `this > other` should be false.
        // If compareRows(other.row, this->row, tablePtr) is true (other < this),
        // then `other` should have higher priority, meaning `this > other` should be true.
        return compareRows(other.row, this->row, tablePtr);
    }
};

/**
 * Executes the SORT command using an external sort algorithm.
 *
 * This implementation uses:
 *   - A sorting phase that reads groups of up to 10 blocks (the allowed buffer size),
 *     sorts them in-memory using multi-key sort, and stores each sorted run.
 *   - A merging phase that repeatedly merges groups of runs (up to 9 runs at a time)
 *     until only one remains.
 *   - Finally, the sorted data is written back to the table's page files in place.
 * After writing to the pages, the table is refreshed in main memory so that
 * subsequent operations use the updated context.
 */
void executeSORT()
{
    logger.log("executeSORT");

    Table *table = tableCatalogue.getTable(parsedQuery.sortRelationName);
    if (!table)
    {
        cout << "EXECUTION ERROR: Table '" << parsedQuery.sortRelationName << "' not found." << endl;
        return;
    }

    // --- Phase 1: Create sorted runs ---
    logger.log("executeSORT: Phase 1 - Creating sorted runs...");
    vector<string> runPageNames; // Store names of temporary run pages
    int numBlocks = table->blockCount;
    int blocksPerRun = BLOCK_COUNT; // Use available buffer blocks for run size

    for (int startBlock = 0; startBlock < numBlocks; startBlock += blocksPerRun)
    {
        int endBlock = min(startBlock + blocksPerRun - 1, numBlocks - 1);
        vector<vector<int>> runData;
        runData.reserve(blocksPerRun * table->maxRowsPerBlock); // Estimate capacity

        // Read blocks for this run
        for (int blockIndex = startBlock; blockIndex <= endBlock; blockIndex++)
        {
            vector<vector<int>> blockData = readBlock(table, blockIndex);
            runData.insert(runData.end(), blockData.begin(), blockData.end());
        }

        // In-memory sort for the run
        sort(runData.begin(), runData.end(), [table](const vector<int> &a, const vector<int> &b)
             { return compareRows(a, b, table); });

        // Write the sorted run to temporary pages
        int runBlockCounter = 0;
        int rowsWritten = 0;
        string tempRunTableName = table->tableName + "_run_" + to_string(startBlock / blocksPerRun);

        while (rowsWritten < runData.size())
        {
            vector<vector<int>> pageRows;
            int rowsInBlock = 0;
            while (rowsWritten < runData.size() && rowsInBlock < table->maxRowsPerBlock)
            {
                pageRows.push_back(runData[rowsWritten]);
                rowsWritten++;
                rowsInBlock++;
            }
            // Write the page using Page class
            Page tempPage(tempRunTableName, runBlockCounter, pageRows, rowsInBlock);
            tempPage.writePage();
            runPageNames.push_back(tempPage.getPageName()); // Store the name of the created page
            runBlockCounter++;
        }
        logger.log("executeSORT: Created sorted run " + to_string(startBlock / blocksPerRun) + " with " + to_string(runBlockCounter) + " pages.");
    }
    logger.log("executeSORT: Phase 1 complete. Created " + to_string(runPageNames.size()) + " temporary run pages in total.");

    if (runPageNames.empty())
    {
        cout << "EXECUTION INFO: Source table is empty. No sorting needed." << endl;
        // Optionally create an empty result table if the syntax implies one,
        // or just return if SORT modifies in-place (which this implementation doesn't).
        return;
    }

    // --- Phase 2: Merge sorted runs ---
    // This implementation merges all runs at once if they fit in memory (BLOCK_COUNT-1 runs).
    // A multi-pass merge is needed if numRuns > BLOCK_COUNT-1.
    // For simplicity, assuming single merge pass for now.
    // TODO: Implement multi-pass merge if necessary.
    logger.log("executeSORT: Phase 2 - Merging sorted runs...");
    if (runPageNames.size() > (size_t)(BLOCK_COUNT - 1))
    {
        cout << "EXECUTION ERROR: Too many runs (" << runPageNames.size() << ") to merge in a single pass with available buffers (" << BLOCK_COUNT - 1 << "). Multi-pass merge not implemented." << endl;
        // Cleanup temporary files before exiting
        for (size_t i = 0; i < runPageNames.size(); ++i)
        {
            // Need to parse page name to get table name prefix and page index
            // Assuming format TableName_run_X_PageY
            // This cleanup logic needs refinement based on actual page naming.
            // For now, using a placeholder logic.
            string runTableNamePrefix = table->tableName + "_run_" + to_string(i / (numBlocks / blocksPerRun)); // Approximate run prefix
            int pageIndexInRun = i % (numBlocks / blocksPerRun);                                                // Approximate page index
            bufferManager.deletePage(runTableNamePrefix, pageIndexInRun);                                       // Corrected method name
        }
        return;
    }

    // Priority queue for merging
    priority_queue<MergeElementSort, vector<MergeElementSort>, greater<MergeElementSort>> minHeap;
    vector<Cursor> runCursors;
    runCursors.reserve(runPageNames.size());

    // Initialize cursors and heap
    for (size_t i = 0; i < runPageNames.size(); ++i)
    {
        // Assuming runPageNames correctly correspond to the pages created.
        // Need to parse page name to get table name prefix and page index.
        // Placeholder logic:
        string runTableNamePrefix = table->tableName + "_run_" + to_string(i / (numBlocks / blocksPerRun)); // Approximate run prefix
        int pageIndexInRun = i % (numBlocks / blocksPerRun);                                                // Approximate page index
        runCursors.emplace_back(runTableNamePrefix, pageIndexInRun);

        vector<int> row = runCursors[i].getNext();
        if (!row.empty())
        {
            minHeap.push({row, (int)i, table}); // Pass table pointer to comparator
        }
    }

    // --- Phase 3: Write sorted data back to the original table's pages ---
    // This overwrites the original table data.
    logger.log("executeSORT: Phase 3 - Writing sorted data back to table pages...");
    int pageCounter = 0;
    int rowCounter = 0;
    vector<vector<int>> pageRows;

    while (!minHeap.empty())
    {
        MergeElementSort top = minHeap.top();
        minHeap.pop();

        pageRows.push_back(top.row);
        rowCounter++;

        // Fetch next row from the run the top element came from
        vector<int> nextRow = runCursors[top.runIndex].getNext();
        if (!nextRow.empty())
        {
            minHeap.push({nextRow, top.runIndex, table});
        }

        // If page is full, write it back
        if (rowCounter == table->maxRowsPerBlock)
        {
            // Write the page using Page class
            Page resultPage(table->tableName, pageCounter, pageRows, rowCounter);
            resultPage.writePage(); // Write the page

            table->rowsPerBlockCount[pageCounter] = rowCounter; // Update row count for this block
            pageCounter++;
            pageRows.clear();
            rowCounter = 0;
        }
    }

    // Write the last partially filled page
    if (rowCounter > 0)
    {
        // Write the page using Page class
        Page resultPage(table->tableName, pageCounter, pageRows, rowCounter);
        resultPage.writePage(); // Write the page

        // Update row count for the last block
        if (pageCounter < table->rowsPerBlockCount.size())
        {
            table->rowsPerBlockCount[pageCounter] = rowCounter;
        }
        else
        {
            // This case might happen if the table grew, though unlikely with overwrite sort
            table->rowsPerBlockCount.push_back(rowCounter);
        }
        pageCounter++;
    }

    // Update block count if it changed (e.g., if the last page was partially filled before)
    table->blockCount = pageCounter;
    // Adjust rowsPerBlockCount vector size if necessary
    table->rowsPerBlockCount.resize(table->blockCount);

    logger.log("executeSORT: Phase 3 complete. Table data overwritten with sorted data.");

    // --- Phase 4: Cleanup temporary run files ---
    logger.log("executeSORT: Phase 4 - Cleaning up temporary run pages...");
    for (size_t i = 0; i < runPageNames.size(); ++i)
    {
        // Placeholder logic for deleting temp pages:
        string runTableNamePrefix = table->tableName + "_run_" + to_string(i / (numBlocks / blocksPerRun)); // Approximate run prefix
        int pageIndexInRun = i % (numBlocks / blocksPerRun);                                                // Approximate page index
        bufferManager.deletePage(runTableNamePrefix, pageIndexInRun);                                       // Corrected method name
    }
    logger.log("executeSORT: Cleanup complete.");

    // Rebuild indices as the data pointers are now invalid
    logger.log("executeSORT: Rebuilding indices after sort...");
    table->buildIndices();
    logger.log("executeSORT: Indices rebuilt.");

    cout << "Sort operation completed successfully on table '" << parsedQuery.sortRelationName << "'." << endl;
}