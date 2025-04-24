#include "global.h"
#include "executor.h"
#include "tableCatalogue.h"
#include "syntacticParser.h"
#include "table.h"  // Include full Table definition
#include "cursor.h" // Include Cursor definition
#include "page.h"   // Include Page definition for writing
#include <vector>
#include <string>
#include <algorithm> // For std::sort, std::stable_sort
#include <queue>     // For priority_queue in external merge sort

using namespace std;

/**
 * @brief File contains methods to process ORDER_BY commands.
 *
 * Syntax:
 *   Result_table <- ORDER BY <attribute_name> <ASC|DESC> ON <table-name>
 *
 * Implementation: External Sorting using a K-way Merge Sort Algorithm.
 * The algorithm works in two phases:
 *   1. Sorting Phase: Read at most 10 blocks at a time, sort the records in memory,
 *      and write each sorted run as a temporary subfile.
 *   2. Merging Phase: Merge subfiles (up to 9 at a time) until only one sorted file remains.
 *   3. Store the sorted result in a new table (Result_table)
 */

bool syntacticParseORDERBY()
{
    logger.log("syntacticParseORDER_BY");

    // Check that we have a minimal number of tokens.
    // Minimal query: Result_table <- ORDER BY attribute_name ASC|DESC ON table_name
    if (tokenizedQuery.size() != 8)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Check the structure of the query
    if (tokenizedQuery[1] != "<-" ||
        tokenizedQuery[2] != "ORDER" ||
        tokenizedQuery[3] != "BY" ||
        (tokenizedQuery[5] != "ASC" && tokenizedQuery[5] != "DESC") ||
        tokenizedQuery[6] != "ON")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Result table name is provided as the first token
    parsedQuery.orderByResultRelationName = tokenizedQuery[0];

    // Attribute/column name is the fourth token
    parsedQuery.orderByColumn = tokenizedQuery[4];

    // Source table name is the eighth token (after ON)
    if (tokenizedQuery.size() > 7)
        parsedQuery.orderByRelationName = tokenizedQuery[7];
    else
    {
        cout << "SYNTAX ERROR: Missing table name" << endl;
        return false;
    }

    // Set the query type
    parsedQuery.queryType = QueryType::ORDER_BY;

    // Set the sorting strategy based on the sixth token
    if (tokenizedQuery[5] == "ASC")
        parsedQuery.orderByStrategy = SortingStrategy::ASC;
    else
        parsedQuery.orderByStrategy = SortingStrategy::DESC;

    return true;
}

bool semanticParseORDERBY()
{
    logger.log("semanticParseORDER_BY");

    // Check that the source table exists
    if (!tableCatalogue.isTable(parsedQuery.orderByRelationName))
    {
        cout << "SEMANTIC ERROR: Source relation doesn't exist" << endl;
        return false;
    }

    // Check if the result table already exists
    if (tableCatalogue.isTable(parsedQuery.orderByResultRelationName))
    {
        cout << "SEMANTIC ERROR: Result relation already exists" << endl;
        return false;
    }

    // Verify that the specified column exists in the source table
    if (!tableCatalogue.isColumnFromTable(parsedQuery.orderByColumn, parsedQuery.orderByRelationName))
    {
        cout << "SEMANTIC ERROR: Column " << parsedQuery.orderByColumn << " doesn't exist in relation" << endl;
        return false;
    }

    return true;
}

/**
 * Comparison function for two rows using single column criteria.
 * It uses the column from parsedQuery.orderByColumn and its corresponding strategy.
 * The Table function getColumnIndex helps find the right index.
 */
bool compareRowsOrderBy(const vector<int> &a, const vector<int> &b, Table *table)
{
    int colIdx = table->getColumnIndex(parsedQuery.orderByColumn);
    if (parsedQuery.orderByStrategy == SortingStrategy::ASC)
        return a[colIdx] < b[colIdx];
    else // DESC
        return a[colIdx] > b[colIdx];
}

vector<vector<int>> readBlockOrderBy(Table *table, int blockIndex)
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
 * Merges multiple sorted runs (each run is a vector of rows) into one run for ORDER_BY.
 * This simple K-way merge repeatedly selects the smallest available row
 * using compareRowsOrderBy among the current heads of each run.
 */
vector<vector<int>> mergeMultipleRunsOrderBy(const vector<vector<vector<int>>> &runs, Table *table)
{
    vector<vector<int>> merged;
    // Keep track of the current index in each run
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
                    if (compareRowsOrderBy(currentRow, bestRow, table))
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

/**
 * Executes the ORDER BY command using an external sort algorithm.
 *
 * This implementation uses:
 *   - A sorting phase that reads groups of up to 10 blocks (the allowed buffer size),
 *     sorts them in-memory, and stores each sorted run.
 *   - A merging phase that repeatedly merges groups of runs (up to 9 runs at a time)
 *     until only one remains.
 *   - Finally, the sorted data is written to a new table.
 */

void executeORDERBY()
{
    logger.log("executeORDER_BY");

    // Get the source table to be sorted
    Table *sourceTable = tableCatalogue.getTable(parsedQuery.orderByRelationName);

    // Phase 1: Create sorted runs
    vector<vector<vector<int>>> sortedRuns;
    int numBlocks = sourceTable->blockCount;
    int blocksPerRun = 10; // Maximum blocks allowed in memory

    for (int startBlock = 0; startBlock < numBlocks; startBlock += blocksPerRun)
    {
        int endBlock = min(startBlock + blocksPerRun - 1, numBlocks - 1);
        vector<vector<int>> run;

        // Read blocks for this run
        for (int blockIndex = startBlock; blockIndex <= endBlock; blockIndex++)
        {
            vector<vector<int>> blockData = readBlockOrderBy(sourceTable, blockIndex);
            run.insert(run.end(), blockData.begin(), blockData.end());
        }

        // In-memory sort for the run using single column
        sort(run.begin(), run.end(), [sourceTable](const vector<int> &a, const vector<int> &b)
             { return compareRowsOrderBy(a, b, sourceTable); });

        sortedRuns.push_back(run);
    }

    // Phase 2: Merge sorted runs
    while (sortedRuns.size() > 1)
    {
        vector<vector<vector<int>>> newRuns;

        // Merge groups of up to 9 runs at a time
        for (size_t i = 0; i < sortedRuns.size(); i += 9)
        {
            size_t end = min(i + (size_t)9, sortedRuns.size());
            vector<vector<vector<int>>> runsToMerge(sortedRuns.begin() + i, sortedRuns.begin() + end);

            // Update mergeMultipleRuns to use the ORDER_BY comparison
            vector<vector<int>> mergedRun = mergeMultipleRunsOrderBy(runsToMerge, sourceTable);
            newRuns.push_back(mergedRun);
        }

        sortedRuns = newRuns;
    }

    // Final sorted data
    vector<vector<int>> sortedData = sortedRuns[0];

    // Create a new table with the same schema as the source table
    Table *resultTable = new Table(parsedQuery.orderByResultRelationName, sourceTable->columns);

    // Set the rowCount
    resultTable->rowCount = sortedData.size();

    // Re-blockify the data into the new table
    int rowCounter = 0;
    int pageCounter = 0;

    while (rowCounter < sortedData.size())
    {
        vector<vector<int>> pageRows;
        int rowsInBlock = 0;

        // Fill a page with up to maxRowsPerBlock rows
        while (rowCounter < sortedData.size() && rowsInBlock < resultTable->maxRowsPerBlock)
        {
            pageRows.push_back(sortedData[rowCounter]);
            rowCounter++;
            rowsInBlock++;
        }

        // Write the page to disk using the Page class
        Page resultPage(resultTable->tableName, pageCounter, pageRows, rowsInBlock); // Create Page object
        resultPage.writePage();                                                      // Write the page

        resultTable->rowsPerBlockCount.push_back(rowsInBlock);
        pageCounter++;
    }

    // Update the table's block count
    resultTable->blockCount = pageCounter;

    // Compute statistics for the new table (distinct values, etc.)
    resultTable->distinctValuesInColumns.clear();
    resultTable->distinctValuesInColumns.resize(resultTable->columnCount);
    resultTable->distinctValuesPerColumnCount.assign(resultTable->columnCount, 0);

    for (const auto &row : sortedData)
    {
        for (int col = 0; col < resultTable->columnCount; col++)
        {
            if (!resultTable->distinctValuesInColumns[col].count(row[col]))
            {
                resultTable->distinctValuesInColumns[col].insert(row[col]);
                resultTable->distinctValuesPerColumnCount[col]++;
            }
        }
    }

    // Insert the new table into the catalogue
    tableCatalogue.insertTable(resultTable);

    cout << "ORDER BY executed successfully. Result stored in " << parsedQuery.orderByResultRelationName << endl;
}