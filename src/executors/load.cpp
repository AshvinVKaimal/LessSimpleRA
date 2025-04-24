#include "global.h"
#include <string> // For string

/**
 * @brief
 * SYNTAX: LOAD relation_name
 */
bool syntacticParseLOAD()
{
    logger.log("syntacticParseLOAD");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR: Expected LOAD <relation_name>" << endl;
        return false;
    }
    parsedQuery.queryType = LOAD;
    parsedQuery.loadRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseLOAD()
{
    logger.log("semanticParseLOAD");
    if (tableCatalogue.isTable(parsedQuery.loadRelationName))
    {
        cout << "SEMANTIC ERROR: Relation '" << parsedQuery.loadRelationName << "' already exists" << endl;
        return false;
    }

    if (!isFileExists(parsedQuery.loadRelationName))
    {
        // isFileExists already logs path
        cout << "SEMANTIC ERROR: Data file for table '" << parsedQuery.loadRelationName << "' doesn't exist." << endl;
        return false;
    }
    logger.log("Semantic parse LOAD successful for " + parsedQuery.loadRelationName);
    return true;
}

void executeLOAD()
{
    logger.log("executeLOAD for " + parsedQuery.loadRelationName);

    Table *table = new Table(parsedQuery.loadRelationName);

    // The load() method now reads the header and calls blockify()
    if (table->load())
    {
        // Build indices after successful load
        table->buildIndices(); // Use member access

        // Insert the newly loaded table into the catalogue
        tableCatalogue.insertTable(table);
        cout << "Loaded Table. Column Count: " << table->columnCount << " Row Count: " << table->rowCount << endl;

        // --- Implicit Index Creation on ALL Columns ---
        table->clearIndex(); // Clear any previous indices just in case
        if (table->columnCount > 0) {
            logger.log("Implicitly building indices for all columns in table: " + table->tableName);

            // Iterate through all columns to create an index for each
            for (int columnIndex = 0; columnIndex < table->columnCount; ++columnIndex) {
                string colName = table->columns[columnIndex];
                logger.log("Building index for column: '" + colName + "' (Index " + to_string(columnIndex) + ")");
                // Create a new empty map for this column's index in the main index structure
                std::map<int, Table::RowLocation> currentColumnIndex;

                // Iterate through all data pages/rows again to populate this column's index
                for (int pageIdx = 0; pageIdx < table->blockCount; ++pageIdx) {
                    // Check if page index is valid for rowsPerBlockCount
                    if (pageIdx >= table->rowsPerBlockCount.size()) {
                        logger.log("executeLOAD index build ERROR: pageIdx " + to_string(pageIdx) + " >= rowsPerBlockCount size " + to_string(table->rowsPerBlockCount.size()));
                        continue; // Skip this potentially invalid page index
                    }

                    Page page = bufferManager.getPage(table->tableName, pageIdx);
                    int rowsInPage = table->rowsPerBlockCount[pageIdx];

                    for (int rowIdx = 0; rowIdx < rowsInPage; ++rowIdx) {
                        vector<int> rowData = page.getRow(rowIdx);
                        // Ensure rowData is not empty and has the indexed column
                        if (!rowData.empty() && columnIndex < (int)rowData.size()) { // Cast size() to int for comparison
                            int key = rowData[columnIndex];
                            // ASSUMPTION: Key is unique per column. If not, this overwrites, storing only the last location.
                            // If duplicates needed storing: table->multiColumnIndexData[colName][key].push_back({pageIdx, rowIdx});
                            currentColumnIndex[key] = {pageIdx, rowIdx}; // Add/update location in this column's specific map
                        } else if (rowData.empty()) {
                            logger.log("executeLOAD index build WARNING: Got empty row from page " + to_string(pageIdx) + " row " + to_string(rowIdx));
                        } else {
                            logger.log("executeLOAD index build WARNING: Row on page " + to_string(pageIdx) + " row " + to_string(rowIdx) + " has too few columns (needs index " + to_string(columnIndex) + ").");
                        }
                    }
                }
                // Store the completed map for this column in the main structure
                table->multiColumnIndexData[colName] = currentColumnIndex;
                logger.log("Index for column '" + colName + "' created. Size: " + to_string(currentColumnIndex.size()));
            } // End for each column
            cout << "Implicit indices created for all columns." << endl;
        } else {
             logger.log("Table has no columns. Skipping implicit index creation.");
        }

    } else {
         // load() failed, delete the partially created table object
         cout << "ERROR: Failed to load table '" << parsedQuery.loadRelationName << "'." << endl;
         delete table; // Clean up memory
    }
    return;
}