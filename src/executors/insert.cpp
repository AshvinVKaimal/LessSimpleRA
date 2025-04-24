#include "global.h"
#include <vector> 
#include <string> 
#include <sstream> 
#include <regex> 
/**
 * @brief 
 * SYNTAX: INSERT INTO relation_name ( column_1 = value_1, column_2 = value_2, ... )
 */

bool syntacticParseINSERT() {
    logger.log("syntacticParseINSERT");
    parsedQuery.queryType = INSERT;
    parsedQuery.insertColumnsAndValues.clear(); 

    if (tokenizedQuery.size() < 7 || tokenizedQuery[1] != "INTO" || tokenizedQuery[3] != "(") {
        cout << "SYNTAX ERROR: Expected INSERT INTO <table_name> ( col1 = val1 ... )" << endl;
        return false;
    }

    parsedQuery.tableName = tokenizedQuery[2];

    
    size_t closingParenIndex = tokenizedQuery.size(); // Default to end if not found
    for (size_t i = 4; i < tokenizedQuery.size(); ++i) {
        if (tokenizedQuery[i] == ")") {
            closingParenIndex = i;
            break;
        }
    }

    if (closingParenIndex == tokenizedQuery.size() || closingParenIndex == 4) { 
        cout << "SYNTAX ERROR: Missing or misplaced ')' in INSERT statement, or no column assignments provided." << endl;
        return false;
    }
    if (closingParenIndex != tokenizedQuery.size() - 1) {
        cout << "SYNTAX ERROR: Tokens found after closing parenthesis ')' in INSERT statement." << endl;
        return false;
    }

    // Parse column = value pairs
    regex numeric("[-]?[0-9]+");
    for (size_t i = 4; i < closingParenIndex;) {
    
        if (i >= closingParenIndex) break; 
        string colName = tokenizedQuery[i];
        i++;

        // Expecting '='
        if (i >= closingParenIndex || tokenizedQuery[i] != "=") {
            cout << "SYNTAX ERROR: Expected '=' after column name '" << colName << "' in INSERT." << endl; return false;
        }
        i++;

        // Expecting value (integer literal)
        if (i >= closingParenIndex) { cout << "SYNTAX ERROR: Expected value after '=' for column '" << colName << "'." << endl; return false;}
        string valStr = tokenizedQuery[i];
        i++;

        // Validate and convert value string NOW
        if (!regex_match(valStr, numeric)) {
             cout << "SYNTAX ERROR: Value '" << valStr << "' for column '" << colName << "' is not a valid integer." << endl;
             return false;
        }
        int value;
        try {
             value = stoi(valStr);
         } catch (const std::out_of_range& oor) {
             cout << "SYNTAX ERROR: Integer value '" << valStr << "' for column '" << colName << "' is out of range." << endl;
             return false;
         } catch (...) {
             cout << "SYNTAX ERROR: Cannot parse integer value '" << valStr << "' for column '" << colName << "'." << endl;
              return false;
         }

        // Store the validated pair
        parsedQuery.insertColumnsAndValues.push_back({colName, value});
        logger.log("Parsed INSERT pair: Col='" + colName + "', Val=" + to_string(value));
    }

    // Basic check: did we parse at least one pair?
    if (parsedQuery.insertColumnsAndValues.empty()) {
        cout << "SYNTAX ERROR: No column = value assignments found within parentheses." << endl;
        return false;
    }

    logger.log("Syntactic parse INSERT successful for table " + parsedQuery.tableName);
    return true;
}

 
bool semanticParseINSERT() {
    logger.log("semanticParseINSERT");
    // 1. Check table exists
        if (!tableCatalogue.isTable(parsedQuery.tableName)) {
        cout << "SEMANTIC ERROR: Table '" << parsedQuery.tableName << "' does not exist for INSERT." << endl;
        return false;
        }
    Table* table = tableCatalogue.getTable(parsedQuery.tableName);

    // 2. Validate column names and values from the pairs parsed syntactically
    regex numeric("[-]?[0-9]+");
    unordered_set<string> specifiedColumns; // To check for duplicate column assignments


    for (size_t i = 0; i < parsedQuery.insertColumnsAndValues.size(); ++i) {
        const string& colName = parsedQuery.insertColumnsAndValues[i].first;

        // a. Check if column exists in table
        if (!table->isColumn(colName)) {
                cout << "SEMANTIC ERROR: Column '" << colName << "' does not exist in table '" << parsedQuery.tableName << "'." << endl;
                return false;
        }
        // b. Check for duplicate column specification in the INSERT command itself
        if (specifiedColumns.count(colName)) {
                cout << "SEMANTIC ERROR: Column '" << colName << "' specified multiple times in INSERT statement." << endl;
                return false;
        }
        specifiedColumns.insert(colName);
        
    }

    logger.log("Semantic parse INSERT successful for " + parsedQuery.tableName);
    return true; 
}




void executeINSERT()
{
    logger.log("executeINSERT into " + parsedQuery.tableName);
    Table* table = tableCatalogue.getTable(parsedQuery.tableName);
    if (!table) {
        cout << "ERROR: Table '" << parsedQuery.tableName << "' not found during execution." << endl;
        return;
    }

    // 1. Create the initial row vector with default values (0)
    vector<int> rowToInsert(table->columnCount, 0);

    // 2. Populate the row vector with specified values
    bool colError = false;
    for (const auto& pair : parsedQuery.insertColumnsAndValues) {
        const string& colName = pair.first;
        int value = pair.second; // Value already parsed and validated

        int colIdx = table->getColumnIndex(colName);
        if (colIdx < 0) {
            // This should not happen if semantic parse worked
            cout << "ERROR: Column '" << colName << "' specified in INSERT not found in table '" << table->tableName << "' during execution." << endl;
            colError = true;
            break;
        }
        // Assign the specified value to the correct column index
        rowToInsert[colIdx] = value;
        logger.log("Setting column '" + colName + "' (index " + to_string(colIdx) + ") to value " + to_string(value));
    }

    if (colError) {
        return; // Stop if a specified column wasn't found
    }

    // 3. Add the fully constructed row to the table's pages
    int targetPageIdx = -1;
    int targetRowIdx = -1;

    if (table->blockCount > 0) { 
        int lastPageIndex = table->blockCount - 1;
        if (lastPageIndex >= (int)table->rowsPerBlockCount.size()){
            cout << "ERROR: Table metadata inconsistent (blockCount=" << table->blockCount
                 << " > rowsPerBlockCount.size()=" << table->rowsPerBlockCount.size() << ")." << endl;
             logger.log("executeINSERT ERROR: Metadata inconsistency detected.");
            return;
        }
        int rowsInLastPage = table->rowsPerBlockCount[lastPageIndex];

        if (rowsInLastPage < table->maxRowsPerBlock) {
            // Append to last page
            targetPageIdx = lastPageIndex;
            targetRowIdx = rowsInLastPage;
            logger.log("Appending row to existing page " + to_string(targetPageIdx) + " at row index " + to_string(targetRowIdx));

            Page targetPage = bufferManager.getPage(table->tableName, targetPageIdx);
            vector<vector<int>> pageData;
            pageData.reserve(rowsInLastPage + 1);
            for(int i=0; i < rowsInLastPage; ++i) {
                 vector<int> existingRow = targetPage.getRow(i);
                 if (!existingRow.empty()) { pageData.push_back(std::move(existingRow)); }
                 else { logger.log("executeINSERT WARNING: Reading page " + to_string(targetPageIdx) + ", got empty row at " + to_string(i) + ". Skipping."); }
            }
            if (pageData.size() != rowsInLastPage) {
                  logger.log("executeINSERT ERROR: Read " + to_string(pageData.size()) + " rows from page " + to_string(targetPageIdx) + ", expected " + to_string(rowsInLastPage) + ". Aborting.");
                  return;
            }
            pageData.push_back(rowToInsert);
            bufferManager.writePage(table->tableName, targetPageIdx, pageData, pageData.size());
            table->rowsPerBlockCount[lastPageIndex]++;

        } else { // Last page is full
            targetPageIdx = table->blockCount;
            targetRowIdx = 0;
            logger.log("Last page full. Creating new page " + to_string(targetPageIdx) + " for the row.");
            vector<vector<int>> newPageData = {rowToInsert}; // Initial data for new page
            bufferManager.writePage(table->tableName, targetPageIdx, newPageData, 1);
            table->blockCount++;
            table->rowsPerBlockCount.push_back(1);
        }
    } else { // Table is empty
        targetPageIdx = 0;
        targetRowIdx = 0;
        logger.log("Table empty. Creating first page (page 0) for the row.");
        vector<vector<int>> newPageData = {rowToInsert};
        bufferManager.writePage(table->tableName, targetPageIdx, newPageData, 1);
        table->blockCount = 1;
        table->rowsPerBlockCount.push_back(1);
    }

    // 4. Update table total row count
    table->rowCount++;

    // 5. Update the implicit indices for ALL columns
    if (targetPageIdx != -1 && targetRowIdx != -1) {
        logger.log("Updating all column indices for inserted row...");
        Table::RowLocation location = {targetPageIdx, targetRowIdx};
        for(int colIdx = 0; colIdx < table->columnCount; ++colIdx) {
            string colName = table->columns[colIdx];
            int key = rowToInsert[colIdx]; // Get the key from the *final* inserted row vector
            if (table->multiColumnIndexData.count(colName)) {
                 table->multiColumnIndexData[colName][key] = location;
                 // logger.log("Updated index for col '" + colName + "' key " + to_string(key)); // Verbose
            } else {
                logger.log("executeINSERT WARNING: No index map found for column '" + colName + "' to update.");
            }
        }
        logger.log("Finished updating indices for insert at location {" + to_string(targetPageIdx) + "," + to_string(targetRowIdx) + "}");
    } else {
         logger.log("executeINSERT ERROR: targetPageIdx or targetRowIdx invalid after insert logic. Index not updated.");
    }

    cout << "Row inserted into " << table->tableName << ". New Row Count: " << table->rowCount << endl;
    // Clear buffer pool pages for this table to  re-read with updated metadata next time
    logger.log("Clearing buffer pool cache for table: " + table->tableName);
    bufferManager.clearPoolForTable(table->tableName);

    return;
}