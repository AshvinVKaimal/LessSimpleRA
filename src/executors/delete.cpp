#include "global.h"
#include <vector>
#include <string>
#include <regex> // For parsing
#include <numeric>
/**
 * @brief DELETE FROM table_name WHERE condition
 */

 bool syntacticParseDELETE()
 {
     logger.log("syntacticParseDELETE");
     // DELETE FROM relation_name WHERE column_name bin_op value (7 tokens)
     if (tokenizedQuery.size() != 7 || tokenizedQuery[1] != "FROM" || tokenizedQuery[3] != "WHERE") {
         cout << "SYNTAX ERROR: Expected DELETE FROM <table_name> WHERE <col_name> <op> <value>" << endl;
         return false;
     }
     parsedQuery.queryType = DELETE;
     parsedQuery.tableName = tokenizedQuery[2]; // Table to delete from

     // Reuse selection fields for WHERE clause
     parsedQuery.selectionFirstColumnName = tokenizedQuery[4]; // WHERE column
     string binaryOperator = tokenizedQuery[5]; // Operator
     string valueStr = tokenizedQuery[6]; // Value

      // Parse operator
     if (binaryOperator == "<") parsedQuery.selectionBinaryOperator = LESS_THAN;
     else if (binaryOperator == ">") parsedQuery.selectionBinaryOperator = GREATER_THAN;
     else if (binaryOperator == ">=" || binaryOperator == "=>") parsedQuery.selectionBinaryOperator = GEQ;
     else if (binaryOperator == "<=" || binaryOperator == "=<") parsedQuery.selectionBinaryOperator = LEQ;
     else if (binaryOperator == "==") parsedQuery.selectionBinaryOperator = EQUAL;
     else if (binaryOperator == "!=") parsedQuery.selectionBinaryOperator = NOT_EQUAL;
     else {
         cout << "SYNTAX ERROR: Unknown binary operator '" << binaryOperator << "' in DELETE." << endl;
         return false;
     }

      // Parse value (assuming integer literal)
     regex numeric("[-]?[0-9]+");
     if (regex_match(valueStr, numeric)) {
         parsedQuery.selectType = INT_LITERAL; // WHERE clause uses literal value
         try {
             parsedQuery.selectionIntLiteral = stoi(valueStr);
         } catch (const std::out_of_range& oor) {
             cout << "SYNTAX ERROR: Integer value '" << valueStr << "' is out of range in DELETE WHERE clause." << endl;
             return false;
         } catch (...) {
             cout << "SYNTAX ERROR: Invalid integer value '" << valueStr << "' in DELETE WHERE clause." << endl;
             return false;
         }
     } else {
         cout << "SYNTAX ERROR: DELETE currently only supports WHERE condition with integer literals." << endl;
         return false;
     }

     logger.log("Parsed DELETE: Table=" + parsedQuery.tableName +
               ", Column=" + parsedQuery.selectionFirstColumnName +
               ", Operator=" + binaryOperator +
               ", Value=" + valueStr);

     return true;
 }

 bool semanticParseDELETE()
{
    logger.log("semanticParseDELETE");
    // 1. Check table exists
    if (!tableCatalogue.isTable(parsedQuery.tableName)) {
        cout << "SEMANTIC ERROR: Table '" << parsedQuery.tableName << "' does not exist for DELETE." << endl;
        return false;
    }
    Table* table = tableCatalogue.getTable(parsedQuery.tableName); // Get table for column check

    // 2. Check WHERE column exists
    if (!table->isColumn(parsedQuery.selectionFirstColumnName)) {
         cout << "SEMANTIC ERROR: Column '" << parsedQuery.selectionFirstColumnName << "' does not exist in table '" << parsedQuery.tableName << "' for DELETE WHERE clause." << endl;
         return false;
     }
    // 3. Value type check (done in parser for literals)

    logger.log("Semantic parse DELETE successful for " + parsedQuery.tableName);
    return true;
}



void executeDELETE()
{
    logger.log("executeDELETE on table " + parsedQuery.tableName);

    Table* table = tableCatalogue.getTable(parsedQuery.tableName);
     if (!table) {
        cout << "ERROR: Table '" << parsedQuery.tableName << "' not found during execution." << endl;
        return;
    }

    // --- Index Invalidation ---
    if (!table->multiColumnIndexData.empty()) {
        logger.log("Invalidating all indices on table '" + table->tableName + "' due to DELETE operation.");
        table->clearIndex(); // Clear all column indices
    }

    // --- Prepare for Deletion ---
    int whereColIdx = table->getColumnIndex(parsedQuery.selectionFirstColumnName);
    int valueToCompare = parsedQuery.selectionIntLiteral;
    BinaryOperator op = parsedQuery.selectionBinaryOperator;

    if (whereColIdx < 0) { // Should be caught by semantic parse
        cout << "ERROR: WHERE column '" << parsedQuery.selectionFirstColumnName << "' not found." << endl;
        return;
    }

    logger.log("Starting physical delete scan...");
    long long totalRowsDeleted = 0;
    vector<uint> newRowsPerBlockCount; // Build the new count vector
    newRowsPerBlockCount.reserve(table->blockCount); // Reserve capacity

    // --- Iterate through Pages and Modify ---
    for (int pageIdx = 0; pageIdx < table->blockCount; ++pageIdx) {
        Page currentPage = bufferManager.getPage(table->tableName, pageIdx);
        int originalRowsInPage = table->rowsPerBlockCount[pageIdx]; // Get original count
        vector<vector<int>> rowsToKeep; // Store rows that *don't* match the condition

        // Read all rows from the current page
        vector<vector<int>> currentPageData;
        currentPageData.reserve(originalRowsInPage);
         for(int i=0; i<originalRowsInPage; ++i) {
            vector<int> row = currentPage.getRow(i);
            if (!row.empty()) {
                currentPageData.push_back(row);
            } else {
                 logger.log("executeDELETE WARNING: Reading page " + to_string(pageIdx) + ", got empty row at index " + to_string(i));
            }
        }
        // Validate read count
        if (currentPageData.size() != originalRowsInPage) {
             logger.log("executeDELETE ERROR: Read " + to_string(currentPageData.size()) + " rows from page " + to_string(pageIdx) + ", but expected " + to_string(originalRowsInPage) + ". Skipping page modification.");
             newRowsPerBlockCount.push_back(originalRowsInPage); // Keep original count for this page
             continue; // Skip to next page
         }

        // Filter rows: Keep rows that DON'T match the condition
        int rowsDeletedThisPage = 0;
        rowsToKeep.reserve(originalRowsInPage); // Reserve space
        for (const auto& row : currentPageData) {
            // Ensure row has the column before checking condition
            if (whereColIdx >= (int)row.size()) {
                 logger.log("executeDELETE WARNING: Row on page " + to_string(pageIdx) + " has too few columns for WHERE condition. Keeping row.");
                 rowsToKeep.push_back(row); // Keep potentially malformed rows? Or discard? Let's keep.
                 continue;
            }

            // Evaluate condition
            if (!evaluateBinOp(row[whereColIdx], valueToCompare, op)) {
                // Condition is FALSE -> Keep the row
                rowsToKeep.push_back(row);
            } else {
                // Condition is TRUE -> Delete the row (by not adding it to rowsToKeep)
                rowsDeletedThisPage++;
                logger.log("Deleting row at {" + to_string(pageIdx) + ", <original index unknown>} based on condition.");
            }
        }

        // Update page file and metadata *only if rows were deleted* from this page
        if (rowsDeletedThisPage > 0) {
            logger.log("Page " + to_string(pageIdx) + ": Deleted " + to_string(rowsDeletedThisPage) + " rows. New row count: " + to_string(rowsToKeep.size()));
            // Write the remaining rows back to the *same* page file
            bufferManager.writePage(table->tableName, pageIdx, rowsToKeep, rowsToKeep.size());
            newRowsPerBlockCount.push_back(rowsToKeep.size()); // Store the new count
            totalRowsDeleted += rowsDeletedThisPage;
        } else {
            // No rows deleted on this page, keep original count
            newRowsPerBlockCount.push_back(originalRowsInPage);
            logger.log("Page " + to_string(pageIdx) + ": No rows matched condition. Page unchanged.");
        }
    } 

    // --- Update Table Metadata ---
    if (totalRowsDeleted > 0) {
        table->rowCount -= totalRowsDeleted;
        table->rowsPerBlockCount = newRowsPerBlockCount; // Assign the updated vector
        logger.log("Total rows deleted: " + to_string(totalRowsDeleted) + ". New table row count: " + to_string(table->rowCount));
        cout << "DELETE completed successfully. " << totalRowsDeleted << " rows deleted." << endl;

        // Clear the buffer pool for this table as page contents and counts have changed
        logger.log("Clearing buffer pool cache for table: " + table->tableName + " after DELETE.");
        bufferManager.clearPoolForTable(table->tableName);

    } else {
        cout << "DELETE completed. No rows matched the WHERE condition." << endl;
        logger.log("No rows were deleted.");
    }

    return;
}