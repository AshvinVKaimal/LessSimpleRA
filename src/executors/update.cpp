#include "global.h"

/**
 * @brief Placeholder for UPDATE command execution.
 * SYNTAX: UPDATE table_name SET column_name = value WHERE column_name bin_op value
 */

bool syntacticParseUPDATE() {
    logger.log("syntacticParseUPDATE");
    if (tokenizedQuery.size() != 10 ||
        tokenizedQuery[2] != "WHERE" ||  // WHERE should be at index 2
        tokenizedQuery[6] != "SET" ||    // SET should be at index 6
        tokenizedQuery[8] != "=") {      // = should be at index 8
        cout << "SYNTAX ERROR: Expected UPDATE <table_name> WHERE <col> <op> <val> SET <col> = <val>" << endl;
        return false;
    }
    parsedQuery.queryType = UPDATE;
    parsedQuery.tableName = tokenizedQuery[1]; // Table name is at index 1

    // Parse WHERE clause (using selection fields)
    parsedQuery.selectionFirstColumnName = tokenizedQuery[3]; // WHERE column is at index 3
    string whereOpStr = tokenizedQuery[4]; // WHERE operator is at index 4
    string whereValueStr = tokenizedQuery[5]; // WHERE value is at index 5

    // Parse SET clause (using rename fields as placeholders)
    parsedQuery.renameFromColumnName = tokenizedQuery[7]; // SET column is at index 7
    string setValueStr = tokenizedQuery[9]; // SET value is at index 9

    // -- Validate WHERE Operator --
    if (whereOpStr == "<") parsedQuery.selectionBinaryOperator = LESS_THAN;
    else if (whereOpStr == ">") parsedQuery.selectionBinaryOperator = GREATER_THAN;
    else if (whereOpStr == ">=" || whereOpStr == "=>") parsedQuery.selectionBinaryOperator = GEQ;
    else if (whereOpStr == "<=" || whereOpStr == "=<") parsedQuery.selectionBinaryOperator = LEQ;
    else if (whereOpStr == "==" || whereOpStr == "=") parsedQuery.selectionBinaryOperator = EQUAL; // Handle both == and =
    else if (whereOpStr == "!=") parsedQuery.selectionBinaryOperator = NOT_EQUAL;
    else {
        cout << "SYNTAX ERROR: Unknown binary operator '" << whereOpStr << "' in UPDATE WHERE clause." << endl;
        return false;
    }

    // -- Validate WHERE Value (must be integer literal) --
    regex numeric("[-]?[0-9]+");
    if (regex_match(whereValueStr, numeric)) {
        parsedQuery.selectType = INT_LITERAL; // WHERE clause uses literal
        try {
            parsedQuery.selectionIntLiteral = stoi(whereValueStr);
        } catch (const std::out_of_range& oor) {
            cout << "SYNTAX ERROR: Integer WHERE value '" << whereValueStr << "' is out of range." << endl;
            return false;
        } catch (...) {
            cout << "SYNTAX ERROR: Invalid integer WHERE value '" << whereValueStr << "'." << endl;
            return false;
        }
    } else {
        cout << "SYNTAX ERROR: UPDATE currently only supports WHERE condition with integer literals." << endl;
        return false;
    }

    // -- Validate SET Value (must be integer literal) --
     if (regex_match(setValueStr, numeric)) {
         try {
             // Store the SET value string in renameToColumnName field
             parsedQuery.renameToColumnName = setValueStr;
         } catch (const std::out_of_range& oor) {
             cout << "SYNTAX ERROR: Integer SET value '" << setValueStr << "' is out of range." << endl;
             return false;
         } catch (...) {
             cout << "SYNTAX ERROR: Invalid integer SET value '" << setValueStr << "'." << endl;
              return false;
         }
     } else {
         cout << "SYNTAX ERROR: UPDATE currently only supports SET with integer literals." << endl;
         return false;
     }


    logger.log("Parsed UPDATE: Table=" + parsedQuery.tableName +
               ", WHERE " + parsedQuery.selectionFirstColumnName + " " + whereOpStr + " " + whereValueStr +
               ", SET " + parsedQuery.renameFromColumnName + " = " + parsedQuery.renameToColumnName);

    return true;
}

bool semanticParseUPDATE() {
    logger.log("semanticParseUPDATE");
    // 1. Check table exists
     if (!tableCatalogue.isTable(parsedQuery.tableName)) {
         cout << "SEMANTIC ERROR: Table '" << parsedQuery.tableName << "' does not exist for UPDATE." << endl;
         return false;
     }
     Table* table = tableCatalogue.getTable(parsedQuery.tableName);

    // 2. Check WHERE column exists
    if (!table->isColumn(parsedQuery.selectionFirstColumnName)) { 
         cout << "SEMANTIC ERROR: Column '" << parsedQuery.selectionFirstColumnName << "' does not exist in table '" << parsedQuery.tableName << "' for WHERE clause." << endl;
         return false;
     }

    // 3. Check SET column exists
     if (!table->isColumn(parsedQuery.renameFromColumnName)) { 
        cout << "SEMANTIC ERROR: Column '" << parsedQuery.renameFromColumnName << "' does not exist in table '" << parsedQuery.tableName << "' for SET clause." << endl;
        return false;
     }


    logger.log("Semantic parse UPDATE successful for " + parsedQuery.tableName);
    return true;
}

void executeUPDATE()
{
    logger.log("executeUPDATE on table " + parsedQuery.tableName); // Use tableName as parsed
    Table* table = tableCatalogue.getTable(parsedQuery.tableName);
     if (!table) {
        cout << "ERROR: Table '" << parsedQuery.tableName << "' not found during execution." << endl;
        return;
    }

    if (!table->multiColumnIndexData.empty()) {
        logger.log("Invalidating all indices on table '" + table->tableName + "' due to UPDATE operation.");
        table->clearIndex(); // Clear all column indices
    }

    // Get column indices based on *correct* parsed query structure
    int whereColIdx = table->getColumnIndex(parsedQuery.selectionFirstColumnName); // WHERE column index
    int targetColIdx = table->getColumnIndex(parsedQuery.renameFromColumnName);    // SET column index
    string opStr = ""; // Store operator string for logging/direct comparison if needed
    BinaryOperator opEnum = parsedQuery.selectionBinaryOperator; // Get WHERE operator enum
    int condVal = parsedQuery.selectionIntLiteral;               // Get WHERE value
    int newVal = 0;                                              // Initialize SET value

    // Convert SET value string (stored in renameToColumnName) to int
     try {
         newVal = stoi(parsedQuery.renameToColumnName);
     } catch (...) {
         cout << "ERROR: Cannot convert SET value '" << parsedQuery.renameToColumnName << "' to integer during execution." << endl;
         return; 
     }

     // Get operator string representation for logging (optional)
     // This logic should match the one in syntacticParseUPDATE
     switch(opEnum) {
         case LESS_THAN: opStr = "<"; break;
         case GREATER_THAN: opStr = ">"; break;
         case LEQ: opStr = "<="; break;
         case GEQ: opStr = ">="; break;
         case EQUAL: opStr = "=="; break;
         case NOT_EQUAL: opStr = "!="; break;
         default: opStr = "UNKNOWN_OP"; break;
     }


    // Check if column indices are valid
     if (whereColIdx < 0 || targetColIdx < 0) {
         cout << "ERROR: Column index not found during UPDATE execution (WHERE Col Idx: " << whereColIdx << ", SET Col Idx: " << targetColIdx << ")." << endl;
         return;
     }

    logger.log("Starting physical update scan for: UPDATE " + table->tableName +
               " WHERE " + table->columns[whereColIdx] + " " + opStr + " " + to_string(condVal) +
               " SET " + table->columns[targetColIdx] + " = " + to_string(newVal));

    long long totalRowsUpdated = 0;

    for (int pageIdx = 0; pageIdx < table->blockCount; ++pageIdx) {
        bool pageModified = false;
        Page currentPage = bufferManager.getPage(table->tableName, pageIdx);
        // Ensure rowsPerBlockCount has an entry for this page index
        if (pageIdx >= (int)table->rowsPerBlockCount.size()) {
             logger.log("executeUPDATE ERROR: Metadata inconsistency - page index " + to_string(pageIdx) + " out of bounds for rowsPerBlockCount. Skipping page.");
             continue;
         }
        int rowsInPage = table->rowsPerBlockCount[pageIdx];

        // Read all rows into a mutable buffer
        vector<vector<int>> pageData;
        pageData.reserve(rowsInPage);
        int rowsReadFromPage = 0;
        for(int i=0; i<rowsInPage; ++i) {
            vector<int> row = currentPage.getRow(i);
            if (!row.empty()) {
                pageData.push_back(row);
                rowsReadFromPage++;
            } else {
                 logger.log("executeUPDATE WARNING: Reading page " + to_string(pageIdx) + ", got empty row at index " + to_string(i));
            }
        }
         // Validate read count
         if (rowsReadFromPage != rowsInPage) {
             logger.log("executeUPDATE ERROR: Read " + to_string(rowsReadFromPage) + " rows from page " + to_string(pageIdx) + ", but expected " + to_string(rowsInPage) + ". Skipping page modification.");
             continue;
         }

        // Iterate through the buffered rows and modify if condition met
        for (int rowIdx = 0; rowIdx < rowsInPage; ++rowIdx) {
            // Check bounds
            if (whereColIdx >= (int)pageData[rowIdx].size() || targetColIdx >= (int)pageData[rowIdx].size()) {
                logger.log("executeUPDATE WARNING: Row " + to_string(rowIdx) + " on page " + to_string(pageIdx) + " too short. Skipping update check.");
                 continue;
            }

            // Evaluate WHERE condition using evaluateBinOp
            if (evaluateBinOp(pageData[rowIdx][whereColIdx], condVal, opEnum))
            {
                // Condition met, update the target column *if needed*
                if (pageData[rowIdx][targetColIdx] != newVal) {
                    logger.log("Updating row at {" + to_string(pageIdx) + "," + to_string(rowIdx) + "}: Setting column '" + table->columns[targetColIdx] + "' from " + to_string(pageData[rowIdx][targetColIdx]) + " to " + to_string(newVal));
                    pageData[rowIdx][targetColIdx] = newVal;
                    pageModified = true; // Mark page for writing back
                    totalRowsUpdated++;
                }
            }
        }

        // If any row in this page was modified, write the entire modified page back
        if (pageModified) {
            logger.log("Writing modified page " + to_string(pageIdx) + " back to disk.");
            // Use the row count currently in the pageData (which shouldn't change during UPDATE)
            bufferManager.writePage(table->tableName, pageIdx, pageData, rowsInPage);
        }
    } 

    if (totalRowsUpdated > 0) {
        cout << "UPDATE completed successfully. " << totalRowsUpdated << " rows updated." << endl;
        logger.log("Total rows updated: " + to_string(totalRowsUpdated));

        // Clear the buffer pool for this table for consistency
        logger.log("Clearing buffer pool cache for table: " + table->tableName + " after UPDATE.");
        bufferManager.clearPoolForTable(table->tableName);

    } else {
        cout << "UPDATE completed. No rows matched the WHERE condition." << endl;
        logger.log("No rows were updated.");
    }

    return;
} 