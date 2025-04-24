#include "global.h"
#include <vector>
#include <map>
#include <set> // For the mini page cache optimization
#include <iostream> // For cout/cerr

/**
 * @brief Placeholder for SEARCH command execution.
 * SYNTAX: R <- SEARCH FROM T WHERE C op V
 * (Actual syntax TBD and needs parsing - this is just one possible variant)
 */


 bool semanticParseSEARCH()
 {
     logger.log("semanticParseSEARCH");
 
     // 1. Check Result Table doesn't exist
     if (tableCatalogue.isTable(parsedQuery.selectionResultRelationName))
     {
         cout << "SEMANTIC ERROR: Resultant relation '" << parsedQuery.selectionResultRelationName << "' already exists" << endl;
         return false;
     }
 
     // 2. Check Source Table exists
     if (!tableCatalogue.isTable(parsedQuery.selectionRelationName))
     {
         cout << "SEMANTIC ERROR: Source relation '" << parsedQuery.selectionRelationName << "' doesn't exist" << endl;
         return false;
     }
 
     // 3. Check WHERE Column exists in Source Table
     if (!tableCatalogue.isColumnFromTable(parsedQuery.selectionFirstColumnName, parsedQuery.selectionRelationName))
     {
         cout << "SEMANTIC ERROR: Column '" << parsedQuery.selectionFirstColumnName << "' doesn't exist in relation '" << parsedQuery.selectionRelationName << "'" << endl;
         return false;
     }

 
     logger.log("Semantic parse SEARCH successful.");
     return true;
 }
 


/**
 * @brief Parses the SEARCH command
 * Syntax: <res_table> <- SEARCH FROM <table_name> WHERE <col_name> <bin_op> <value>
 */
bool syntacticParseSEARCH()
{
    logger.log("syntacticParseSEARCH");
    // Expected format: res <- SEARCH FROM tbl WHERE col op val (9 tokens)
    if (tokenizedQuery.size() != 9 || tokenizedQuery[1] != "<-" || tokenizedQuery[2] != "SEARCH" || tokenizedQuery[3] != "FROM" || tokenizedQuery[5] != "WHERE")
    {
        cout << "SYNTAX ERROR: Invalid SEARCH format. Expected: R <- SEARCH FROM T WHERE C op V" << endl;
        return false;
    }

    parsedQuery.queryType = SEARCH;
    // Reuse selection fields for simplicity
    parsedQuery.selectionResultRelationName = tokenizedQuery[0]; // Result table
    parsedQuery.selectionRelationName = tokenizedQuery[4];      // Source table
    parsedQuery.selectionFirstColumnName = tokenizedQuery[6];   // WHERE column

    // Parse binary operator (reuse selection logic)
    string binaryOperator = tokenizedQuery[7];
    if (binaryOperator == "<")
        parsedQuery.selectionBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.selectionBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.selectionBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.selectionBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.selectionBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.selectionBinaryOperator = NOT_EQUAL;
    else
    {
        cout << "SYNTAX ERROR: Unknown binary operator '" << binaryOperator << "' in SEARCH." << endl;
        return false;
    }

    // Parse value 
    regex numeric("[-]?[0-9]+");
    if (regex_match(tokenizedQuery[8], numeric))
    {
        parsedQuery.selectType = INT_LITERAL; // Mark as literal comparison
        try {
             parsedQuery.selectionIntLiteral = stoi(tokenizedQuery[8]);
         } catch (const std::out_of_range& oor) {
             cout << "SYNTAX ERROR: Integer literal '" << tokenizedQuery[8] << "' is out of range." << endl;
             return false;
         } catch (...) { // Catch potential stoi errors generally
             cout << "SYNTAX ERROR: Invalid integer literal '" << tokenizedQuery[8] << "'." << endl;
              return false;
         }
    }
    else
    {
        
        cout << "SYNTAX ERROR: SEARCH currently only supports comparison with integer literals." << endl;
        return false;
    }

    logger.log("Parsed SEARCH: Result=" + parsedQuery.selectionResultRelationName +
               ", Source=" + parsedQuery.selectionRelationName +
               ", Column=" + parsedQuery.selectionFirstColumnName +
               ", Operator=" + binaryOperator +
               ", Value=" + to_string(parsedQuery.selectionIntLiteral));
    return true;
}




/**
 * @brief Helper function to fetch specific rows using their locations.
 *
 * @param resultTable The table to write fetched rows into.
 * @param sourceTable The table from which to fetch rows.
 * @param locations A vector of RowLocation pairs {pageIndex, rowIndexInPage}.
 */
void fetchRows(Table* resultTable, Table* sourceTable, const std::vector<Table::RowLocation>& locations) {
    // Cache pages locally for this fetch operation to reduce buffer manager calls
    std::map<int, Page> pageCache;

    logger.log("fetchRows: Fetching " + to_string(locations.size()) + " rows based on index locations.");

    for (const auto& loc : locations) {
        int pageIdx = loc.first;
        int rowIdx = loc.second;
        Page page;

        // Simple caching approach
        if (pageCache.find(pageIdx) == pageCache.end()) {
             // Page not in cache, fetch it
             logger.log("fetchRows: Fetching page " + to_string(pageIdx) + " for table " + sourceTable->tableName);
             page = bufferManager.getPage(sourceTable->tableName, pageIdx);
             if (page.pageName.empty()) { // Basic check if getPage failed somehow
                 logger.log("fetchRows ERROR: Failed to get page " + to_string(pageIdx) + ". Skipping location.");
                 continue;
             }
             pageCache[pageIdx] = page; // Store fetched page
         } else {
             // Page already in cache
             // logger.log("fetchRows: Using cached page " + to_string(pageIdx)); // Can be verbose
             page = pageCache[pageIdx];
         }


        // Get the specific row from the page object
        vector<int> rowData = page.getRow(rowIdx);
        if (!rowData.empty()) {
            // logger.log("fetchRows: Writing row from page " + to_string(pageIdx) + " row " + to_string(rowIdx));
            resultTable->writeRow<int>(rowData); // Write to the result table's temporary file/buffer
        } else {
             logger.log("fetchRows WARNING: Tried to get invalid or empty row at page " + to_string(pageIdx) + ", row " + to_string(rowIdx));
             // This might happen if Page::getRow has strict bounds checking and rowIdx is invalid,
             // or if the page data is somehow corrupted, or if the location from index is stale.
        }
    }
    logger.log("fetchRows: Finished fetching rows.");
}


/**
 * @brief Executes the SEARCH command.
 * Syntax: <res_table> <- SEARCH FROM <table_name> WHERE <col_name> <bin_op> <value>
 * Uses the implicitly created index on the queried column if available.
 */
void executeSEARCH() {
    logger.log("executeSEARCH starting...");

    // Reuse selection fields based on parser logic
    Table* table = tableCatalogue.getTable(parsedQuery.selectionRelationName);
    if (!table) {
         cout << "ERROR: Source table '" << parsedQuery.selectionRelationName << "' not found during SEARCH execution." << endl;
         return;
    }
    // Create the result table structure (empty for now)
    Table* resultantTable = new Table(parsedQuery.selectionResultRelationName, table->columns);


    bool index_used = false;
    string queryColumnName = parsedQuery.selectionFirstColumnName; // Column in the WHERE clause

    // Check if an index exists specifically for the column in the WHERE clause
    // and if the query involves comparing with an integer literal
    if (table->isIndexed(queryColumnName) && parsedQuery.selectType == INT_LITERAL)
    {
        logger.log("Attempting to use implicit index (std::map) for SEARCH on column: '" + queryColumnName + "'");

        int value = parsedQuery.selectionIntLiteral;
        BinaryOperator op = parsedQuery.selectionBinaryOperator;

        // Get the specific index map for the queried column
        // Use .at() which throws if key not found (shouldn't happen if isIndexed passed)
        // Or use find() and check iterator for more safety.
        auto map_iter = table->multiColumnIndexData.find(queryColumnName);
        if (map_iter == table->multiColumnIndexData.end()) {
             logger.log("executeSEARCH ERROR: Index map not found for column '" + queryColumnName + "' even though isIndexed returned true. Falling back.");
             // This indicates an internal logic error. Fallback to scan.
        } else {
             auto& index = map_iter->second; // Reference to the inner std::map<int, RowLocation>
             std::vector<Table::RowLocation> locationsToFetch;


             try { // Use try-catch for map operations (like lower_bound, upper_bound)
                 switch (op) {
                     case EQUAL: {
                         auto it = index.find(value);
                         if (it != index.end()) {
                             locationsToFetch.push_back(it->second); // Add the single location (unique key assumption)
                             index_used = true;
                         }
                         // If not found, index_used remains false, locationsToFetch is empty
                         break;
                     }
                     case LESS_THAN: { // All keys < value
                         auto end_it = index.lower_bound(value); // First element NOT less than value
                         for (auto it = index.begin(); it != end_it; ++it) {
                             locationsToFetch.push_back(it->second);
                         }
                          index_used = true; // Index was consulted, even if result is empty
                         break;
                     }
                     case LEQ: { // All keys <= value
                         auto end_it = index.upper_bound(value); // First element GREATER than value
                         for (auto it = index.begin(); it != end_it; ++it) {
                             locationsToFetch.push_back(it->second);
                         }
                          index_used = true; // Index was consulted
                         break;
                     }
                     case GREATER_THAN: { // All keys > value
                          auto start_it = index.upper_bound(value); // First element GREATER than value
                          for (auto it = start_it; it != index.end(); ++it) {
                              locationsToFetch.push_back(it->second);
                          }
                           index_used = true; // Index was consulted
                          break;
                     }
                     case GEQ: { // All keys >= value
                          auto start_it = index.lower_bound(value); // First element NOT LESS than value
                          for (auto it = start_it; it != index.end(); ++it) {
                              locationsToFetch.push_back(it->second);
                          }
                           index_used = true; // Index was consulted
                          break;
                     }
                     case NOT_EQUAL: {
                         // Fetch ALL locations and skip the one matching 'value' if found.
                         // This is often slower than a full scan, so we fallback.
                         logger.log("Index usage for '!=' is inefficient; falling back to full scan.");
                         index_used = false; // Decide *not* to use the index here
                         break;
                     }
                     default:
                         logger.log("Unknown operator for index search; falling back.");
                         index_used = false; // Decide *not* to use the index
                 }
             } catch (const std::exception& e) {
                  logger.log("Exception during index map access for column '" + queryColumnName + "': " + string(e.what()) + ". Falling back to full scan.");
                  index_used = false; // Fallback if map operation fails
             }


             // Fetch rows if index was successfully used and locations were found
             if(index_used && !locationsToFetch.empty()) {
                  logger.log("Index lookup identified " + to_string(locationsToFetch.size()) + " potential rows. Fetching...");
                  fetchRows(resultantTable, table, locationsToFetch);
                  logger.log("Finished fetching rows using index.");
             } else if (index_used) {
                  logger.log("Index lookup completed, but found no matching rows.");
                  // No rows to fetch, resultantTable will remain empty (correct)
             }
         } // end else (index map found)


    } else { // Index not available ( will never happen but just in case ?)
          if (!table->isIndexed(queryColumnName)) {
               logger.log("Index not available for column '" + queryColumnName + "'.");
          } else if (parsedQuery.selectType != INT_LITERAL) {
               logger.log("Index usage only supported for integer literal comparisons in WHERE clause.");
          }
          logger.log("Proceeding with full table scan for SEARCH.");
    } 




    // --- Fallback to Full Table Scan / Only happens due to data is lost ---
    if (!index_used) {
        logger.log("Performing full table scan for SEARCH.");
        Cursor cursor = table->getCursor();
        vector<int> row;
        int searchColumnIndex = table->getColumnIndex(queryColumnName); // Use parsed queryColumnName


        if (searchColumnIndex < 0) {
             cout << "ERROR: Search column '" << queryColumnName << "' not found during execution (should have been caught in semantic parse)." << endl;
             // Clean up the empty result table
             resultantTable->unload();
             delete resultantTable;
             return; // Stop execution
        }


        int comparisonValue = parsedQuery.selectionIntLiteral;
        BinaryOperator comparisonOp = parsedQuery.selectionBinaryOperator;


        while (!(row = cursor.getNext()).empty()) {
            // Ensure row has enough columns before accessing
            if (searchColumnIndex >= (int)row.size()) { // Safe cast
                logger.log("Full scan WARNING: Row encountered with fewer columns (" + to_string(row.size()) + ") than expected index " + to_string(searchColumnIndex) + ".");
                continue; // Skip malformed row
            }
            int columnValue = row[searchColumnIndex];


            if (evaluateBinOp(columnValue, comparisonValue, comparisonOp)) {
                resultantTable->writeRow<int>(row);
            }
        }
        logger.log("Full table scan completed.");
    }


    // --- Finalize Result Table ---
    logger.log("Blockifying result table: '" + resultantTable->tableName + "'");

    if (resultantTable->blockify()) {
        // Only insert if blockify succeeded 
        tableCatalogue.insertTable(resultantTable);
        cout << "SEARCH completed. ";
        cout << "Result stored in table: '" << resultantTable->tableName << "'. Row Count: " << resultantTable->rowCount << endl;
    } 
    else {
        // blockify returns false if the table is empty OR if an error occurred during write.
        logger.log("Blockify returned false for result table '" + resultantTable->tableName + "'. Checking row count...");
        if (resultantTable->rowCount == 0) { // Rely on rowCount updated by blockify (if it does) or check source file size?
             cout << "SEARCH completed. Result table is empty." << endl;
             logger.log("Result table '" + resultantTable->tableName + "' is empty.");
        } else {
             cout << "ERROR: SEARCH completed but failed to blockify/write the result table '" << resultantTable->tableName << "'." << endl;
              logger.log("Blockify failed for non-empty result table. Disk space? Permissions?");
        }
        // Clean up the result table object and its potential temp files regardless
        resultantTable->unload();
        delete resultantTable;
    }
}