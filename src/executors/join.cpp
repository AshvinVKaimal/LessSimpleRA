#include "global.h"
#include "executor.h"
#include "tableCatalogue.h"
#include "syntacticParser.h"
#include "table.h"  // Include full Table definition
#include "cursor.h" // Include Cursor definition
#include <vector>
#include <string>
#include <unordered_map> // For hash join
#include <algorithm>     // For std::copy

using namespace std;

/**
 * @brief
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 column_name2
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 8 || tokenizedQuery[5] != "ON")
    {
        cout << "SYNTAC ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    parsedQuery.joinFirstRelationName = tokenizedQuery[3];
    parsedQuery.joinSecondRelationName = tokenizedQuery[4];
    parsedQuery.joinFirstColumnName = tokenizedQuery[6];
    parsedQuery.joinSecondColumnName = tokenizedQuery[7];

    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }
    return true;
}

void executeJOIN()
{
    logger.log("executeJOIN");

    Table *table1 = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table *table2 = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);

    // Determine which table is smaller for potential optimization (e.g., hash join)
    Table *smallerTable = (table1->rowCount <= table2->rowCount) ? table1 : table2; // Use member access
    Table *largerTable = (table1->rowCount > table2->rowCount) ? table1 : table2;   // Use member access

    string smallerColumn = (smallerTable == table1) ? parsedQuery.joinFirstColumnName : parsedQuery.joinSecondColumnName;
    string largerColumn = (largerTable == table1) ? parsedQuery.joinFirstColumnName : parsedQuery.joinSecondColumnName;

    int smallerIndex = smallerTable->getColumnIndex(smallerColumn); // Use member access
    int largerIndex = largerTable->getColumnIndex(largerColumn);    // Use member access

    // --- Hash Join Implementation ---
    unordered_map<int, vector<vector<int>>> hashTable;
    Cursor cursor = smallerTable->getCursor(); // Cursor is now defined, use member access
    vector<int> row;

    // 1. Build Phase: Populate hash table with rows from the smaller table
    logger.log("executeJOIN: Building hash table from smaller table: " + smallerTable->tableName);
    while (!(row = cursor.getNext()).empty())
    {
        hashTable[row[smallerIndex]].push_back(row);
    }
    logger.log("executeJOIN: Hash table built with " + to_string(hashTable.size()) + " unique keys.");

    // Prepare resultant table
    vector<string> resultantColumns = table1->columns;                                               // Use member access
    resultantColumns.insert(resultantColumns.end(), table2->columns.begin(), table2->columns.end()); // Use member access
    Table *resultantTable = new Table(parsedQuery.joinResultRelationName, resultantColumns);         // Table is now defined

    // 2. Probe Phase: Iterate through the larger table and probe the hash table
    logger.log("executeJOIN: Probing hash table with larger table: " + largerTable->tableName);
    cursor = largerTable->getCursor(); // Use member access
    vector<int> resultantRow;

    while (!(row = cursor.getNext()).empty())
    {
        int key = row[largerIndex];
        if (hashTable.count(key))
        {
            // Found matching keys in the hash table
            for (const auto &smallerTableRow : hashTable[key])
            {
                resultantRow.clear();
                // Combine rows based on which table was smaller/larger
                if (smallerTable == table1)
                {
                    resultantRow.insert(resultantRow.end(), smallerTableRow.begin(), smallerTableRow.end());
                    resultantRow.insert(resultantRow.end(), row.begin(), row.end());
                }
                else
                {
                    resultantRow.insert(resultantRow.end(), row.begin(), row.end());
                    resultantRow.insert(resultantRow.end(), smallerTableRow.begin(), smallerTableRow.end());
                }
                resultantTable->writeRow<int>(resultantRow); // Use member access
            }
        }
    }
    logger.log("executeJOIN: Probe phase complete.");

    // Finalize the resultant table
    if (resultantTable->blockify()) // Use member access
    {
        tableCatalogue.insertTable(resultantTable);
        cout << "JOIN operation successful. Resultant table '" << parsedQuery.joinResultRelationName << "' created." << endl;
    }
    else
    {
        // If blockify fails (e.g., empty result), still need to clean up
        cout << "JOIN operation completed, but resultant table '" << parsedQuery.joinResultRelationName << "' is empty or failed to blockify." << endl;
        // Clean up the resultant table object if it wasn't inserted
        if (!tableCatalogue.isTable(parsedQuery.joinResultRelationName))
        {
            delete resultantTable; // Delete pointer to incomplete type warning is acceptable if blockify failed
        }
    }
}