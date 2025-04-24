#include "global.h"
#include "executor.h"
#include "tableCatalogue.h"
#include "syntacticParser.h"
#include "table.h"  // Include full Table definition
#include "cursor.h" // Include Cursor definition
#include <vector>
#include <string>
#include <algorithm> // Include for std::copy

using namespace std;

/**
 * @brief
 * SYNTAX: R <- CROSS relation_name relation_name
 */
bool syntacticParseCROSS()
{
    logger.log("syntacticParseCROSS");
    if (tokenizedQuery.size() != 5)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CROSS;
    parsedQuery.crossResultRelationName = tokenizedQuery[0];
    parsedQuery.crossFirstRelationName = tokenizedQuery[3];
    parsedQuery.crossSecondRelationName = tokenizedQuery[4];
    return true;
}

bool semanticParseCROSS()
{
    logger.log("semanticParseCROSS");
    // Both tables must exist and resultant table shouldn't
    if (tableCatalogue.isTable(parsedQuery.crossResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.crossFirstRelationName) || !tableCatalogue.isTable(parsedQuery.crossSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Cross relations don't exist" << endl;
        return false;
    }
    return true;
}

void executeCROSS()
{
    logger.log("executeCROSS");

    // Validate relation names
    if (!tableCatalogue.isTable(parsedQuery.crossFirstRelationName) ||
        !tableCatalogue.isTable(parsedQuery.crossSecondRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        // return false;
    }

    // Check for resultant table existence
    if (tableCatalogue.isTable(parsedQuery.crossResultRelationName))
    {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        // return false;
    }

    // Get table objects (now using full Table definition)
    Table table1 = *(tableCatalogue.getTable(parsedQuery.crossFirstRelationName));
    Table table2 = *(tableCatalogue.getTable(parsedQuery.crossSecondRelationName));

    // Prepare column names for the resultant table
    vector<string> columns;
    columns.reserve(table1.columnCount + table2.columnCount); // Use member access

    // Add columns from table1
    for (const auto &col : table1.columns) // Use member access
    {
        columns.push_back(table1.tableName + "_" + col); // Prefix with table name
    }

    // Add columns from table2
    for (const auto &col : table2.columns) // Use member access
    {
        columns.push_back(table2.tableName + "_" + col); // Prefix with table name
    }

    // Create the resultant table object
    Table *resultantTable = new Table(parsedQuery.crossResultRelationName, columns); // Now Table is defined

    // Prepare cursors (now Cursor is defined)
    Cursor cursor1 = table1.getCursor();
    Cursor cursor2 = table2.getCursor(); // Corrected type

    vector<int> row1, row2;
    vector<int> resultantRow;
    resultantRow.reserve(resultantTable->columnCount); // Use member access

    // Perform the cross product
    row1 = cursor1.getNext();
    while (!row1.empty())
    {
        cursor2 = table2.getCursor(); // Reset cursor2 for each row1
        row2 = cursor2.getNext();     // Use corrected cursor2
        while (!row2.empty())
        {
            resultantRow.clear();
            resultantRow.insert(resultantRow.end(), row1.begin(), row1.end());
            resultantRow.insert(resultantRow.end(), row2.begin(), row2.end());
            resultantTable->writeRow<int>(resultantRow); // Use member access
            row2 = cursor2.getNext();                    // Use corrected cursor2
        }
        row1 = cursor1.getNext();
    }

    // Finalize the resultant table
    resultantTable->blockify(); // Use member access
    tableCatalogue.insertTable(resultantTable);

    cout << "Cross product table '" << parsedQuery.crossResultRelationName << "' created successfully." << endl;
    // return true;
}