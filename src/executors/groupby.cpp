#include "global.h"
#include "executor.h"
#include "tableCatalogue.h"
#include "syntacticParser.h"
#include "table.h"  // Include full Table definition
#include "cursor.h" // Include Cursor definition
#include <vector>
#include <string>
#include <unordered_map>
#include <set>
#include <algorithm> // For std::sort, std::min, std::max
#include <limits>    // For std::numeric_limits
#include <numeric>   // For std::accumulate

using namespace std;

// Forward declarations for helper functions
long long applyAggregate(const string &funcName, const vector<int> &values);
bool evaluateCondition(long long aggregateValue, const string &op, int conditionValue);

bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseGROUPBY");

    // We expect exactly 13 tokens
    if (tokenizedQuery.size() != 13 ||
        tokenizedQuery[1] != "<-" ||
        tokenizedQuery[2] != "GROUP" ||
        tokenizedQuery[3] != "BY" ||
        tokenizedQuery[5] != "FROM" ||
        tokenizedQuery[7] != "HAVING" ||
        tokenizedQuery[11] != "RETURN")
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }

    // Set basic query parameters
    parsedQuery.queryType = GROUP_BY;
    parsedQuery.resultTableName = tokenizedQuery[0]; // e.g., "Result"
    parsedQuery.groupByColumn = tokenizedQuery[4];   // e.g., "DepartmentID"
    parsedQuery.tableName = tokenizedQuery[6];       // e.g., "EMPLOYEE"

    // Parse HAVING clause aggregate function token (e.g., "AVG(Salary)")
    string havingToken = tokenizedQuery[8];
    size_t openParen = havingToken.find('(');
    size_t closeParen = havingToken.find(')');
    if (openParen == string::npos || closeParen == string::npos || closeParen <= openParen + 1)
    {
        cout << "SYNTAX ERROR: Invalid HAVING function format" << endl;
        return false;
    }
    parsedQuery.havingFunction = havingToken.substr(0, openParen);                               // e.g., "AVG"
    parsedQuery.havingAttribute = havingToken.substr(openParen + 1, closeParen - openParen - 1); // e.g., "Salary"

    // Parse comparison operator and numeric value
    parsedQuery.havingOperator = tokenizedQuery[9]; // e.g., ">"
    try
    {
        parsedQuery.havingValue = stoi(tokenizedQuery[10]); // e.g., 50000
    }
    catch (exception &e)
    {
        cout << "SYNTAX ERROR: Invalid numeric value in HAVING clause" << endl;
        return false;
    }

    // Parse RETURN clause aggregate function token (e.g., "MAX(Salary)")
    string returnToken = tokenizedQuery[12];
    openParen = returnToken.find('(');
    closeParen = returnToken.find(')');
    if (openParen == string::npos || closeParen == string::npos || closeParen <= openParen + 1)
    {
        cout << "SYNTAX ERROR: Invalid RETURN function format" << endl;
        return false;
    }
    parsedQuery.returnFunction = returnToken.substr(0, openParen);                               // e.g., "MAX"
    parsedQuery.returnAttribute = returnToken.substr(openParen + 1, closeParen - openParen - 1); // e.g., "Salary"

    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");

    // Check if the source table exists
    if (!tableCatalogue.isTable(parsedQuery.groupByRelationName))
    {
        cout << "SEMANTIC ERROR: Source relation '" << parsedQuery.groupByRelationName << "' does not exist." << endl;
        return false;
    }

    // Check if the result table already exists
    if (tableCatalogue.isTable(parsedQuery.resultTableName))
    {
        cout << "SEMANTIC ERROR: Resultant relation '" << parsedQuery.resultTableName << "' already exists." << endl;
        return false;
    }

    Table *table = tableCatalogue.getTable(parsedQuery.groupByRelationName);
    if (!table)
    {
        // This should not happen if isTable passed, but good practice to check
        cout << "SEMANTIC ERROR: Failed to retrieve source relation '" << parsedQuery.groupByRelationName << "'." << endl;
        return false;
    }

    // Check if the GROUP BY column exists in the source table
    if (!table->isColumn(parsedQuery.groupByColumn)) // Use member access
    {
        cout << "SEMANTIC ERROR: Grouping column '" << parsedQuery.groupByColumn << "' does not exist in relation '" << parsedQuery.groupByRelationName << "'." << endl;
        return false;
    }

    // Check if HAVING and RETURN columns exist in the source table
    if (!table->isColumn(parsedQuery.havingAttribute) || !table->isColumn(parsedQuery.returnAttribute)) // Use member access
    {
        cout << "SEMANTIC ERROR: HAVING column '" << parsedQuery.havingAttribute << "' or RETURN column '" << parsedQuery.returnAttribute << "' does not exist in relation '" << parsedQuery.groupByRelationName << "'." << endl;
        return false;
    }

    // Validate aggregate function names (basic check)
    string aggFunc = parsedQuery.havingFunction;
    string retFunc = parsedQuery.returnFunction;
    set<string> validAggFuncs = {"MAX", "MIN", "SUM", "AVG", "COUNT"};
    if (validAggFuncs.find(aggFunc) == validAggFuncs.end() || validAggFuncs.find(retFunc) == validAggFuncs.end())
    {
        cout << "SEMANTIC ERROR: Invalid aggregate function specified in HAVING ('" << aggFunc << "') or RETURN ('" << retFunc << "')." << endl;
        return false;
    }

    // Validate HAVING operator (basic check)
    string op = parsedQuery.havingOperator;
    set<string> validOps = {">", "<", ">=", "<=", "==", "!="};
    if (validOps.find(op) == validOps.end())
    {
        cout << "SEMANTIC ERROR: Invalid operator '" << op << "' specified in HAVING clause." << endl;
        return false;
    }

    // Validate HAVING value (should be convertible to int)
    try
    {
        stoi(parsedQuery.havingValue);
    }
    catch (const std::invalid_argument &ia)
    {
        cout << "SEMANTIC ERROR: Invalid integer value '" << parsedQuery.havingValue << "' specified in HAVING clause." << endl;
        return false;
    }
    catch (const std::out_of_range &oor)
    {
        cout << "SEMANTIC ERROR: Integer value '" << parsedQuery.havingValue << "' out of range in HAVING clause." << endl;
        return false;
    }

    return true;
}

// Helper function implementations
long long applyAggregate(const string &funcName, const vector<int> &values)
{
    if (values.empty())
    {
        if (funcName == "COUNT")
            return 0;
        // For other aggregates, returning 0 or throwing might be appropriate.
        // Let's return 0 for simplicity, but this might need refinement.
        return 0;
    }

    if (funcName == "COUNT")
    {
        return values.size();
    }
    else if (funcName == "SUM")
    {
        // Use long long for accumulation to prevent overflow
        return std::accumulate(values.begin(), values.end(), 0LL);
    }
    else if (funcName == "AVG")
    {
        long long sum = std::accumulate(values.begin(), values.end(), 0LL);
        // Perform integer division for AVG
        return sum / values.size();
    }
    else if (funcName == "MAX")
    {
        return *std::max_element(values.begin(), values.end());
    }
    else if (funcName == "MIN")
    {
        return *std::min_element(values.begin(), values.end());
    }
    else
    {
        // Should have been caught by semantic parser, but handle defensively
        logger.log("applyAggregate ERROR: Unknown aggregate function '" + funcName + "'");
        return 0; // Or throw an exception
    }
}

bool evaluateCondition(long long aggregateValue, const string &op, int conditionValue)
{
    if (op == ">")
        return aggregateValue > conditionValue;
    if (op == "<")
        return aggregateValue < conditionValue;
    if (op == ">=")
        return aggregateValue >= conditionValue;
    if (op == "<=")
        return aggregateValue <= conditionValue;
    if (op == "==")
        return aggregateValue == conditionValue;
    if (op == "!=")
        return aggregateValue != conditionValue;

    // Should have been caught by semantic parser
    logger.log("evaluateCondition ERROR: Unknown operator '" + op + "'");
    return false;
}

void executeGROUPBY()
{
    logger.log("executeGROUPBY");

    Table *inputTable = tableCatalogue.getTable(parsedQuery.groupByRelationName);
    if (!inputTable)
    {
        // Should have been caught by semantic check, but double-check
        cout << "EXECUTION ERROR: Source table '" << parsedQuery.groupByRelationName << "' not found." << endl;
        return;
    }

    // Get column indices
    int groupByIndex = inputTable->getColumnIndex(parsedQuery.groupByColumn);  // Use member access
    int havingIndex = inputTable->getColumnIndex(parsedQuery.havingAttribute); // Use member access
    int returnIndex = inputTable->getColumnIndex(parsedQuery.returnAttribute); // Use member access

    if (groupByIndex < 0 || havingIndex < 0 || returnIndex < 0)
    {
        cout << "EXECUTION ERROR: Column index lookup failed (this shouldn't happen after semantic check)." << endl;
        return;
    }

    // Map to store intermediate results: GroupValue -> { {HavingValues}, {ReturnValues} }
    unordered_map<int, pair<vector<int>, vector<int>>> groups;

    // Iterate through the input table using a cursor
    Cursor cursor = inputTable->getCursor(); // Use member access, Cursor is now defined
    vector<int> row;

    logger.log("executeGROUPBY: Starting data scan and grouping...");
    while (!(row = cursor.getNext()).empty())
    {
        // Basic row validation
        if (row.size() < (unsigned)inputTable->columnCount) // Use member access
        {
            logger.log("executeGROUPBY: Warning - Skipping malformed row with size " + to_string(row.size()));
            continue;
        }

        int groupValue = row[groupByIndex];
        int havingValue = row[havingIndex];
        int returnValue = row[returnIndex];

        // Add values to the corresponding group
        groups[groupValue].first.push_back(havingValue);
        groups[groupValue].second.push_back(returnValue);
    }
    logger.log("executeGROUPBY: Finished data scan. Found " + to_string(groups.size()) + " unique groups.");

    // Process groups: apply aggregates, check HAVING condition, prepare result rows
    vector<vector<int>> resultRows;
    string returnHeader = parsedQuery.returnFunction + parsedQuery.returnAttribute; // Construct return column header

    logger.log("executeGROUPBY: Processing groups and applying conditions...");
    // Replace C++17 structured binding with C++11 compatible loop
    for (auto it = groups.begin(); it != groups.end(); ++it)
    {
        int groupValue = it->first;
        const pair<vector<int>, vector<int>> &valuePairs = it->second;
        // for (auto const &[groupValue, valuePairs] : groups) // Replaced C++17 feature

        const vector<int> &havingValues = valuePairs.first;
        const vector<int> &returnValues = valuePairs.second;

        if (havingValues.empty() || returnValues.empty())
        {
            // Should not happen if data was inserted correctly
            logger.log("executeGROUPBY: Warning - Group " + to_string(groupValue) + " has empty value lists. Skipping.");
            continue;
        }

        // Apply aggregate function for HAVING clause
        long long havingAggResult = applyAggregate(parsedQuery.havingFunction, havingValues); // Now declared/defined

        // Evaluate HAVING condition
        int havingConditionValue = stoi(parsedQuery.havingValue);
        if (evaluateCondition(havingAggResult, parsedQuery.havingOperator, havingConditionValue)) // Now declared/defined
        {
            // Apply aggregate function for RETURN clause
            long long returnAggResult = applyAggregate(parsedQuery.returnFunction, returnValues); // Now declared/defined

            // Prepare the row for the result table: {GroupValue, ReturnAggregateValue}
            // Cast returnAggResult safely to int if necessary, handle potential overflow/truncation
            int returnAggInt = static_cast<int>(returnAggResult);
            if (returnAggResult > numeric_limits<int>::max() || returnAggResult < numeric_limits<int>::min())
            {
                logger.log("executeGROUPBY: Warning - Return aggregate result " + to_string(returnAggResult) + " for group " + to_string(groupValue) + " overflows int. Truncating.");
                // Decide how to handle overflow: clamp, error, etc. Using static_cast truncates.
            }
            resultRows.push_back({groupValue, returnAggInt});
        }
    }
    logger.log("executeGROUPBY: Finished processing groups. " + to_string(resultRows.size()) + " groups satisfy the HAVING condition.");

    // Create the result table
    Table *resultTable = new Table(parsedQuery.resultTableName, {parsedQuery.groupByColumn, returnHeader}); // Table is now defined

    // Write the result rows to the new table
    logger.log("executeGROUPBY: Writing results to table '" + parsedQuery.resultTableName + "'...");
    for (const auto &rRow : resultRows)
    {
        resultTable->writeRow<int>(rRow); // Use member access
    }

    // Finalize the result table (blockify, update stats, insert into catalogue)
    resultTable->rowCount = resultRows.size(); // Use member access
    resultTable->blockify();                   // Use member access
    tableCatalogue.insertTable(resultTable);
    logger.log("executeGROUPBY: Result table '" + parsedQuery.resultTableName + "' created successfully.");

    cout << "Group By operation completed. Result table '" << parsedQuery.resultTableName << "' created with " << resultRows.size() << " rows." << endl;
}
