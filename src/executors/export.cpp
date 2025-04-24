#include "global.h"
#include "executor.h"        // Include executor base if needed
#include "tableCatalogue.h"  // Include for tableCatalogue access
#include "syntacticParser.h" // Include for parsedQuery access
#include "table.h"           // Include full Table definition

using namespace std;

/**
 * @brief
 * SYNTAX: EXPORT <relation_name>
 */

bool syntacticParseEXPORT()
{
    logger.log("syntacticParseEXPORT");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = EXPORT;
    parsedQuery.exportRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseEXPORT()
{
    logger.log("semanticParseEXPORT");
    // Table should exist
    if (tableCatalogue.isTable(parsedQuery.exportRelationName))
        return true;
    cout << "SEMANTIC ERROR: No such relation exists" << endl;
    return false;
}

void executeEXPORT()
{
    logger.log("executeEXPORT");
    Table *table = tableCatalogue.getTable(parsedQuery.exportRelationName);
    if (table == nullptr) // Add check for null pointer
    {
        cout << "SEMANTIC ERROR: Relation '" << parsedQuery.exportRelationName << "' not found for EXPORT." << endl;
        return;
    }
    table->makePermanent(); // Now Table definition is available
    cout << "Exported table '" << parsedQuery.exportRelationName << "' successfully." << endl;
    return;
}