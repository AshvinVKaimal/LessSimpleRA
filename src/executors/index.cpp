#include "global.h"
#include "executor.h"        // Include executor base if needed
#include "tableCatalogue.h"  // Include for tableCatalogue access
#include "syntacticParser.h" // Include for parsedQuery access
#include "table.h"           // Include full Table definition
#include "indexing.h"        // Include BPTree definition

using namespace std;

/**
 * @brief
 * SYNTAX: INDEX ON column_name FROM relation_name USING indexing_strategy
 * NOTE: This command is DISABLED as indexing is now implicit on column 0.
 */
bool syntacticParseINDEX()
{
    logger.log("syntacticParseINDEX");
    cout << "INDEX command is disabled; indexing is implicit on the first column during LOAD." << endl;
    return false;
}

bool semanticParseINDEX()
{
    logger.log("semanticParseINDEX");
    // Disable explicit INDEX command
    cout << "SEMANTIC ERROR: INDEX command is disabled." << endl;
    return false;
    /* --- Original Code Disabled ---
    if (!tableCatalogue.isTable(parsedQuery.indexRelationName))
    {
        cout << "SEMANTIC ERROR: Relation '" << parsedQuery.indexRelationName << "' does not exist." << endl;
        return false;
    }
    // Check if column exists in the relation
    if (!tableCatalogue.isColumnFromTable(parsedQuery.indexColumnName, parsedQuery.indexRelationName))
    {
        cout << "SEMANTIC ERROR: Column '" << parsedQuery.indexColumnName << "' does not exist in relation '" << parsedQuery.indexRelationName << "'." << endl;
        return false;
    }
    Table* table = tableCatalogue.getTable(parsedQuery.indexRelationName);
    // Allow removing index even if table->indexed is false (harmless)
    // if(table->indexed && parsedQuery.indexingStrategy != NOTHING){ // Check if already indexed only if creating new
    //     cout << "SEMANTIC ERROR: Table already indexed" << endl;
    //     return false;
    // }
    return true;
    */
}

void executeINDEX()
{
    logger.log("executeINDEX");
    // Command is disabled
    cout << "LOGIC ERROR: executeINDEX called but command is disabled." << endl;
    return;
}