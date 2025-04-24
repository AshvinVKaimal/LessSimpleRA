#include "global.h"
#include "executor.h"
#include "tableCatalogue.h"
#include "syntacticParser.h"
#include "table.h" // Include full Table definition

using namespace std;

/**
 * @brief
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINT()
{
    logger.log("syntacticParsePRINT");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = PRINT;
    parsedQuery.printRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParsePRINT()
{
    logger.log("semanticParsePRINT");
    if (!tableCatalogue.isTable(parsedQuery.printRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    return true;
}

void executePRINT()
{
    logger.log("executePRINT");
    Table *table = tableCatalogue.getTable(parsedQuery.printRelationName);
    if (table == nullptr)
    {
        cout << "SEMANTIC ERROR: Relation '" << parsedQuery.printRelationName << "' not found." << endl;
        return;
    }
    table->print(); // Now Table definition is available
    return;
}
