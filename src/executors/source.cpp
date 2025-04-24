#include "global.h"
#include "table.h"
/**
 * @brief 
 * SYNTAX: SOURCE filename
 */
bool syntacticParseSOURCE()
{
    logger.log("syntacticParseSOURCE");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = SOURCE;
    parsedQuery.sourceFileName = tokenizedQuery[1];
    return true;
}

bool semanticParseSOURCE()
{
    logger.log("semanticParseSOURCE");
    if (!isQueryFile(parsedQuery.sourceFileName))
    {
        cout << "SEMANTIC ERROR: File doesn't exist" << endl;
        return false;
    }
    return true;
}

void executeSOURCE()
{
    logger.log("executeSOURCE");
    
    string fileName = "../data/" + parsedQuery.sourceFileName + ".ra";
    ifstream queryFile(fileName);
    if (!queryFile.is_open())
    {
        cout << "Unable to open file " << fileName << endl;
        return;
    }

    string queryLine;
    while (getline(queryFile, queryLine))
    {
        if (queryLine == "")
            continue;

        // cout << "Query: " << queryLine << endl;
        
        // Tokenize query
        tokenizedQuery.clear();
        stringstream check(queryLine);
        string word;
        while (check >> word)
        {
            if (!word.empty() && word[word.size() - 1] == ',')
                word = word.substr(0, word.size() - 1);

            tokenizedQuery.push_back(word);
        }
        if (tokenizedQuery.empty())
            continue;

        // Parse and execute query
        if (syntacticParse() && semanticParse())
        {
            executeCommand();

            // Debugging
            if (parsedQuery.queryType == SELECTION || parsedQuery.queryType == PROJECTION || parsedQuery.queryType == JOIN)
            {
                if (!tableCatalogue.isTable(parsedQuery.selectionResultRelationName) && !tableCatalogue.isTable(parsedQuery.projectionResultRelationName) && !tableCatalogue.isTable(parsedQuery.joinResultRelationName))
                    cout << "Table doesn't exist" << endl;
                else
                {
                    Table* table = tableCatalogue.getTable(parsedQuery.selectionResultRelationName);
                    if (table)
                        table->print();
                    else
                    {
                        table = tableCatalogue.getTable(parsedQuery.projectionResultRelationName);
                        if (table)
                            table->print();
                        else
                        {
                            table = tableCatalogue.getTable(parsedQuery.joinResultRelationName);
                            if (table)
                                table->print();
                        }
                    }
                }
            }
        }
        else
            cout << "ERROR: Incorrect query " << queryLine << endl;
    }

    queryFile.close();
}
