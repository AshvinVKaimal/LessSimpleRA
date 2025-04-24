#include "global.h"
#include "logger.h" // Include Logger definition
#include <iostream> // Include for cout, endl
#include <string>
#include <vector>
#include <sys/stat.h> // Include for stat function and struct stat

// Forward declarations or include headers for specific syntactic parse functions
bool syntacticParseCLEAR();
bool syntacticParseINDEX();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParseLOADMATRIX();
bool syntacticParsePRINT();
bool syntacticParsePRINTMATRIX();
bool syntacticParseRENAME();
// bool syntacticParseRENAMEMATRIX(); // Removed
bool syntacticParseEXPORT();
bool syntacticParseEXPORTMATRIX();
bool syntacticParseSOURCE();
bool syntacticParseTRANSPOSEMATRIX();
bool syntacticParseCHECKSYMMETRYMATRIX();
bool syntacticParseCOMPUTEMATRIX();
bool syntacticParseROTATEMATRIX();
bool syntacticParseCHECKANTISYMMATRIX();
bool syntacticParseCROSSTRANSPOSEMATRIX();
bool syntacticParseINSERT();
bool syntacticParseUPDATE();
bool syntacticParseDELETE();
bool syntacticParsePROJECTION();
bool syntacticParseSELECTION();
bool syntacticParseJOIN();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseSORT();
bool syntacticParseGROUPBY();
bool syntacticParseORDERBY();
bool syntacticParseSEARCH();

using namespace std; // Make std namespace accessible

bool syntacticParse()
{
    logger.log("syntacticParse");
    string possibleQueryType = tokenizedQuery[0];

    if (tokenizedQuery.size() < 1) {
        cout << "SYNTAX ERROR: No command entered." << endl;
        return false;
    }

    // Most commands need at least 2 tokens
    if (tokenizedQuery.size() < 2 && possibleQueryType != "LIST" )
    {
        cout << "SYNTAX ERROR: Insufficient parameters for command " << possibleQueryType << "." << endl;
        return false;
    }


    if (possibleQueryType == "CLEAR")
        return syntacticParseCLEAR();
    else if (possibleQueryType == "INDEX")
        return syntacticParseINDEX(); 
    else if (possibleQueryType == "LIST")
        return syntacticParseLIST();
    else if (possibleQueryType == "LOAD")
    {
        // LOAD MATRIX relationName
        if (tokenizedQuery.size() > 2 && tokenizedQuery[1] == "MATRIX") // Check token 1 for MATRIX keyword
            return syntacticParseLOADMATRIX();
        // LOAD relationName
        else
            return syntacticParseLOAD();
    }
    else if (possibleQueryType == "PRINT")
    {
        // PRINT MATRIX relationName
        if (tokenizedQuery.size() > 2 && tokenizedQuery[1] == "MATRIX") // Check token 1 for MATRIX keyword
            return syntacticParsePRINTMATRIX();
        // PRINT relationName
        else
            return syntacticParsePRINT();
    }
    else if (possibleQueryType == "RENAME")
    {
        // RENAME MATRIX oldName TO newName - Needs at least 5 tokens
        // RENAME tableName oldColumnName TO newColumnName - Needs at least 5 tokens
        // RENAME tableName TO newTableName - Needs at least 4 tokens
        // Distinguish based on keywords or token count if necessary,
        // but since RENAME MATRIX is removed, only table/column rename remains.
        // The specific parsing logic is inside syntacticParseRENAME()
        return syntacticParseRENAME();
    }
    else if (possibleQueryType == "EXPORT")
    {
        // EXPORT MATRIX relationName
        if (tokenizedQuery.size() > 2 && tokenizedQuery[1] == "MATRIX") // Check token 1 for MATRIX keyword
            return syntacticParseEXPORTMATRIX();
        // EXPORT relationName
        else
            return syntacticParseEXPORT();
    }
    else if (possibleQueryType == "SOURCE")
        return syntacticParseSOURCE();
    else if (possibleQueryType == "TRANSPOSE") // Assuming TRANSPOSE MATRIX relationName
        return syntacticParseTRANSPOSEMATRIX();
    else if (possibleQueryType == "CHECKSYMMETRY") // Assuming CHECKSYMMETRY MATRIX relationName
        return syntacticParseCHECKSYMMETRYMATRIX();
    else if (possibleQueryType == "COMPUTE") // Assuming COMPUTE MATRIX relationName
        return syntacticParseCOMPUTEMATRIX();
    else if (possibleQueryType == "ROTATE") // Assuming ROTATE MATRIX relationName
        return syntacticParseROTATEMATRIX();
    else if (possibleQueryType == "CHECKANTISYM") // Assuming CHECKANTISYM MATRIX relationName
        return syntacticParseCHECKANTISYMMATRIX();
    else if (possibleQueryType == "CROSSTRANSPOSE") // Assuming CROSSTRANSPOSE MATRIX relationName1 relationName2
        return syntacticParseCROSSTRANSPOSEMATRIX();
    else if (possibleQueryType == "INSERT")
        return syntacticParseINSERT(); 
    else if (possibleQueryType == "UPDATE")
        return syntacticParseUPDATE(); 
    else if (possibleQueryType == "DELETE")
        return syntacticParseDELETE(); 
    else 
    {
        string resultantRelationName = possibleQueryType;
        if (tokenizedQuery.size() < 3 || tokenizedQuery[1] != "<-" )
        {
            
            cout << "SYNTAX ERROR: Unknown command '" << possibleQueryType << "' or invalid assignment syntax." << endl;
            return false;
        }

        possibleQueryType = tokenizedQuery[2];
        if (possibleQueryType == "PROJECT")
            return syntacticParsePROJECTION();
        else if (possibleQueryType == "SELECT") // SELECT remains unmodified
            return syntacticParseSELECTION();
        else if (possibleQueryType == "SEARCH") // New SEARCH command
            return syntacticParseSEARCH();
        else if (possibleQueryType == "JOIN") // Check JOIN before GROUP BY which might be similar
             return syntacticParseJOIN(); 
        else if (possibleQueryType == "GROUP" && tokenizedQuery.size() > 3 && tokenizedQuery[3] == "BY")
            return syntacticParseGROUPBY(); // Keep GROUP BY
        else if (possibleQueryType == "ORDER" && tokenizedQuery.size() > 3 && tokenizedQuery[3] == "BY")
            return syntacticParseORDERBY(); // Keep ORDER BY
        else if (possibleQueryType == "CROSS")
            return syntacticParseCROSS();
        else if (possibleQueryType == "DISTINCT")
            return syntacticParseDISTINCT();
        else if (possibleQueryType == "SORT")
            return syntacticParseSORT();
        else if (possibleQueryType == "GROUP") // Assuming GROUP BY ...
            return syntacticParseGROUPBY();
        else if (possibleQueryType == "ORDER") // Assuming ORDER BY ...
            return syntacticParseORDERBY();
        else if (possibleQueryType == "SEARCH")
            return syntacticParseSEARCH();
        else
        {
            cout << "SYNTAX ERROR: Unknown command '" << possibleQueryType << "' after '<-'." << endl;
            return false;
        }
    }
    
    return false;
}

ParsedQuery::ParsedQuery()
{
    // Call clear() to initialize all members
    this->clear();
}

void ParsedQuery::clear()
{
    logger.log("ParseQuery::clear");
    this->queryType = UNDETERMINED;

    this->clearRelationName = "";

    this->crossResultRelationName = "";
    this->crossFirstRelationName = "";
    this->crossSecondRelationName = "";

    this->distinctResultRelationName = "";
    this->distinctRelationName = "";

    this->exportRelationName = "";

    this->indexingStrategy = NOTHING;
    this->indexColumnName = "";
    this->indexRelationName = "";

    this->joinBinaryOperator = NO_BINOP_CLAUSE;
    this->joinResultRelationName = "";
    this->joinFirstRelationName = "";
    this->joinSecondRelationName = "";
    this->joinFirstColumnName = "";
    this->joinSecondColumnName = "";

    this->loadRelationName = "";

    this->printRelationName = "";

    this->projectionResultRelationName = "";
    this->projectionColumnList.clear();
    this->projectionRelationName = "";

    this->renameFromColumnName = "";
    this->renameToColumnName = "";
    this->renameRelationName = "";
    this->renameFromTableName = ""; // Added for RENAME TABLE TO
    this->renameToTableName = "";   // Added for RENAME TABLE TO

    // Reset fields used by SELECT and SEARCH
    this->selectType = NO_SELECT_CLAUSE;
    this->selectionBinaryOperator = NO_BINOP_CLAUSE;
    this->selectionResultRelationName = "";
    this->selectionRelationName = "";
    this->selectionFirstColumnName = "";
    this->selectionSecondColumnName = ""; 
    this->selectionIntLiteral = 0;

    // Reset Sort fields
    this->sortingStrategies.clear();
    this->sortResultRelationName = "";
    this->sortColumns.clear();
    this->sortRelationName = "";

    this->sourceFileName = "";

    // Reset Matrix fields
    this->loadMatrixName = "";
    this->printMatrixName = "";
    this->exportMatrixName = "";
    this->crossTransposeMatrixName1 = "";
    this->crossTransposeMatrixName2 = "";
    this->rotateMatrixName = "";
    this->checkAntiSymMatrixName1 = "";
    this->checkAntiSymMatrixName2 = "";

    // Reset Group By fields
    this->aggregateFunction = ""; 
    this->groupByColumn = "";

    this->resultTableName="";
    this->havingFunction="";
    this->havingAttribute=""; 
    this->havingOperator="";
    this->havingValue=""; 
    this->returnFunction="";
    this->returnAttribute=""; 

    // Reset Order By fields

    this->orderByTableName = ""; 
    this->orderByRelation = ""; 
    this->orderByColumn = ""; 
    this->orderByColumnName=""; 
    this->orderByRelationName=""; 
    this->orderByResultRelationName=""; 
    this->orderByStrategy = NO_SORT_CLAUSE; // Use enum NO_SORT_CLAUSE

    
    this->tableName= ""; // General table name field
    this->insertColumnsAndValues.clear();

}

/**
 * @brief Checks to see if source file exists. Called when LOAD command is
 * invoked. Checks in ../data/ directory.
 *
 * @param tableName Name of the table (without .csv)
 * @return true if ../data/tableName.csv exists
 * @return false otherwise
 */
bool isFileExists(const string &fileName)
{
    struct stat buffer;
    logger.log("Checking if file exists: " + fileName);
    bool exists = (stat(fileName.c_str(), &buffer) == 0);
    logger.log(exists ? "File found." : "File not found.");
    return exists;
}

/**
 * @brief Checks to see if query source file exists. Called when SOURCE command is
 * invoked. Checks in ../data/ directory.
 *
 * @param fileName Name of the query file (without .ra)
 * @return true if ../data/fileName.ra exists
 * @return false otherwise
 */
bool isQueryFile(const string &fileName)
{
    struct stat buffer;
     logger.log("Checking if query file exists: " + fileName);
    bool exists = (stat(fileName.c_str(), &buffer) == 0);
    logger.log(exists ? "Query file found." : "Query file not found.");
    return exists;
}