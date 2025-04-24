#ifndef SYNTACTICPARSER_H
#define SYNTACTICPARSER_H
#include "tableCatalogue.h"
#include "matrixCatalogue.h"
#include <vector> // Include vector
#include <string> // Include string

using namespace std;

enum QueryType
{
    CLEAR,
    CROSS,
    DISTINCT,
    EXPORT,
    INDEX,
    JOIN,
    LIST,
    LOAD,
    PRINT,
    PROJECTION,
    RENAME,
    SELECTION,
    SORT,
    SOURCE,
    GROUP_BY,
    ORDER_BY,
    SEARCH,
    INSERT,
    UPDATE,
    DELETE,

    LOAD_MATRIX,
    PRINT_MATRIX,
    EXPORT_MATRIX,
    TRANSPOSE_MATRIX, // Added (assuming this maps to TRANSPOSEMATRIX)
    CHECKSYMMETRY,    // Added (assuming this maps to CHECKSYMMETRYMATRIX)
    COMPUTE,          // Added (assuming this maps to COMPUTEMATRIX)
    CROSSTRANSPOSE,
    ROTATE,
    CHECKANTISYM,
    UNDETERMINED
};

enum BinaryOperator
{
    LESS_THAN,
    GREATER_THAN,
    LEQ,
    GEQ,
    EQUAL,
    NOT_EQUAL,
    NO_BINOP_CLAUSE
};

enum SortingStrategy
{
    ASC,
    DESC,
    NO_SORT_CLAUSE
};
enum IndexingStrategy
{
    BTREE,
    HASH,
    NOTHING
};

enum SelectType
{
    COLUMN,
    INT_LITERAL,
    NO_SELECT_CLAUSE
};

class ParsedQuery
{

public:
    QueryType queryType = UNDETERMINED;

    string clearRelationName = "";

    string crossResultRelationName = "";
    string crossFirstRelationName = "";
    string crossSecondRelationName = "";

    string distinctResultRelationName = "";
    string distinctRelationName = "";

    string exportRelationName = "";

    IndexingStrategy indexingStrategy = NOTHING;
    string indexColumnName = "";
    string indexRelationName = "";

    BinaryOperator joinBinaryOperator = NO_BINOP_CLAUSE;
    string joinResultRelationName = "";
    string joinFirstRelationName = "";
    string joinSecondRelationName = "";
    string joinFirstColumnName = "";
    string joinSecondColumnName = "";

    string loadRelationName = "";

    string printRelationName = "";

    string projectionResultRelationName = "";
    vector<string> projectionColumnList;
    string projectionRelationName = "";

    string renameFromColumnName = "";
    string renameToColumnName = "";
    string renameRelationName = "";

    SelectType selectType = NO_SELECT_CLAUSE;
    BinaryOperator selectionBinaryOperator = NO_BINOP_CLAUSE;
    string selectionResultRelationName = "";
    string selectionRelationName = "";
    string selectionFirstColumnName = "";
    string selectionSecondColumnName = "";
    int selectionIntLiteral = 0;

    vector<SortingStrategy> sortingStrategies;
    string sortResultRelationName = "";
    vector<string> sortColumns;
    string sortRelationName = "";

    string sourceFileName = "";

    // New matrix field
    string loadMatrixName = "";
    string printMatrixName = "";
    string exportMatrixName = "";
    string crossTransposeMatrixName1 = "";
    string crossTransposeMatrixName2 = "";
    string rotateMatrixName = "";
    string checkAntiSymMatrixName1 = "";
    string checkAntiSymMatrixName2 = "";
    string aggregateFunction = "";
    string groupByColumn = "";
    string resultRelationName = "";
    string orderByAttributeName = "";
    string orderByType = "";
    string orderByOrder = "";
    string orderByTableName = "";
    string orderByRelation = "";
    string orderByColumn = "";
    string orderByAscending = "";
    string orderByResult = "";
    string orderByColumnName = "";
    string orderByRelationName = "";
    string orderByResultRelationName = "";
    string tableName = "";
    string havingFunction = "";
    string havingColumn = "";
    string havingOperator = "";
    string havingValue = "";
    string returnFunction = "";
    string returnColumn = "";
    string resultTableName = "";
    string havingAttribute = "";
    string returnAttribute = "";
    SortingStrategy orderByStrategy;

    string updateTableName = "";
    string updateConditionColumn = "";
    int updateConditionValue = 0;
    string updateTargetColumn = "";
    int updateTargetValue = 0;
    string updateConditionOperator = "";
    
    vector<pair<string, int>> insertColumnsAndValues;

    ParsedQuery();
    void clear();

}; // Corrected end of class definition

bool syntacticParse();
bool syntacticParseCLEAR();
bool syntacticParseCROSS();
bool syntacticParseDISTINCT();
bool syntacticParseEXPORT();
bool syntacticParseINDEX();
bool syntacticParseJOIN();
bool syntacticParseLIST();
bool syntacticParseLOAD();
bool syntacticParsePRINT();
bool syntacticParsePROJECTION();
bool syntacticParseRENAME();
bool syntacticParseSELECTION();
bool syntacticParseSORT();
bool syntacticParseSOURCE();
bool syntacticParseINSERT();
bool syntacticParseUPDATE();
bool syntacticParseDELETE();
// New prototype for LOAD MATRIX
bool syntacticParseLOADMATRIX();
bool syntacticParsePRINTMATRIX();
bool syntacticParseEXPORTMATRIX();
bool syntacticParseCROSSTRANSPOSEMATRIX();
bool syntacticParseROTATEMATRIX();
bool syntacticParseCHECKANTISYMMATRIX();
bool syntacticParseORDERBY();
bool syntacticParseGROUPBY();
bool syntacticParseSEARCH();
bool syntacticParseTRANSPOSEMATRIX(); // Added declaration
bool syntacticParseCHECKSYMMETRY();   // Added declaration
bool syntacticParseCOMPUTE();         // Added declaration

bool isFileExists(string tableName);
bool isQueryFile(string fileName);

#endif // SYNTACTICPARSER_H