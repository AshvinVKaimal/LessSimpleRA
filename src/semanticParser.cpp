#include "logger.h" // Include Logger class definition
#include "global.h" // Include global definitions (logger, etc.)
#include <iostream> // Include for cout, endl

// Forward declarations or include headers for specific semantic parse functions
bool semanticParseCLEAR();
bool semanticParseCROSS();
bool semanticParseDISTINCT();
bool semanticParseEXPORT();
bool semanticParseINDEX();
bool semanticParseJOIN();
bool semanticParseLIST();
bool semanticParseLOAD();
bool semanticParseLOADMATRIX();
bool semanticParsePRINT();
bool semanticParsePROJECTION();
bool semanticParseRENAME();
bool semanticParseSELECTION();
bool semanticParseSORT();
bool semanticParseSOURCE();
bool semanticParsePRINTMATRIX();
bool semanticParseEXPORTMATRIX();
bool semanticParseROTATEMATRIX();
bool semanticParseCHECKANTISYMMATRIX();
bool semanticParseCROSSTRANSPOSEMATRIX();
bool semanticParseGROUPBY();
bool semanticParseORDERBY();
bool semanticParseSEARCH();
bool semanticParseINSERT();
bool semanticParseUPDATE();
bool semanticParseDELETE();
// Remove RENAME MATRIX related functions if they existed
// bool semanticParseRENAMEMATRIX(); // Example if it existed

using namespace std; // Make std namespace accessible

bool semanticParse()
{
    logger.log("semanticParse");
    switch (parsedQuery.queryType)
    {
    case CLEAR:
        return semanticParseCLEAR();
    case CROSS:
        return semanticParseCROSS();
    case DISTINCT:
        return semanticParseDISTINCT();
    case EXPORT:
        return semanticParseEXPORT();
    case INDEX:
        return semanticParseINDEX();
    case JOIN:
        return semanticParseJOIN();
    case LIST:
        return semanticParseLIST();
    case LOAD:
        return semanticParseLOAD();
    case LOAD_MATRIX:
        return semanticParseLOADMATRIX(); // new case
    case PRINT:
        return semanticParsePRINT();
    case PROJECTION:
        return semanticParsePROJECTION();
    case RENAME:
        return semanticParseRENAME(); // Keep RENAME for tables/columns
    // RENAME_MATRIX case removed
    case SELECTION:
        return semanticParseSELECTION();
    case SORT:
        return semanticParseSORT();
    case SOURCE:
        return semanticParseSOURCE();
    case PRINT_MATRIX:
        return semanticParsePRINTMATRIX();
    case EXPORT_MATRIX:
        return semanticParseEXPORTMATRIX();
    case ROTATE:
        return semanticParseROTATEMATRIX();
    case CHECKANTISYM:
        return semanticParseCHECKANTISYMMATRIX();
    case CROSSTRANSPOSE:
        return semanticParseCROSSTRANSPOSEMATRIX();
    case GROUP_BY:
        return semanticParseGROUPBY();
    case ORDER_BY:
        return semanticParseORDERBY();
    case SEARCH:
        return semanticParseSEARCH();
    case INSERT:
        return semanticParseINSERT();
    case UPDATE:
        return semanticParseUPDATE();
    case DELETE:
        return semanticParseDELETE();
    default:
        cout << "SEMANTIC ERROR: Unrecognized query type." << endl; // More specific error
    }
    return false;
}
