#include "global.h"
#include "executor.h"
#include <iostream>   // Include the iostream library
#include <sys/stat.h> // Include for stat
#include <fstream>    // Include for ifstream

// Forward declarations or include headers for specific execute functions
void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeINDEX();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executeLOADMATRIX();
void executePRINT();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();
void executePRINTMATRIX();
void executeEXPORTMATRIX();
void executeCROSSTRANSPOSEMATRIX();
void executeROTATEMATRIX();
void executeCHECKANTISYMMATRIX();
void executeORDERBY();
void executeGROUPBY();
void executeSEARCH();
void executeINSERT();
void executeUPDATE();
void executeDELETE();
// Remove RENAME MATRIX related functions if they existed
// void executeRENAMEMATRIX(); // Example if it existed

using namespace std; // Make std namespace accessible

// Define isFileExists function
bool isFileExists(const string &filename)
{
    struct stat buffer;
    return (stat(filename.c_str(), &buffer) == 0);
}

// Define isQueryFile function (simple check if file exists and is readable)
bool isQueryFile(const string &filename)
{
    // Check if file exists using the other function
    if (!isFileExists(filename))
    {
        return false;
    }
    // Try opening the file for reading
    ifstream fin(filename, ios::in);
    if (!fin)
    {
        // Failed to open (e.g., permissions issue)
        return false;
    }
    fin.close();
    return true; // File exists and is readable
}

void executeCommand()
{
    // Print the parsed commands for debugging
    // printf("First token: %s\n", tokenizedQuery[0].c_str());
    // printf("Second token: %s\n", tokenizedQuery[1].c_str());
    // printf("ehfigeiughiehgfieghegiegh\n");

    switch (parsedQuery.queryType)
    {
    case CLEAR:
        executeCLEAR();
        break;
    case CROSS:
        executeCROSS();
        break;
    case DISTINCT:
        executeDISTINCT();
        break;
    case EXPORT:
        executeEXPORT();
        break;
    case INDEX:
        executeINDEX();
        break;
    case JOIN:
        executeJOIN();
        break;
    case LIST:
        executeLIST();
        break;
    case LOAD:
        executeLOAD();
        break;
    case LOAD_MATRIX:
        executeLOADMATRIX();
        break; // Ensure this case is present
    case PRINT:
        executePRINT();
        break;
    case PROJECTION:
        executePROJECTION();
        break;
    case RENAME:
        executeRENAME(); // Keep RENAME for tables/columns
        break;
    // RENAME_MATRIX case removed
    case SELECTION:
        executeSELECTION();
        break;
    case SORT:
        executeSORT();
        break;
    case SOURCE:
        executeSOURCE();
        break;
    case PRINT_MATRIX:
        executePRINTMATRIX();
        break;
    case EXPORT_MATRIX:
        executeEXPORTMATRIX();
        break;
    case CROSSTRANSPOSE:
        executeCROSSTRANSPOSEMATRIX();
        break;
    case ROTATE:
        executeROTATEMATRIX();
        break;
    case CHECKANTISYM:
        executeCHECKANTISYMMATRIX();
        break;
    case ORDER_BY:
        executeORDERBY();
        break;
    case GROUP_BY:
        executeGROUPBY();
        break;
    case SEARCH:
        executeSEARCH();
        break;
    case INSERT:
        executeINSERT();
        break;
    case UPDATE:
        executeUPDATE();
        break;
    case DELETE:
        executeDELETE();
        break;
    default:
        cout << "PARSING ERROR" << endl;
    }
    return;
}

void printRowCount(int rowCount)
{
    cout << "\n\nRow Count: " << rowCount << endl;
    return;
}