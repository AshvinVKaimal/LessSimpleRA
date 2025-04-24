#include "global.h"
#include "executor.h"
#include "tableCatalogue.h"
#include "syntacticParser.h"
#include "table.h" // Include full Table definition

using namespace std;
/**
 * @brief 
 * SYNTAX: PRINT_MATRIX relation_name
 */



bool syntacticParsePRINTMATRIX() {
    logger.log("syntacticParsePRINTMATRIX");
    if (tokenizedQuery.size() != 2) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = PRINT_MATRIX;
    parsedQuery.printMatrixName = tokenizedQuery[1];
    return true;
}

bool semanticParsePRINTMATRIX() {
    logger.log("semanticParsePRINTMATRIX");
    if (!matrixCatalogue.hasMatrix(parsedQuery.printMatrixName)) {
        cout << "SEMANTIC ERROR: Matrix does not exist" << endl;
        return false;
    }
    return true;
}

void executePRINTMATRIX() {
    logger.log("executePRINTMATRIX");

    BufferManager bufferManager;
    int pageIndex = 0;
    string matrixName = parsedQuery.printMatrixName;
    string fileName;
    ifstream fin;
    string line;
    int rowCount = 0;
    const int maxRows = 20;
    const int maxCols = 20;

    while (rowCount < maxRows) {
        fileName = "../data/temp/" + matrixName + "_Page" + to_string(pageIndex) + ".matrix";
        fin.open(fileName);
        if (!fin.is_open()) break;

        while (getline(fin, line) && rowCount < maxRows) {
            stringstream ss(line);
            string value;
            int colCount = 0;
            while (getline(ss, value, ' ') && colCount < maxCols) {
                cout << value << " ";
                colCount++;
            }
            cout << endl;
            rowCount++;
        }
        fin.close();
        pageIndex++;
    }

    if (pageIndex == 0) {
        cout << "Error: Matrix is empty or does not exist." << endl;
    } else {
        cout << "Matrix " << matrixName << " printed successfully." << endl;
    }
}