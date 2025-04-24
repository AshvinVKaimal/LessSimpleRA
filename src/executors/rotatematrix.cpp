#include "global.h"
#include "../bufferManager.h"
#include <fstream>
#include <sstream>
#include "executor.h"
#include "tableCatalogue.h"
#include "syntacticParser.h"
#include "table.h" // Include full Table definition

using namespace std;

/**
 * @brief 
 * SYNTAX: ROTATE relation_name
 */

bool syntacticParseROTATEMATRIX() {
    logger.log("syntacticParseROTATEMATRIX");
    if (tokenizedQuery.size() != 2) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = ROTATE;
    parsedQuery.rotateMatrixName = tokenizedQuery[1];
    return true;
}

bool semanticParseROTATEMATRIX() {
    logger.log("semanticParseROTATEMATRIX");
    if (matrixCatalogue.hasMatrix(parsedQuery.rotateMatrixName))
        return true;
    cout << "SEMANTIC ERROR: No such matrix exists" << endl;
    return false;
}

void executeROTATEMATRIX() {
    logger.log("executeROTATEMATRIX");

    BufferManager bufferManager;
    int dimension;
    vector<vector<int>> matrix = bufferManager.loadMatrix(parsedQuery.rotateMatrixName, dimension);

    if (!matrix.empty()) {
        int n = dimension;

        for (int i = 0; i < n / 2; ++i) {
            for (int j = i; j < n - i - 1; ++j) {
                int temp = matrix[i][j];
                matrix[i][j] = matrix[n - j - 1][i];
                matrix[n - j - 1][i] = matrix[n - i - 1][n - j - 1];
                matrix[n - i - 1][n - j - 1] = matrix[j][n - i - 1];
                matrix[j][n - i - 1] = temp;
            }
        }

        // Write the rotated matrix back to the pages
        int pageIndex = 0;
        for (int i = 0; i < n; ++i) {
            bufferManager.writeBlock(parsedQuery.rotateMatrixName, pageIndex++, matrix[i]);
        }

        cout << "Matrix " << parsedQuery.rotateMatrixName << " rotated successfully." << endl;
    } else {
        cout << "Error: Matrix is empty." << endl;
    }
}