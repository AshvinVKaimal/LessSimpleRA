#include "global.h"
#include "../matrix.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <exception>

/**
 * @brief 
 * SYNTAX: LOAD_MATRIX relation_name
 */
void executeLOADMATRIX() {
    // printf("executeLOADMATRIX\n");
    logger.log("executeLOADMATRIX");

    Matrix *matrix = new Matrix(parsedQuery.loadMatrixName);
    if (matrix->load()) {
        matrixCatalogue.addMatrix(parsedQuery.loadMatrixName, matrix->data);
        cout << "Matrix " << parsedQuery.loadMatrixName << " loaded successfully." << endl;
    }
}

bool syntacticParseLOADMATRIX() {
    logger.log("syntacticParseLOADMATRIX");
    if (tokenizedQuery.size() != 2) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = LOAD_MATRIX;
    parsedQuery.loadMatrixName = tokenizedQuery[1];
    // printf("syntacticParseLOADMATRIX\n");
    return true;
}

bool semanticParseLOADMATRIX() {
    logger.log("semanticParseLOADMATRIX");
    if (!isFileExists(parsedQuery.loadMatrixName))
    {
        cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
        return false;
    }
    // printf("semanticParseLOADMATRIX\n");
    return true;
}