#include "global.h"
#include "../bufferManager.h"
#include <fstream>
#include <sstream>

/**
 * @brief 
 * SYNTAX: CROSSTRANSPOSE <matrix_name1> <matrix_name2>
 */

bool syntacticParseCROSSTRANSPOSEMATRIX() {
    logger.log("syntacticParseCROSSTRANSPOSEMATRIX");
    if (tokenizedQuery.size() != 3) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CROSSTRANSPOSE;
    parsedQuery.crossTransposeMatrixName1 = tokenizedQuery[1];
    parsedQuery.crossTransposeMatrixName2 = tokenizedQuery[2];
    return true;
}

bool semanticParseCROSSTRANSPOSEMATRIX() {
    logger.log("semanticParseCROSSTRANSPOSEMATRIX");
    if (matrixCatalogue.hasMatrix(parsedQuery.crossTransposeMatrixName1) &&
        matrixCatalogue.hasMatrix(parsedQuery.crossTransposeMatrixName2))
        return true;
    cout << "SEMANTIC ERROR: One or both matrices do not exist" << endl;
    return false;
}

void transposeMatrix(vector<vector<int>>& matrix, int dimension) {
    for (int i = 0; i < dimension; ++i) {
        for (int j = i + 1; j < dimension; ++j) {
            swap(matrix[i][j], matrix[j][i]);
        }
    }
}

void executeCROSSTRANSPOSEMATRIX() {
    logger.log("executeCROSSTRANSPOSEMATRIX");

    BufferManager bufferManager;
    int dimension1, dimension2;
    vector<vector<int>> matrix1 = bufferManager.loadMatrix(parsedQuery.crossTransposeMatrixName1, dimension1);
    vector<vector<int>> matrix2 = bufferManager.loadMatrix(parsedQuery.crossTransposeMatrixName2, dimension2);

    if (!matrix1.empty() && !matrix2.empty() && dimension1 == dimension2) {
        transposeMatrix(matrix1, dimension1);
        transposeMatrix(matrix2, dimension2);

        bufferManager.writeMatrix(parsedQuery.crossTransposeMatrixName1, matrix2, dimension2);
        bufferManager.writeMatrix(parsedQuery.crossTransposeMatrixName2, matrix1, dimension1);

        cout << "Matrices " << parsedQuery.crossTransposeMatrixName1 << " and " << parsedQuery.crossTransposeMatrixName2 << " transposed and swapped successfully." << endl;
    } else {
        cout << "Error: Matrices are either empty or not of equal size." << endl;
    }
}