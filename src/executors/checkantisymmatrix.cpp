#include "global.h"
#include "matrix.h"
/**
 * @brief 
 * SYNTAX: CHECKANTISYM relation_name relation_name
 */
bool syntacticParseCHECKANTISYMMATRIX() {
    logger.log("syntacticParseCHECKANTISYM");
    if (tokenizedQuery.size() != 3) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = CHECKANTISYM;
    parsedQuery.checkAntiSymMatrixName1 = tokenizedQuery[1];
    parsedQuery.checkAntiSymMatrixName2 = tokenizedQuery[2];
    return true;
}

bool semanticParseCHECKANTISYMMATRIX() {
    logger.log("semanticParseCHECKANTISYM");
    if (matrixCatalogue.hasMatrix(parsedQuery.checkAntiSymMatrixName1) &&
        matrixCatalogue.hasMatrix(parsedQuery.checkAntiSymMatrixName2))
        return true;
    cout << "SEMANTIC ERROR: One or both matrices do not exist" << endl;
    return false;
}

void executeCHECKANTISYMMATRIX() {
    logger.log("executeCHECKANTISYM");
    
    Matrix matrixA(parsedQuery.checkAntiSymMatrixName1);
    Matrix matrixB(parsedQuery.checkAntiSymMatrixName2);
    
    if (!matrixA.load() || !matrixB.load()) {
        cout << "Error: Unable to load one or both matrices." << endl;
        return;
    }
    
    int n = matrixA.dimension;
    if (matrixB.dimension != n) {
        cout << "False" << endl; // Matrices must be the same dimension
        return;
    }
    
    // Compute -B^T using at most 2 temporary matrices
    Matrix transposeB("tempTransposeB");
    transposeB.data.resize(n, vector<int>(n, 0));
    
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            transposeB.data[j][i] = -matrixB.data[i][j];
        }
    }
    
    // Compare A with -B^T
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < n; j++) {
            if (matrixA.data[i][j] != transposeB.data[i][j]) {
                cout << "False" << endl;
                return;
            }
        }
    }
    
    cout << "True" << endl;
}

