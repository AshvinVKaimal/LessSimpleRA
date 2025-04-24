#include "global.h"

/**
 * @brief 
 * SYNTAX: EXPORT_MATRIX <matrix_name> 
 */

bool syntacticParseEXPORTMATRIX()
{
    logger.log("syntacticParseEXPORTMATRIX");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = EXPORT_MATRIX;
    parsedQuery.exportMatrixName = tokenizedQuery[1];
    return true;
}

bool semanticParseEXPORTMATRIX()
{
    logger.log("semanticParseEXPORTMATRIX");
    if (matrixCatalogue.hasMatrix(parsedQuery.exportMatrixName))
        return true;
    cout << "SEMANTIC ERROR: No such matrix exists" << endl;
    return false;
}

void executeEXPORTMATRIX()
{
    logger.log("executeEXPORTMATRIX");
    const vector<vector<int>>* matrix = matrixCatalogue.getMatrix(parsedQuery.exportMatrixName);
    if (matrix) {
        string fileName = "../data/" + parsedQuery.exportMatrixName + ".csv";
        ofstream fout(fileName, ios::trunc);
        for (const auto& row : *matrix) {
            for (size_t i = 0; i < row.size(); ++i) {
                if (i != 0)
                    fout << ",";
                fout << row[i];
            }
            fout << endl;
        }
        fout.close();
        cout << "Matrix " << parsedQuery.exportMatrixName << " exported successfully." << endl;
    }
}