#ifndef MATRIX_H
#define MATRIX_H

#include <vector>
#include <string>

using namespace std;

class Matrix {
public:
    string matrixName;
    int dimension;
    vector<vector<int>> data;

    Matrix(string matrixName);
    bool load();
    void writeMatrix();
    bool blockify();
    void writePage(int pageIndex, const vector<int>& rowData);


};

#endif // MATRIX_H