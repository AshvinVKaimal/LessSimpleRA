#include "matrixCatalogue.h"

void MatrixCatalogue::addMatrix(const string &name, const vector<vector<int>> &matrix) {
    matrices[name] = matrix;
}

bool MatrixCatalogue::hasMatrix(const string &name) const {
    return matrices.find(name) != matrices.end();
}

vector<vector<int>>* MatrixCatalogue::getMatrix(const string &name) {
    auto it = matrices.find(name);
    if(it != matrices.end()){
        return &it->second;
    }
    return nullptr;
}

const vector<vector<int>>* MatrixCatalogue::getMatrix(const string &name) const {
    auto it = matrices.find(name);
    if(it != matrices.end()){
        return &it->second;
    }
    return nullptr;
}

void MatrixCatalogue::removeMatrix(const string &name) {
    matrices.erase(name);
}

void MatrixCatalogue::clear() {
    matrices.clear();
}