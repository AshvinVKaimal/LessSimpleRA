#ifndef MATRIXCATALOGUE_H
#define MATRIXCATALOGUE_H

#include <string>
#include <vector>
#include <unordered_map>

using namespace std;

class MatrixCatalogue {
public:
    void addMatrix(const string &name, const vector<vector<int>> &matrix);
    bool hasMatrix(const string &name) const;
    // const vector<vector<int>>* getMatrix(const string &name) const;
    void removeMatrix(const string &name);
    void clear();
    vector<vector<int>>* getMatrix(const string &name); 
    const vector<vector<int>>* getMatrix(const string &name) const; 

private:
    unordered_map<string, vector<vector<int>>> matrices;
};

#endif // MATRIXCATALOGUE_H
