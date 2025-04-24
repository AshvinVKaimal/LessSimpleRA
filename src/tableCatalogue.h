#ifndef TABLECATALOGUE_H // Add include guard
#define TABLECATALOGUE_H // Add include guard

#include <string>
#include <unordered_map>
#include "logger.h" // Include Logger for logging capabilities

using namespace std;

// Forward declare Table instead of including the full header
class Table;

/**
 * @brief The TableCatalogue acts like an index of tables existing in the
 * system. Everytime a table is added(removed) to(from) the system, it needs to
 * be added(removed) to(from) the tableCatalogue.
 *
 */
class TableCatalogue
{

    unordered_map<string, Table *> tables;

public:
    TableCatalogue() {}
    void insertTable(Table *table);
    void deleteTable(const std::string &tableName);                                      // Use const&
    Table *getTable(const std::string &tableName);                                       // Use const&
    bool isTable(const std::string &tableName);                                          // Use const&
    bool isColumnFromTable(const std::string &columnName, const std::string &tableName); // Added declaration
    bool isMatrix(string matrixName);                                                    // Assuming this is used elsewhere
    void print();
    ~TableCatalogue();
};

#endif // TABLECATALOGUE_H // End include guard