#include "global.h"
#include "tableCatalogue.h"
#include "table.h"  // Include the full definition of Table
#include <iostream> // Include for cout

using namespace std;

void TableCatalogue::insertTable(Table *table)
{
    logger.log("TableCatalogue::~insertTable");
    this->tables[table->tableName] = table;
}
// Change parameter type to const string&
void TableCatalogue::deleteTable(const string &tableName)
{
    logger.log("TableCatalogue::deleteTable");
    // Check if table exists before accessing it
    if (this->tables.count(tableName))
    {
        this->tables[tableName]->unload();
        delete this->tables[tableName];
        this->tables.erase(tableName);
    }
    else
    {
        logger.log("TableCatalogue::deleteTable: Warning - Attempted to delete non-existent table '" + tableName + "'.");
    }
}
// Change parameter type to const string&
Table *TableCatalogue::getTable(const string &tableName)
{
    logger.log("TableCatalogue::getTable");
    // Use find to avoid inserting if not found
    auto it = this->tables.find(tableName);
    if (it != this->tables.end())
    {
        return it->second;
    }
    // logger.log("TableCatalogue::getTable: Warning - Table '" + tableName + "' not found.");
    return nullptr; // Return nullptr if table doesn't exist
}
// Change parameter type to const string&
bool TableCatalogue::isTable(const string &tableName)
{
    logger.log("TableCatalogue::isTable");
    return this->tables.count(tableName); // count is efficient for checking existence
}

// Change parameter types to const string&
bool TableCatalogue::isColumnFromTable(const string &columnName, const string &tableName)
{
    logger.log("TableCatalogue::isColumnFromTable");
    if (this->isTable(tableName))
    {
        Table *table = this->getTable(tableName);
        // Ensure table pointer is not null before accessing
        if (table && table->isColumn(columnName))
            return true;
    }
    return false;
}

void TableCatalogue::print()
{
    logger.log("TableCatalogue::print");
    cout << "\nRELATIONS" << endl;

    int rowCount = 0;
    for (auto rel : this->tables)
    {
        cout << rel.first << endl;
        rowCount++;
    }
    printRowCount(rowCount);
}

TableCatalogue::~TableCatalogue()
{
    logger.log("TableCatalogue::~TableCatalogue");
    for (auto table : this->tables)
    {
        table.second->unload();
        delete table.second;
    }
}
