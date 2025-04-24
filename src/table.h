#ifndef TABLE_H // Added include guards if missing
#define TABLE_H

#include "cursor.h"
#include <map>
#include <vector>
#include <string>
#include <unordered_set> // For distinctValuesInColumns
#include <unordered_map> // For multiColumnIndexData outer map
#include <utility>

// Bring commonly used std names into scope for this header
using std::vector;
using std::string;
using std::unordered_set;
using std::map;
using std::unordered_map;
using std::pair;
using std::ostream;
using std::ofstream;
using std::cerr;
using std::endl;
using std::ios;
using std::to_string; // For logging if needed inside Table
// Include global logger if needed directly in Table methods
// #include "logger.h

enum IndexingStrategy
{
    BTREE, // Represents std::map for now
    HASH,  // Currently unused
    NOTHING
};

/**
 * @brief The Table class holds all information related to a loaded table. It
 * also implements methods that interact with the parsers, executors, cursors
 * and the buffer manager. There are typically 2 ways a table object gets
 * created through the course of the workflow - the first is by using the LOAD
 * command and the second is to use assignment statements (SELECT, PROJECT,
 * JOIN, SORT, CROSS and DISTINCT).
 *
 */
class Table
{

public:
    // Type alias for row location {pageIndex, rowIndexInPage}
    using RowLocation = pair<int, int>;
    // Key: Column Name -> Value: The index map (ColumnValue -> Location) for that column
    unordered_map<string, map<int, RowLocation>> multiColumnIndexData; // <<< Index for all columns

    vector<unordered_set<int> > distinctValuesInColumns; // Still useful for optimization stats
    // std::map<int, RowLocation> indexMapData; // <<< Replaced by multiColumnIndexData
    string sourceFileName = "";
    string tableName = "";
    vector<string> columns;
    vector<uint> distinctValuesPerColumnCount;

    // Schema & storage
    string sourceFileName;
    string tableName;
    vector<string> columns;
    uint columnCount = 0;
    long long rowCount = 0;
    uint blockCount = 0;
    uint maxRowsPerBlock = 0;
    vector<uint> rowsPerBlockCount;
    // Removed explicit index flags, check multiColumnIndexData instead
    // bool indexed = false;
    // string indexedColumn = "";
    // IndexingStrategy indexingStrategy = NOTHING;


    bool extractColumnNames(string firstLine);
    bool blockify();
    void updateStatistics(vector<int> row);
    Table();
    Table(const string &tableName);
    Table(const string &tableName, const vector<string> &columns);
    ~Table() = default;

    // Load & persistence
    bool load(); // LOAD command
    bool isPermanent() const;
    void makePermanent(); // EXPORT command
    void unload();        // CLEAR or DROP

    // Schema inspection
    bool extractColumnNames(const string &firstLine);
    bool isColumn(const string &columnName) const;
    int getColumnIndex(const string &columnName) const;
    void renameColumn(const string &fromColumnName,
                      const string &toColumnName);

    // Statistics update
    void updateStatistics(const vector<int> &row);

    // Blockify and I/O
    bool blockify();    // split into pages
    void print() const; // PRINT command
    void getNextPage(Cursor *cursor);
    Cursor getCursor();
    int getColumnIndex(string columnName);
    void unload();


    // --- Indexing Methods ---
    void clearIndex();                            // Clears all index data
    bool isIndexed(const string& columnName) const; // Checks if a specific column is indexed
    // Removed getIndexedColumn() and getIndexingStrategy()


    /**
 * @brief Static function that takes a vector of valued and prints them out in a
 * comma seperated format.
 *
 * @tparam T current usaages include int and string
 * @param row
 */
template <typename T>
void writeRow(vector<T> row, ostream &fout)
{
    // logger.log("Table::printRow"); // Keep logger if needed
    for (size_t columnCounter = 0; columnCounter < row.size(); columnCounter++) // Use size_t for vector size comparison
    {
        if (columnCounter != 0)
            fout << ", ";
        fout << row[columnCounter];
    }
    fout << endl;
}

/**
 * @brief Static function that takes a vector of valued and prints them out in a
 * comma seperated format. Appends to the source file.
 *
 * @tparam T current usaages include int and string
 * @param row
 */
template <typename T>
void writeRow(vector<T> row)
{
    // logger.log("Table::printRow"); // Keep logger if needed
    // Need valid sourceFileName for this overload
    if (this->sourceFileName.empty()) {
         // Handle error: Cannot write row without a source file path
         cerr << "ERROR: Table::writeRow called without a valid sourceFileName for table " << this->tableName << endl;
         return;
     }
    // Check if file exists? Or just let ofstream create/append? Append is usually desired here.
    ofstream fout(this->sourceFileName, ios::app);
    if (!fout.is_open()) {
         // Handle error: Could not open file
         cerr << "ERROR: Could not open source file " << this->sourceFileName << " for appending." << endl;
         return;
    }
    // Call the other overload to do the actual formatting and writing
    this->writeRow<T>(row, fout);
    fout.close();
}
};

#endif // TABLE_H