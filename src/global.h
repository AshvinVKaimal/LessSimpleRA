#ifndef GLOBAL_H
#define GLOBAL_H

#include <vector> // Include vector for tokenizedQuery declaration
#include <string> // Include string for tokenizedQuery declaration
// #include "executor.h" // Forward declare ParsedQuery instead
#include "logger.h"
#include "bufferManager.h"
#include "tableCatalogue.h"
#include "matrixCatalogue.h"

// Forward declare ParsedQuery instead of including the full header
class ParsedQuery;

// Define constants used globally
extern const unsigned int BLOCK_SIZE;     // Size of a data block/page in bytes (e.g., 32KB) - Declared extern
const unsigned int BLOCK_COUNT = 2;       // Number of blocks available in the buffer pool
const unsigned int PRINT_COUNT = 20;      // Default number of rows to print
const unsigned int MATRIX_BLOCK_DIM = 32; // Dimension of a square matrix block (e.g., 32x32) - Adjust as needed

extern std::vector<std::string> tokenizedQuery; // Added extern declaration
extern ParsedQuery parsedQuery;                 // Global object to hold parsed query details
extern BufferManager bufferManager;             // Global buffer manager instance
extern TableCatalogue tableCatalogue;           // Global table catalogue instance
extern MatrixCatalogue matrixCatalogue;         // Global matrix catalogue instance
extern Logger logger;                           // Global logger instance

// Function declarations for file checks (definitions in executor.cpp)
bool isFileExists(const std::string &filename);
bool isQueryFile(const std::string &filename);

#endif // GLOBAL_H