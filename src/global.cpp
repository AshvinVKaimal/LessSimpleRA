#include "global.h"
#include "syntacticParser.h" // Include ParsedQuery definition here
#include <vector>
#include <string>

// Define global constants declared as extern in global.h
const unsigned int BLOCK_SIZE = 32768; // Definition for BLOCK_SIZE

// Define global variables declared as extern in global.h
std::vector<std::string> tokenizedQuery;
ParsedQuery parsedQuery; // Requires full definition from syntacticParser.h
BufferManager bufferManager;
TableCatalogue tableCatalogue;
MatrixCatalogue matrixCatalogue;
Logger logger; // Assuming Logger has a constructor defined in logger.cpp
