// Server Code
#include "global.h"
#include "bufferManager.h"   // Include BufferManager definition
#include "logger.h"          // Include Logger definition
#include "tableCatalogue.h"  // Include TableCatalogue definition
#include "matrixCatalogue.h" // Include MatrixCatalogue definition
#include "executor.h"        // Include declaration for executeCommand
#include <iostream>          // Include for cin, cout, endl
#include <string>            // Include for string
#include <vector>            // Include for vector
#include <regex>             // Include for regex, sregex_iterator
#include <cstdlib>           // Include for system()

using namespace std; // Make std namespace accessible

// Define global variables declared as extern in global.h
Logger logger;
BufferManager bufferManager;
TableCatalogue tableCatalogue;
MatrixCatalogue matrixCatalogue; // Assuming MatrixCatalogue is also global

vector<string> tokenizedQuery;
ParsedQuery parsedQuery;

void doCommand()
{
    logger.log("doCommand");
    if (syntacticParse() && semanticParse())
    {
        // printf("Parsed and Semantic\n"); // Debugging print
        executeCommand();
    }
    else
    {
        // Error messages are usually printed by the parsers themselves
        logger.log("doCommand: Parsing failed.");
    }
    return;
}

int main(void)
{

    regex delim("[^\\s,()]+"); // Updated regex to handle parentheses as delimiters as well
    string command;
    // Use system calls cautiously, consider platform compatibility
    // Ensure the paths are correct relative to the executable's CWD
    if (system("rm -rf ../data/temp") != 0)
    {
        cerr << "Warning: Could not remove temp directory." << endl;
    }
    if (system("mkdir -p ../data/temp") != 0)
    { // Use -p to avoid error if exists
        cerr << "Error: Could not create temp directory." << endl;
        return 1; // Exit if essential directory cannot be created
    }

    while (!cin.eof())
    {
        cout << "\n> ";
        tokenizedQuery.clear();
        parsedQuery.clear();
        logger.log("\nReading New Command: ");
        getline(cin, command);

        // Handle EOF after getline
        if (cin.eof())
        {
            break;
        }

        logger.log(command);

        // Handle empty input line
        if (command.empty())
        {
            continue;
        }

        // Tokenize using regex
        auto words_begin = std::sregex_iterator(command.begin(), command.end(), delim);
        auto words_end = std::sregex_iterator();
        for (std::sregex_iterator i = words_begin; i != words_end; ++i)
        {
            // Check for comments (e.g., lines starting with --)
            string token = (*i).str();
            if (token.rfind("--", 0) == 0)
            { // Check if token starts with --
                // If the whole command starts with --, ignore the line
                if (i == words_begin)
                {
                    tokenizedQuery.clear(); // Clear any previous tokens from this line
                    break;                  // Stop processing this line
                }
                // Otherwise, ignore this token and subsequent ones on the line
                break;
            }
            tokenizedQuery.emplace_back(token);
        }

        if (tokenizedQuery.empty()) // Check if tokenization resulted in empty list (e.g., empty line or comment)
        {
            continue;
        }

        if (tokenizedQuery.size() == 1 && tokenizedQuery.front() == "QUIT")
        {
            cout << "Exiting..." << endl;
            logger.log("Exiting.");
            break;
        }

        // Basic syntax check: most commands need at least 2 tokens (Command + Argument)
        // Specific commands like LIST might only need 1, handled by syntactic parser
        // if (tokenizedQuery.size() < 1) // This check is redundant due to the empty check above
        // {
        //     cout << "SYNTAX ERROR: Empty command." << endl;
        //     logger.log("SYNTAX ERROR: Empty command.");
        //     continue;
        // }

        doCommand();
    }

    // Cleanup before exiting if necessary
    // system("rm -rf ../data/temp"); // Optional: clean up temp files on exit

    return 0; // Indicate successful execution
}