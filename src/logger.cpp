#include "logger.h" // Include the header file for the Logger class
#include "global.h"
#include <fstream>
#include <iostream> // Include for endl

using namespace std; // Make std namespace accessible

Logger::Logger()
{
    // Check if the log file was opened successfully
    this->fout.open(this->logFile, ios::out | ios::app); // Use append mode
    if (!this->fout.is_open())
    {
        cerr << "Error opening log file: " << this->logFile << endl;
    }
}

void Logger::log(string logString)
{
    if (this->fout.is_open())
    { // Only write if the file is open
        fout << logString << endl;
    }
    else
    {
        // Optionally log to cerr if file isn't open
        cerr << "Log Error (file not open): " << logString << endl;
    }
}

// Destructor to ensure the file is closed
Logger::~Logger()
{
    if (this->fout.is_open())
    {
        this->fout.close();
    }
}
