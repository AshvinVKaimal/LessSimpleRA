#ifndef LOGGER_H
#define LOGGER_H
#include <iostream>
#include "bits/stdc++.h"
#include <sys/stat.h>
#include <fstream>

using namespace std;

class Logger
{

    string logFile = "log";
    ofstream fout;

public:
    Logger();
    ~Logger(); // Declare the destructor
    void log(string logString);
};

extern Logger logger;

#endif // LOGGER_H