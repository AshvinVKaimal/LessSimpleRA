// #ifndef EXECUTOR_H
// #define EXECUTOR_H

#include "semanticParser.h"

void executeCommand();

void executeCLEAR();
void executeCROSS();
void executeDISTINCT();
void executeEXPORT();
void executeINDEX();
void executeJOIN();
void executeLIST();
void executeLOAD();
void executePRINT();
void executePROJECTION();
void executeRENAME();
void executeSELECTION();
void executeSORT();
void executeSOURCE();
void executeORDERBY();
void executeGROUPBY();
void executeSEARCH();
void executeINSERT();
void executeUPDATE();
void executeDELETE();
// MATRICES
void executeLOADMATRIX(); 
void executePRINTMATRIX();
void executeEXPORTMATRIX();
void executeCROSSTRANSPOSEMATRIX();
void executeROTATEMATRIX();
void executeCHECKANTISYMMATRIX();


bool evaluateBinOp(int value1, int value2, BinaryOperator binaryOperator);
void printRowCount(int rowCount);

// #endif // EXECUTOR_H