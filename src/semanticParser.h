#ifndef SEMANTICPARSER_H
#define SEMANTICPARSER_H

#include "syntacticParser.h"

bool semanticParse();

bool semanticParseCLEAR();
bool semanticParseCROSS();
bool semanticParseDISTINCT();
bool semanticParseEXPORT();
bool semanticParseINDEX();
bool semanticParseJOIN();
bool semanticParseLIST();
bool semanticParseLOAD();
bool semanticParsePRINT();
bool semanticParsePROJECTION();
bool semanticParseRENAME();
bool semanticParseSELECTION();
bool semanticParseSORT();
bool semanticParseSOURCE();
bool semanticParseGROUPBY();
bool semanticParseORDERBY();
bool semanticParseSEARCH();
bool semanticParseINSERT();
bool semanticParseUPDATE();
bool semanticParseDELETE();

bool semanticParseLOADMATRIX();
bool semanticParsePRINTMATRIX();
bool semanticParseEXPORTMATRIX();
bool semanticParseCROSSTRANSPOSEMATRIX();
bool semanticParseROTATEMATRIX();
bool semanticParseCHECKANTISYMMATRIX();
// file: semanticParser.h


#endif  // SEMANTICPARSER_H
