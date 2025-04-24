<!-- [![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=17886365&assignment_repo_type=AssignmentRepo) -->
# LessSimpleRA

This is a modified version of the [SimpleRA](https://github.com/SimpleRA/SimpleRA) project, a simple integer-based Relational Database Management System (RDBMS) that supports basic SQL operations.

## Compilation Instructions

This project is written in C++ and uses the GNU Make build system. To compile the project, you will need to have a C++ compiler and make installed on your system.

```cd``` into the soure directory (```src```)
```
cd src
```
To compile:
```
make clean
make
```

## Running the System

Post compilation, an executable names ```server``` will be created in the ```src``` directory
```
./server
```

## Features Added

- ```SOURCE``` command - Takes in a '.ra' file script and executes the commands in it

- Added support for square matrices and matrix operations
- ```LOAD_MATRIX``` command - Loads a matrix from a CSV file
- ```PRINT_MATRIX``` command - Prints a matrix to the console
- ```EXPORT_MATRIX``` command - Exports a matrix to a CSV file
- ```ROTATE``` command - Rotates a matrix by 90 degrees
- ```CROSSTRANSPOSE``` command - Transposes a matrix and then flips it over its diagonal
- ```CHECKANTISYM``` command - Checks if a matrix is antisymmetric

- ```SORT``` command - Sorts a table based on specific columns
- ```ORDER BY``` command - Makes a new table based on sorted data of an existing table
- ```GROUP BY``` command - Groups rows based on a specified condition
- ```JOIN``` command - Joins two tables based on equality of specified columns

- Implemented an indexing structure for faster data retrieval
- ```SEARCH``` command - Searches for rows in a table based on a specific condition
- ```INSERT``` command - Inserts a new row into a table
- ```UPDATE``` command - Updates existing rows in a table
- ```DELETE``` command - Deletes rows from a table based on a specific condition