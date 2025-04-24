#include "global.h" // Include global definitions like BLOCK_SIZE
#include "matrix.h"
#include <fstream>
#include <sstream>
#include <iostream>

extern int BLOCK_SIZE; // Declare the external block size

Matrix::Matrix(string matrixName)
{
    this->matrixName = matrixName;
    this->dimension = 0;
}

bool Matrix::load()
{
    string filename = "../data/" + this->matrixName + ".csv";
    ifstream infile(filename);
    if (!infile)
    {
        cout << "Error: Unable to open file " << filename << endl;
        return false;
    }

    string line;
    int pageIndex = 0;
    while (getline(infile, line))
    {
        vector<int> row;
        stringstream ss(line);
        string token;
        while (getline(ss, token, ','))
        {
            try
            {
                int value = stoi(token);
                row.push_back(value);
            }
            catch (exception &e)
            {
                cout << "Error: Invalid integer in file " << filename << endl;
                return false;
            }
        }
        this->data.push_back(row);
        this->writePage(pageIndex++, row); // Write each row as a separate page
    }
    infile.close();

    this->dimension = this->data.size();
    for (auto &row : this->data)
    {
        if (row.size() != this->dimension)
        {
            cout << "Error: Matrix is not square in file " << filename << endl;
            return false;
        }
    }

    return true;
}

void Matrix::writePage(int pageIndex, const vector<int> &rowData)
{
    string fileName = "../data/temp/" + this->matrixName + "_Page" + to_string(pageIndex) + ".matrix";
    ofstream fout(fileName, ios::trunc);
    for (size_t i = 0; i < rowData.size(); ++i)
    {
        if (i != 0)
            fout << " ";
        fout << rowData[i];
    }
    fout << endl;
    fout.close();
}

bool Matrix::blockify()
{
    logger.log("Matrix::blockify");
    ifstream fin(this->sourceFileName, ios::in);
    if (!fin)
    {
        logger.log("Matrix::blockify ERROR: Cannot open source file: " + this->sourceFileName);
        return false;
    }

    string line;
    // Assuming the first line contains dimensions (rows, cols) - This might need adjustment based on actual CSV format
    // If dimensions are known beforehand (e.g., from constructor), skip reading them here.
    // For now, let's assume dimensions are already set in the Matrix object.

    if (this->dimension == 0)
    {
        logger.log("Matrix::blockify ERROR: Matrix dimension is zero.");
        fin.close();
        return false;
    }

    // Calculate matrix properties based on dimension
    this->blocksPerRow = (unsigned int)std::ceil((double)this->dimension / MATRIX_BLOCK_DIM);
    this->blockCount = this->blocksPerRow * this->blocksPerRow;
    this->maxCellsPerBlock = MATRIX_BLOCK_DIM * MATRIX_BLOCK_DIM;

    // Prepare buffer for one block
    vector<vector<int>> blockData(MATRIX_BLOCK_DIM, vector<int>(MATRIX_BLOCK_DIM, 0));
    int cellCounter = 0;
    int blockRow = 0, blockCol = 0;
    int rowInBlock = 0, colInBlock = 0;

    // Read the matrix data cell by cell
    for (unsigned int r = 0; r < this->dimension; ++r)
    {
        if (!getline(fin, line))
        {
            logger.log("Matrix::blockify ERROR: Not enough rows in CSV file. Expected " + to_string(this->dimension));
            fin.close();
            return false;
        }
        stringstream ss(line);
        string cellValueStr;
        for (unsigned int c = 0; c < this->dimension; ++c)
        {
            if (!getline(ss, cellValueStr, ','))
            {
                logger.log("Matrix::blockify ERROR: Not enough columns in CSV row " + to_string(r) + ". Expected " + to_string(this->dimension));
                fin.close();
                return false;
            }

            // Determine which block this cell belongs to
            blockRow = r / MATRIX_BLOCK_DIM;
            blockCol = c / MATRIX_BLOCK_DIM;
            rowInBlock = r % MATRIX_BLOCK_DIM;
            colInBlock = c % MATRIX_BLOCK_DIM;

            // Calculate the linear block index
            unsigned int currentBlockIndex = blockRow * this->blocksPerRow + blockCol;

            // If this is the first cell of a new block, write the previous block (if any)
            // This logic assumes reading row-by-row, filling blocks as they complete.
            // A more block-oriented read might be needed if memory is constrained.
            // For simplicity, let's assume we buffer one block at a time.

            // Read cell value
            try
            {
                blockData[rowInBlock][colInBlock] = stoi(cellValueStr);
            }
            catch (const std::invalid_argument &ia)
            {
                logger.log("Matrix::blockify ERROR: Invalid integer value '" + cellValueStr + "' at row " + to_string(r) + ", col " + to_string(c));
                fin.close();
                return false;
            }
            catch (const std::out_of_range &oor)
            {
                logger.log("Matrix::blockify ERROR: Integer value out of range '" + cellValueStr + "' at row " + to_string(r) + ", col " + to_string(c));
                fin.close();
                return false;
            }

            // Check if the current block is full or if we've reached the end of the matrix
            bool endOfMatrix = (r == this->dimension - 1 && c == this->dimension - 1);
            bool endOfBlock = (rowInBlock == MATRIX_BLOCK_DIM - 1 && colInBlock == MATRIX_BLOCK_DIM - 1);

            if (endOfBlock || endOfMatrix)
            {
                // Flatten the 2D block data into a 1D vector for Page constructor
                vector<vector<int>> pageRows; // Treat each row of the block as a "row" for the Page
                pageRows.reserve(MATRIX_BLOCK_DIM);
                for (int i = 0; i < MATRIX_BLOCK_DIM; ++i)
                {
                    // Only add rows up to the actual data read if it's the last partial block
                    // This needs more careful handling for partially filled blocks.
                    // For now, assume full blocks or padding is handled by Page/BufferManager.
                    pageRows.push_back(blockData[i]);
                }

                // Write the block using Page class
                // The "rowCount" for a matrix block page is effectively MATRIX_BLOCK_DIM (number of rows in the block)
                Page matrixPage(this->matrixName, currentBlockIndex, pageRows, MATRIX_BLOCK_DIM);
                matrixPage.writePage();

                // Reset block data buffer for the next block (if not end of matrix)
                if (!endOfMatrix)
                {
                    blockData.assign(MATRIX_BLOCK_DIM, vector<int>(MATRIX_BLOCK_DIM, 0));
                }
            }
        } // End column loop
    } // End row loop

    fin.close();
    logger.log("Matrix::blockify completed successfully for " + this->matrixName);
    return true;
}