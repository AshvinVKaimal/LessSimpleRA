#include "indexing.h" // Should be first among project headers
#include "global.h"   // For logger, bufferManager potentially
#include "logger.h"
// #include "tableCatalogue.h" // Removed - No longer needed here
// #include "table.h"          // Removed - No longer needed here
#include <vector>
#include <algorithm> // For std::lower_bound, std::upper_bound, std::copy, std::sort
#include <stdexcept>
#include <cmath>   // For std::ceil
#include <cstring> // For potential future raw buffer memcpy
#include <limits>  // For numeric_limits

// --- Helper Functions Implementation ---

// Generate index page name
std::string getIndexPageName(const std::string &tableName, const std::string &columnName, unsigned int pageId)
{
    // Using "_idx_" infix ensures these names don't collide with standard table data pages ("../data/temp/TableName_PageX")
    return "../data/temp/" + tableName + "_" + columnName + "_idx_Page" + std::to_string(pageId);
}

void serializeNode(BPTreeNode *node, std::vector<int> &pageData)
{
    pageData.clear(); // Ensure we start with an empty vector

    // --- Metadata ---
    // 1. isLeaf flag (1 for true, 0 for false)
    pageData.push_back(node->isLeaf ? 1 : 0);
    // 2. keyCount (number of keys currently stored)
    pageData.push_back(static_cast<int>(node->keyCount));

    // --- Node Specific Data ---
    if (node->isLeaf)
    {
        // --- Leaf Node Serialization ---
        BPTreeLeafNode *leaf = static_cast<BPTreeLeafNode *>(node);

        // 3. nextPageId (Page ID of the next leaf node in the sequence)
        pageData.push_back(static_cast<int>(leaf->nextPageId)); // Cast required

        // 4. Key-Pointer Pairs (key1, pageId1, rowIndex1, key2, pageId2, rowIndex2, ...)
        if (leaf->keys.size() != node->keyCount || leaf->pointers.size() != node->keyCount)
        {
            logger.log("Serialization Error: Leaf node key/pointer count mismatch for page " + std::to_string(node->pageId));
            // Decide on error handling: throw, log and continue with potentially corrupt data, etc.
            // For now, we'll proceed using keyCount, but this indicates an internal inconsistency.
        }
        for (size_t i = 0; i < node->keyCount; ++i)
        {
            pageData.push_back(leaf->keys[i]);                                // The key value
            pageData.push_back(static_cast<int>(leaf->pointers[i].pageId));   // RecordPointer's pageId (cast)
            pageData.push_back(static_cast<int>(leaf->pointers[i].rowIndex)); // RecordPointer's rowIndex (cast)
        }
    }
    else
    {
        // --- Internal Node Serialization ---
        BPTreeInternalNode *internal = static_cast<BPTreeInternalNode *>(node);

        // Sanity check: Internal node must have keyCount + 1 children pointers.
        if (internal->childrenPageIds.size() != node->keyCount + 1)
        {
            logger.log("Serialization Error: Internal node children pointer count mismatch for page " + std::to_string(node->pageId) + ". Expected: " + std::to_string(node->keyCount + 1) + ", Found: " + std::to_string(internal->childrenPageIds.size()));
            // Handle error as above. Proceeding might lead to corrupt index structure.
        }
        if (internal->keys.size() != node->keyCount)
        {
            logger.log("Serialization Error: Internal node key count mismatch for page " + std::to_string(node->pageId));
            // Handle error.
        }

        // 3. First Child Pointer (Pointer to subtree for keys < keys[0])
        // Ensure vector is not empty before accessing index 0, although count check should cover this.
        if (!internal->childrenPageIds.empty())
        {
            pageData.push_back(static_cast<int>(internal->childrenPageIds[0])); // Cast required
        }
        else if (node->keyCount == 0)
        { // Special case: An internal node might briefly have 0 keys and 1 child during root adjustments?
            // Or this indicates an error state. If childrenPageIds is truly empty, we can't serialize it.
            logger.log("Serialization Warning: Internal node " + std::to_string(node->pageId) + " has 0 keys and empty children vector.");
            pageData.push_back(0); // Serialize a placeholder? Needs careful thought based on tree logic. Or throw.
        }
        else
        {
            logger.log("Serialization Error: Internal node " + std::to_string(node->pageId) + " children vector empty despite keyCount > 0.");
            throw std::runtime_error("Internal node state inconsistent during serialization.");
        }

        // 4. Key-ChildPointer Pairs (key1, childPtr1, key2, childPtr2, ...)
        for (size_t i = 0; i < node->keyCount; ++i)
        {
            pageData.push_back(internal->keys[i]); // The key value
            // Check bounds before accessing childrenPageIds[i + 1]
            if (i + 1 < internal->childrenPageIds.size())
            {
                pageData.push_back(static_cast<int>(internal->childrenPageIds[i + 1])); // Child pointer corresponding to keys >= keys[i] (cast)
            }
            else
            {
                logger.log("Serialization Error: Internal node " + std::to_string(node->pageId) + " missing child pointer for key index " + std::to_string(i));
                throw std::runtime_error("Internal node state inconsistent during serialization (missing child pointer).");
            }
        }
    }

    // --- Optional Padding ---
    // To ensure the serialized data always fills a block, you might pad it here.
    size_t intsPerPage = PAGE_SIZE_BYTES / SIZEOF_INT; // Calculate how many ints fit
    if (pageData.size() < intsPerPage)
    {
        pageData.resize(intsPerPage, 0); // Pad with zeros (or a specific marker)
    }
    else if (pageData.size() > intsPerPage)
    {
        logger.log("Serialization Error: Node data exceeds page size for page " + std::to_string(node->pageId));
        // This indicates a fundamental problem with FANOUT/LEAF_CAPACITY calculation or node logic.
        throw std::runtime_error("Node data exceeds page size during serialization.");
    }
}

BPTreeNode *deserializeNode(const std::vector<int> &pageData, unsigned int pageId, BPTree *tree)
{
    // --- Basic Validation ---
    if (pageData.size() < 2)
    { // Need at least isLeaf and keyCount fields
        logger.log("Deserialize Error: Page data vector too small (size=" + std::to_string(pageData.size()) + ") for node " + std::to_string(pageId));
        throw std::runtime_error("Cannot deserialize node " + std::to_string(pageId) + " from insufficient data.");
    }

    // --- Read Metadata ---
    bool isLeaf = (pageData[0] == 1);
    // Basic check for keyCount validity (e.g., non-negative)
    if (pageData[1] < 0)
    {
        logger.log("Deserialize Error: Invalid keyCount (" + std::to_string(pageData[1]) + ") found for node " + std::to_string(pageId));
        throw std::runtime_error("Invalid keyCount during deserialization for node " + std::to_string(pageId));
    }
    unsigned int keyCount = static_cast<unsigned int>(pageData[1]);

    BPTreeNode *node = nullptr;
    size_t currentPos = 2; // Start reading data after the metadata fields

    try
    {
        if (isLeaf)
        {
            // --- Leaf Node Deserialization ---
            BPTreeLeafNode *leaf = new BPTreeLeafNode(pageId, tree); // Allocate derived type
            node = leaf;                                             // Assign to base pointer for unified error handling if needed

            leaf->keyCount = keyCount;

            // Read nextPageId (requires at least 3 elements total)
            if (pageData.size() < currentPos + 1)
                throw std::out_of_range("Insufficient data for nextPageId");
            leaf->nextPageId = static_cast<unsigned int>(pageData[currentPos++]); // Cast back

            // Allocate space for keys and pointers
            leaf->keys.resize(keyCount);
            leaf->pointers.resize(keyCount);

            // Read Key-Pointer pairs (each pair requires 3 ints: key, pageId, rowIndex)
            size_t expectedSize = currentPos + keyCount * 3;
            if (pageData.size() < expectedSize)
                throw std::out_of_range("Insufficient data for leaf node key-pointer pairs");

            for (size_t i = 0; i < keyCount; ++i)
            {
                leaf->keys[i] = pageData[currentPos++];                                         // Key
                leaf->pointers[i].pageId = static_cast<unsigned int>(pageData[currentPos++]);   // Pointer pageId (cast back)
                leaf->pointers[i].rowIndex = static_cast<unsigned int>(pageData[currentPos++]); // Pointer rowIndex (cast back)
            }
        }
        else
        {
            // --- Internal Node Deserialization ---
            BPTreeInternalNode *internal = new BPTreeInternalNode(pageId, tree); // Allocate derived type
            node = internal;                                                     // Assign to base pointer

            internal->keyCount = keyCount;

            // Internal node has keyCount keys and keyCount + 1 children pointers.
            size_t numChildren = keyCount + 1;
            internal->childrenPageIds.resize(numChildren);
            internal->keys.resize(keyCount);

            // Read first child pointer (requires at least 3 elements total)
            if (pageData.size() < currentPos + 1)
                throw std::out_of_range("Insufficient data for first child pointer");
            internal->childrenPageIds[0] = static_cast<unsigned int>(pageData[currentPos++]); // Cast back

            // Read Key-ChildPointer pairs (each pair requires 2 ints: key, childPtr)
            size_t expectedSize = currentPos + keyCount * 2;
            if (pageData.size() < expectedSize)
                throw std::out_of_range("Insufficient data for internal node key-child pairs");

            for (size_t i = 0; i < keyCount; ++i)
            {
                internal->keys[i] = pageData[currentPos++];                                           // Key
                internal->childrenPageIds[i + 1] = static_cast<unsigned int>(pageData[currentPos++]); // Child pointer (cast back)
            }
        }
    }
    catch (const std::bad_alloc &ba)
    {
        logger.log("Deserialize Error: Memory allocation failed for node " + std::to_string(pageId));
        // No need to delete node pointer here as allocation failed.
        throw std::runtime_error("Memory allocation failed during deserialization for node " + std::to_string(pageId));
    }
    catch (const std::out_of_range &oor)
    {
        logger.log("Deserialize Error: Out of range access during node deserialization for page " + std::to_string(pageId) + ". Details: " + oor.what());
        delete node; // Clean up partially created node if allocation succeeded before error
        throw std::runtime_error("Deserialization failed for node " + std::to_string(pageId) + " due to data bounds.");
    }
    catch (const std::exception &e)
    {
        logger.log("Deserialize Error: Generic exception during node deserialization for page " + std::to_string(pageId) + ". Details: " + e.what());
        delete node;
        throw std::runtime_error("Generic deserialization error for node " + std::to_string(pageId));
    }
    catch (...)
    {
        // Catch any other potential exceptions
        logger.log("Deserialize Error: Unknown error during node deserialization for page " + std::to_string(pageId));
        delete node;
        throw std::runtime_error("Unknown deserialization error for node " + std::to_string(pageId));
    }

    // logger.log("Deserialize Success: Node " + std::to_string(pageId) + " deserialized. Type: " + (isLeaf ? "Leaf" : "Internal") + ", KeyCount: " + std::to_string(keyCount));
    return node;
}

// --- BPTreeLeafNode Implementation ---

BPTreeLeafNode::BPTreeLeafNode(unsigned int id, BPTree *treePtr)
    : BPTreeNode(id, treePtr, true), // Call base constructor, setting isLeaf to true
      nextPageId(0)                  // Initialize the next page pointer to 0 (end of list marker)
{
}

void BPTreeLeafNode::writeNode()
{
    // Optional: Log write operation for debugging
    // logger.log("BPTreeLeafNode::writeNode attempting to write pageId=" + std::to_string(pageId));

    // 1. Serialize the node data into the pageData vector.
    //    This function should handle the internal format (metadata, keys, pointers, nextPtr)
    //    AND crucially, perform padding if uncommented in its implementation.
    std::vector<int> pageData;
    serializeNode(this, pageData); // 'this' is the BPTreeLeafNode*

    // 2. Determine the correct filename for this index page.
    std::string pageName = getIndexPageName(tree->getTableName(), tree->getColumnName(), pageId);

    // 3. Use the BufferManager to write the serialized data.
    //    HACK/ASSUMPTION: Using writePage. A more robust BufferManager might have
    //    a dedicated function like writeIndexPage(pageName, pageDataBuffer).
    //    Here, we wrap the single vector<int> representing the page content
    //    into a vector<vector<int>> and specify rowCount as 1, treating the
    //    entire page as a single entity for the write operation.
    try
    {
        // logger.log("BPTreeLeafNode::writeNode calling bufferManager->writeIndexPage for " + pageName);
        BufferManager *bufMgr = tree->getBufferManager();
        if (!bufMgr)
        {
            logger.log("BPTreeLeafNode::writeNode ERROR: Buffer Manager pointer is null for page " + std::to_string(pageId));
            throw std::runtime_error("Buffer Manager is null during writeNode.");
        }
        // Use the new method designed for index pages
        bufMgr->writeIndexPage(pageName, pageData);
        // logger.log("BPTreeLeafNode::writeNode successfully wrote pageId=" + std::to_string(pageId));
    }
    catch (const std::exception &e)
    {
        logger.log("BPTreeLeafNode::writeNode ERROR: Exception during bufferManager->writePage for page " + std::to_string(pageId) + ". Details: " + e.what());
        // Depending on requirements, you might re-throw or handle the error.
        throw; // Re-throw for now to signal failure
    }
    catch (...)
    {
        logger.log("BPTreeLeafNode::writeNode ERROR: Unknown exception during bufferManager->writePage for page " + std::to_string(pageId));
        throw; // Re-throw unknown errors
    }
}

/**
 * @brief Finds the index of the first key in the node's sorted keys vector
 * that is greater than or equal to the given search key.
 * This utilizes binary search (`std::lower_bound`) for efficiency.
 * The returned index indicates where the key *should* be inserted to maintain order,
 * or the starting position for searching for existing entries equal to the key.
 *
 * @param key The key value to search for or find the insertion position of.
 * @return int The 0-based index within the keys vector. If the key is larger than
 *         all existing keys, it returns keyCount.
 */
int BPTreeLeafNode::findFirstKeyIndex(int key)
{
    // std::lower_bound requires iterators. We operate on the valid portion of the keys vector.
    // It finds the first iterator 'it' in the range [first, last) such that *it is NOT LESS than key.
    // If all elements are less than key, it returns 'last'.
    auto it = std::lower_bound(keys.begin(),            // Start of the range
                               keys.begin() + keyCount, // End of the *valid* data range (one past the last element)
                               key);                    // The value to compare against

    // Calculate the index by finding the distance from the beginning of the vector.
    return static_cast<int>(std::distance(keys.begin(), it));
}

int BPTreeLeafNode::insert(int key, RecordPointer pointer)
{
    // Optional: Log entry for debugging
    // logger.log("BPTreeLeafNode::insert attempting key=" + std::to_string(key) + " into pageId=" + std::to_string(pageId));

    // 1. Find the correct insertion index using binary search (lower_bound).
    int index = findFirstKeyIndex(key);

    // 2. Optional but recommended: Check for exact duplicate key-pointer pair.
    // This prevents the index from having multiple entries for the *exact* same
    // key pointing to the *exact* same row location.
    for (size_t i = index; i < keyCount && keys[i] == key; ++i)
    {
        if (pointers[i] == pointer)
        { // Uses RecordPointer::operator==
            // logger.log("BPTreeLeafNode::insert - Duplicate key-pointer pair ignored for key=" + std::to_string(key) + " pageId=" + std::to_string(pageId));
            return 0; // Indicate success, no change was made or needed.
        }
    }

    // 3. Insert the new key and pointer into the vectors at the found index.
    //    `vector::insert` shifts existing elements automatically.
    try
    {
        keys.insert(keys.begin() + index, key);
        pointers.insert(pointers.begin() + index, pointer);
        keyCount++; // Increment the count of valid keys
    }
    catch (const std::bad_alloc &ba)
    {
        logger.log("BPTreeLeafNode::insert ERROR: Memory allocation failed during vector insert for page " + std::to_string(pageId));
        throw; // Re-throw memory allocation errors
    }
    catch (...)
    {
        logger.log("BPTreeLeafNode::insert ERROR: Unknown exception during vector insert for page " + std::to_string(pageId));
        throw; // Re-throw other potential errors
    }

    // 4. Check if the node has overflowed its capacity.
    if (keyCount > tree->getLeafCapacity())
    {
        // --- Handle Node Split ---
        // logger.log("BPTreeLeafNode::insert - Overflow detected (keyCount=" + std::to_string(keyCount) + " > capacity=" + std::to_string(tree->getLeafCapacity()) + "), splitting node " + std::to_string(pageId));

        // a. Allocate a page ID for the new sibling node.
        unsigned int newSiblingPageId = tree->getNewPageId(); // Assumes this returns a valid, unique ID

        // b. Create the new sibling leaf node object in memory.
        BPTreeLeafNode *newNode = nullptr;
        try
        {
            newNode = new BPTreeLeafNode(newSiblingPageId, tree);
        }
        catch (const std::bad_alloc &ba)
        {
            logger.log("BPTreeLeafNode::insert ERROR: Memory allocation failed for new sibling node during split of page " + std::to_string(pageId));
            // Critical error, index might be inconsistent if we can't create the sibling.
            // Rollback might be complex here. For now, re-throw.
            throw;
        }

        // c. Determine the split point. Typically, move ceil(N/2) elements to the new node,
        //    where N is the current (overflowing) keyCount. The original node keeps floor(N/2).
        int splitIndex = static_cast<int>(std::ceil((double)keyCount / 2.0));
        int elementsToMove = keyCount - splitIndex;

        // d. Copy the second half of the keys and pointers to the new sibling node.
        //    `vector::assign` replaces the content of the target vector.
        try
        {
            newNode->keys.assign(keys.begin() + splitIndex, keys.end());
            newNode->pointers.assign(pointers.begin() + splitIndex, pointers.end());
            newNode->keyCount = static_cast<unsigned int>(elementsToMove); // Update new node's count
        }
        catch (const std::bad_alloc &ba)
        {
            logger.log("BPTreeLeafNode::insert ERROR: Memory allocation failed during vector assign in split for page " + std::to_string(pageId));
            delete newNode; // Clean up allocated sibling
            throw;          // Re-throw
        }

        // e. Truncate the original node (this node) to keep only the first half.
        //    `vector::resize` adjusts the size, discarding elements beyond the new size.
        try
        {
            keys.resize(splitIndex);
            pointers.resize(splitIndex);
            keyCount = static_cast<unsigned int>(splitIndex); // Update this node's count
        }
        catch (const std::bad_alloc &ba)
        {
            logger.log("BPTreeLeafNode::insert ERROR: Memory allocation failed during vector resize in split for page " + std::to_string(pageId));
            // Potential inconsistency. newNode exists but this node wasn't fully updated.
            // This state is hard to recover from easily. Consider throwing.
            delete newNode; // Clean up allocated sibling
            throw;
        }

        // f. Update the linked list pointers between leaf nodes.
        newNode->nextPageId = this->nextPageId; // New node points to where original node pointed.
        this->nextPageId = newNode->pageId;     // Original node now points to the new node.

        // g. Write both modified nodes back to persistent storage.
        try
        {
            this->writeNode();
            newNode->writeNode();
            // logger.log("BPTreeLeafNode::insert - Split complete. OldNode=" + std::to_string(pageId) + " (" + std::to_string(this->keyCount) + " keys), NewNode=" + std::to_string(newNode->pageId) + " (" + std::to_string(newNode->keyCount) + " keys).");
        }
        catch (const std::exception &e)
        {
            logger.log("BPTreeLeafNode::insert ERROR: Exception during writeNode after split for page " + std::to_string(pageId) + " or " + std::to_string(newNode->pageId) + ". Details: " + e.what());
            // Index might be inconsistent on disk.
            delete newNode; // Clean up sibling object
            throw;          // Signal the error upwards
        }

        // h. Prepare information for the parent internal node. The key to be inserted
        //    into the parent is the *first key* of the new sibling node.
        //    *** REMOVED storing split info via tree->setLastSplitInfo ***
        //    The parent's insert logic will need redesign to get this info.
        //    For now, just signal the split occurred.
        if (newNode->keyCount == 0)
        {
            // This case should not happen with ceil split logic.
            logger.log("BPTreeLeafNode::insert WARNING: New sibling node " + std::to_string(newNode->pageId) + " is empty after split!");
            delete newNode; // Clean up sibling object
            throw std::runtime_error("New sibling node empty after split, logic error.");
        }
        // int keyToPropagate = newNode->keys[0]; // Key is needed by parent, but mechanism removed.

        // i. Clean up the temporary in-memory object for the new node.
        delete newNode;

        // j. Return 1 to signal split occurred.
        return 1;
    }
    else
    {
        // --- No Overflow ---
        // The node has capacity, just write the updated node back.
        try
        {
            writeNode();
            // logger.log("BPTreeLeafNode::insert - Insert successful, no split for key=" + std::to_string(key) + " pageId=" + std::to_string(pageId));
            return 0; // Indicate success, no split occurred.
        }
        catch (const std::exception &e)
        {
            logger.log("BPTreeLeafNode::insert ERROR: Exception during writeNode after simple insert for page " + std::to_string(pageId) + ". Details: " + e.what());
            // If write fails, the in-memory state is ahead of disk state.
            throw; // Signal the error upwards
        }
    }
}

/**
 * @brief Searches for a specific key within this leaf node.
 * If the key is found, all RecordPointers associated with that key in *this node*
 * are added to the result vector. It does not traverse to other nodes.
 *
 * @param key The integer key value to search for.
 * @param result Output parameter; a vector where matching RecordPointer objects are appended.
 * @return bool True if at least one instance of the key was found in this node, false otherwise.
 */
bool BPTreeLeafNode::search(int key, std::vector<RecordPointer> &result)
{
    // Optional: Log entry for debugging
    // logger.log("BPTreeLeafNode::search seeking key=" + std::to_string(key) + " in pageId=" + std::to_string(pageId));

    // 1. Find the index of the first key >= search key using binary search.
    int index = findFirstKeyIndex(key);

    // 2. Iterate forward from that index as long as the key matches the search key.
    bool found = false;
    while (index < keyCount && keys[index] == key)
    {
        // 3. Add the corresponding RecordPointer to the result vector.
        try
        {
            result.push_back(pointers[index]);
            found = true; // Mark that we found at least one match
            index++;      // Move to the next potential match
        }
        catch (const std::bad_alloc &ba)
        {
            logger.log("BPTreeLeafNode::search ERROR: Memory allocation failed while adding to result vector for page " + std::to_string(pageId));
            // Stop searching in this node, let caller handle potential partial results?
            // Or clear results and re-throw? For now, just stop adding.
            return found; // Return whether anything was found *before* the error
        }
        catch (...)
        {
            logger.log("BPTreeLeafNode::search ERROR: Unknown exception while adding to result vector for page " + std::to_string(pageId));
            return found; // Return based on finds before error
        }
    }

    // 4. Return whether any matches were found in this node.
    // if (found) {
    //     logger.log("BPTreeLeafNode::search found key=" + std::to_string(key) + " in pageId=" + std::to_string(pageId));
    // } else {
    //     logger.log("BPTreeLeafNode::search key=" + std::to_string(key) + " not found in pageId=" + std::to_string(pageId));
    // }
    return found;
}

/**
 * @brief Searches for all keys within the inclusive range [low, high] starting
 * from this leaf node and traversing subsequent leaf nodes via the nextPageId pointers.
 * Adds the RecordPointers associated with keys within the range to the result vector.
 *
 * @param low The lower bound of the key range (inclusive).
 * @param high The upper bound of the key range (inclusive).
 * @param result Output parameter; a vector where matching RecordPointer objects are appended.
 */
void BPTreeLeafNode::searchRange(int low, int high, std::vector<RecordPointer> &result)
{
    // Optional: Log entry for debugging
    // logger.log("BPTreeLeafNode::searchRange searching [" + std::to_string(low) + "," + std::to_string(high) + "] starting from pageId=" + std::to_string(pageId));

    BPTreeLeafNode *currentNode = this; // Start scan from the current node
    bool firstNodeProcessed = true;     // Flag to track if we are still on the initial 'this' node

    while (currentNode != nullptr)
    {
        // logger.log("BPTreeLeafNode::searchRange processing pageId=" + std::to_string(currentNode->pageId));

        // 1. Iterate through keys in the current node.
        for (size_t i = 0; i < currentNode->keyCount; ++i)
        {
            int currentKey = currentNode->keys[i];

            // 2. Check if the key falls within the desired range.
            if (currentKey >= low && currentKey <= high)
            {
                // 3. If in range, add the corresponding pointer to the results.
                try
                {
                    result.push_back(currentNode->pointers[i]);
                }
                catch (const std::bad_alloc &ba)
                {
                    logger.log("BPTreeLeafNode::searchRange ERROR: Memory allocation failed adding to result vector on page " + std::to_string(currentNode->pageId));
                    // Stop processing further results for this range scan on memory error.
                    // Clean up any fetched node before returning.
                    if (!firstNodeProcessed)
                    {
                        delete currentNode;
                    }
                    return; // Exit the function
                }
                catch (...)
                {
                    logger.log("BPTreeLeafNode::searchRange ERROR: Unknown exception adding to result vector on page " + std::to_string(currentNode->pageId));
                    if (!firstNodeProcessed)
                    {
                        delete currentNode;
                    }
                    return; // Exit on other errors
                }
            }

            // 4. Optimization: If the current key exceeds the upper bound 'high',
            //    no further keys in this node or subsequent nodes can be in the range.
            if (currentKey > high)
            {
                // logger.log("BPTreeLeafNode::searchRange stopping early on page " + std::to_string(currentNode->pageId) + " as key " + std::to_string(currentKey) + " > " + std::to_string(high));
                // Clean up the dynamically allocated node if it wasn't the initial 'this' node.
                if (!firstNodeProcessed)
                {
                    delete currentNode;
                }
                return; // Exit the searchRange function entirely.
            }
        } // End for loop iterating through keys in currentNode

        // 5. Prepare to move to the next leaf node in the linked list.
        unsigned int nextNodeId = currentNode->nextPageId;

        // 6. Clean up the memory for the *current* node if it was dynamically fetched
        //    (i.e., if it wasn't the initial 'this' node we started with).
        if (!firstNodeProcessed)
        {
            delete currentNode;
            currentNode = nullptr; // Avoid dangling pointer
        }
        else
        {
            // We've finished processing the initial node, subsequent nodes will be fetched.
            firstNodeProcessed = false;
        }

        // 7. Check if we reached the end of the leaf node list.
        if (nextNodeId == 0)
        {
            // logger.log("BPTreeLeafNode::searchRange reached end of leaf list (nextPageId is 0).");
            break; // Exit the while loop.
        }

        // 8. Fetch the next leaf node from the BufferManager.
        try
        {
            // logger.log("BPTreeLeafNode::searchRange fetching next leaf pageId=" + std::to_string(nextNodeId));
            BPTreeNode *nextNodeBase = tree->fetchNode(nextNodeId); // Fetch the node

            // Validate the fetched node
            if (nextNodeBase && nextNodeBase->isLeaf)
            {
                // Cast to the correct type and continue the loop
                currentNode = static_cast<BPTreeLeafNode *>(nextNodeBase);
            }
            else
            {
                // Handle error: Fetch failed or the fetched node wasn't a leaf (indicates index corruption).
                logger.log("BPTreeLeafNode::searchRange ERROR: Failed to fetch next node, or fetched node is not leaf. nextId=" + std::to_string(nextNodeId));
                if (nextNodeBase)
                { // Clean up if fetch worked but it wasn't a leaf
                    delete nextNodeBase;
                }
                currentNode = nullptr; // Ensure loop terminates
                break;                 // Exit the while loop on error.
            }
        }
        catch (const std::exception &e)
        {
            logger.log("BPTreeLeafNode::searchRange ERROR: Exception fetching next node " + std::to_string(nextNodeId) + ". Details: " + e.what());
            currentNode = nullptr; // Ensure loop terminates
            break;                 // Exit while loop
        }
        catch (...)
        {
            logger.log("BPTreeLeafNode::searchRange ERROR: Unknown exception fetching next node " + std::to_string(nextNodeId));
            currentNode = nullptr; // Ensure loop terminates
            break;                 // Exit while loop
        }

    } // End while (currentNode != nullptr)

    // Final cleanup: If the loop terminated while currentNode held a dynamically fetched node
    // (e.g., due to an error during fetch of the *next* node), clean it up.
    // This condition is already handled inside the loop now.
    // if (currentNode != nullptr && !firstNodeProcessed) {
    //    delete currentNode;
    // }
}

/**
 * @brief Removes a specific key-pointer pair from this leaf node.
 * Finds the exact match for both the key and the RecordPointer value.
 * If found and removed, it checks if the node underflows (falls below the
 * minimum required number of keys). If underflow occurs (and this node is not
 * the root), it signals this by returning 1, indicating that the parent node
 * needs to handle potential merging or redistribution.
 *
 * **NOTE:** This implementation currently **detects underflow but does not fix it**
 * via merging or redistribution. That logic remains a TODO.
 *
 * @param key The key value to remove.
 * @param pointer The specific RecordPointer associated with the key to remove.
 * @return int Status code:
 *         -  0: Success, key-pointer found and removed, no underflow occurred (or node is root).
 *         -  1: Success, key-pointer found and removed, but node **underflowed** (requires parent action).
 *         - -1: Key-pointer pair not found in this node.
 */
int BPTreeLeafNode::remove(int key, const RecordPointer &pointer)
{
    // Optional: Log entry for debugging
    // logger.log("BPTreeLeafNode::remove attempting key=" + std::to_string(key) + " ptr=(" + std::to_string(pointer.pageId) + "," + std::to_string(pointer.rowIndex) + ") from pageId=" + std::to_string(pageId));

    // 1. Find the potential starting index for the key using binary search.
    int index = findFirstKeyIndex(key);
    bool found = false;

    // 2. Iterate through entries matching the key to find the exact key-pointer pair.
    for (size_t i = index; i < keyCount && keys[i] == key; ++i)
    {
        if (pointers[i] == pointer)
        { // Uses RecordPointer::operator==
            // 3. Erase the matching key and pointer from the vectors.
            try
            {
                keys.erase(keys.begin() + i);
                pointers.erase(pointers.begin() + i);
                keyCount--; // Decrement the count
                found = true;
            }
            catch (const std::exception &e)
            { // Catch potential errors from vector::erase
                logger.log("BPTreeLeafNode::remove ERROR: Exception during vector erase for key=" + std::to_string(key) + " pageId=" + std::to_string(pageId) + ". Details: " + e.what());
                // Index might be inconsistent. Can't proceed safely.
                throw; // Re-throw error
            }
            // logger.log("BPTreeLeafNode::remove - Found and removed key=" + std::to_string(key) + " ptr=(" + std::to_string(pointer.pageId) + "," + std::to_string(pointer.rowIndex) + ")");
            break; // Exit the loop once the specific pair is found and removed.
        }
    }

    // 4. If the specific key-pointer pair was not found.
    if (!found)
    {
        // logger.log("BPTreeLeafNode::remove - Key-pointer pair not found for key=" + std::to_string(key) + " pageId=" + std::to_string(pageId));
        return -1; // Indicate "not found".
    }

    // 5. Write the modified node back to disk/buffer.
    try
    {
        writeNode();
    }
    catch (const std::exception &e)
    {
        logger.log("BPTreeLeafNode::remove ERROR: Exception during writeNode after removal for page " + std::to_string(pageId) + ". Details: " + e.what());
        // If write fails, the in-memory state is ahead of disk state. Index is inconsistent.
        // Reverting the erase might be complex. Re-throw is often the only safe option.
        throw; // Signal the error upwards.
    }

    // 6. Check for underflow condition.
    // Underflow occurs if keyCount < ceil(max_capacity / 2.0).
    // The root node is allowed to underflow (even become empty).
    int minKeys = static_cast<int>(std::ceil((double)tree->getLeafCapacity() / 2.0));
    bool isRoot = (tree->getRootPageId() == this->pageId);

    if (!isRoot && keyCount < minKeys)
    {
        logger.log("BPTreeLeafNode::remove - Underflow detected in non-root node " + std::to_string(pageId) + " (keyCount=" + std::to_string(keyCount) + " < minKeys=" + std::to_string(minKeys) + "). Requires merge/redistribute.");

        // ********************************************************************
        // TODO: IMPLEMENT MERGE / REDISTRIBUTE LOGIC HERE
        // This involves:
        // 1. Fetching the parent node to find siblings.
        // 2. Fetching a sibling node (prefer left, then right, or vice-versa).
        // 3. Checking if sibling has enough keys to lend one (redistribution).
        //    - If yes: Move appropriate key/pointer from sibling, update separating key in parent, write all modified nodes back. Return 0.
        // 4. Checking if nodes can be merged (combined key count <= max capacity).
        //    - If yes: Move all keys/pointers from one node to the other, update linked list pointer (nextPageId), signal parent to remove separating key and pointer to the deleted node, deallocate the empty node's page. Write back modified nodes. Return 1 (signals parent change).
        // Note: This logic is intricate and needs careful handling of edge cases and parent updates.
        // ********************************************************************

        // Placeholder: Return 1 to signal underflow that needs parent handling.
        return 1;
    }

    // 7. If no underflow, or if it's the root node, return 0 for success.
    return 0;
}

// --- BPTreeInternalNode Method Implementations ---

/**
 * @brief Constructor for BPTreeInternalNode.
 * Initializes the base BPTreeNode with leaf status set to false.
 * The keys and childrenPageIds vectors are default-initialized by std::vector.
 * @param id The unique Page ID for this node.
 * @param treePtr Pointer to the owning BPTree object.
 */
BPTreeInternalNode::BPTreeInternalNode(unsigned int id, BPTree *treePtr)
    : BPTreeNode(id, treePtr, false) // Call base constructor, setting isLeaf to false
{
    // Optional: Log node creation for debugging
    // logger.log("BPTreeInternalNode::BPTreeInternalNode created pageId=" + std::to_string(id));
    // Vectors keys and childrenPageIds are ready for use.
}

/**
 * @brief Writes the current state of this internal node back to its corresponding page
 * using the BufferManager provided by the parent BPTree.
 * It first serializes the node's data (metadata, keys, child pointers) into a
 * vector<int> format (including padding if enabled) and then requests the BufferManager
 * to write this data vector to the appropriate page file.
 *
 * NOTE: This relies heavily on the assumptions about the BufferManager handling
 * index pages via getIndexPageName and the specific writePage signature.
 */
void BPTreeInternalNode::writeNode()
{
    // Optional: Log write operation for debugging
    // logger.log("BPTreeInternalNode::writeNode attempting to write pageId=" + std::to_string(pageId));

    // 1. Serialize the node data. Handles metadata, keys, and child pointers.
    //    Also handles padding if serializeNode is configured to do so.
    std::vector<int> pageData;
    serializeNode(this, pageData); // 'this' is the BPTreeInternalNode*

    // 2. Get the unique filename for this index page.
    std::string pageName = getIndexPageName(tree->getTableName(), tree->getColumnName(), pageId);

    // 3. Use the BufferManager to write the serialized data.
    //    HACK/ASSUMPTION: Using writePage similar to the leaf node.
    try
    {
        // logger.log("BPTreeInternalNode::writeNode calling bufferManager->writeIndexPage for " + pageName);
        BufferManager *bufMgr = tree->getBufferManager();
        if (!bufMgr)
        {
            logger.log("BPTreeInternalNode::writeNode ERROR: Buffer Manager pointer is null for page " + std::to_string(pageId));
            throw std::runtime_error("Buffer Manager is null during writeNode.");
        }
        // Use the new method designed for index pages
        bufMgr->writeIndexPage(pageName, pageData);
        // logger.log("BPTreeInternalNode::writeNode successfully wrote pageId=" + std::to_string(pageId));
    }
    catch (const std::exception &e)
    {
        logger.log("BPTreeInternalNode::writeNode ERROR: Exception during bufferManager->writePage for page " + std::to_string(pageId) + ". Details: " + e.what());
        throw; // Re-throw to signal failure
    }
    catch (...)
    {
        logger.log("BPTreeInternalNode::writeNode ERROR: Unknown exception during bufferManager->writePage for page " + std::to_string(pageId));
        throw; // Re-throw unknown errors
    }
}

/**
 * @brief Fetches the child BPTreeNode object corresponding to the given child index
 * within this internal node's childrenPageIds vector.
 * It retrieves the child's page ID and uses the parent BPTree's fetchNode method
 * (which interacts with the BufferManager and deserializes the node).
 *
 * @param childIndex The 0-based index of the child pointer in the childrenPageIds vector.
 * @return BPTreeNode* Pointer to the fetched child node (Leaf or Internal).
 *         The caller is responsible for deleting this pointer after use.
 * @throws std::out_of_range If the provided childIndex is invalid (less than 0 or
 *         greater than or equal to the number of children pointers).
 * @throws std::runtime_error If the BPTree's fetchNode method fails (e.g., due to
 *         BufferManager error or deserialization failure).
 */
BPTreeNode *BPTreeInternalNode::fetchChild(int childIndex)
{
    // 1. Validate the child index against the size of the childrenPageIds vector.
    //    An internal node with keyCount keys has keyCount + 1 children.
    if (childIndex < 0 || static_cast<size_t>(childIndex) >= childrenPageIds.size())
    {
        logger.log("BPTreeInternalNode::fetchChild ERROR: Invalid child index " + std::to_string(childIndex) + " requested. Node " + std::to_string(pageId) + " has " + std::to_string(childrenPageIds.size()) + " children.");
        throw std::out_of_range("Invalid child index (" + std::to_string(childIndex) + ") requested in BPTreeInternalNode::fetchChild for node " + std::to_string(pageId));
    }

    // 2. Get the page ID of the child to fetch.
    unsigned int childPageId = childrenPageIds[childIndex];
    // Optional: Log fetching attempt
    // logger.log("BPTreeInternalNode::fetchChild node=" + std::to_string(pageId) + " fetching child at index " + std::to_string(childIndex) + " with pageId=" + std::to_string(childPageId));

    // 3. Use the parent tree's fetchNode method to get the actual child node object.
    //    This delegates the responsibility of buffer management and deserialization.
    try
    {
        BPTreeNode *childNode = tree->fetchNode(childPageId);
        if (!childNode)
        {
            // fetchNode returning nullptr likely indicates an issue (e.g., page not found, deserialization error)
            logger.log("BPTreeInternalNode::fetchChild WARNING: tree->fetchNode returned nullptr for child pageId " + std::to_string(childPageId));
            // Depending on context, might throw or return nullptr. Returning nullptr seems reasonable here.
        }
        return childNode;
    }
    catch (const std::exception &e)
    {
        // Catch exceptions from fetchNode (e.g., runtime_error from deserialize)
        logger.log("BPTreeInternalNode::fetchChild ERROR: Exception occurred during tree->fetchNode for child pageId " + std::to_string(childPageId) + ". Details: " + e.what());
        throw; // Re-throw the exception to signal the failure upwards
    }
    catch (...)
    {
        logger.log("BPTreeInternalNode::fetchChild ERROR: Unknown exception occurred during tree->fetchNode for child pageId " + std::to_string(childPageId));
        throw; // Re-throw
    }
}

/**
 * @brief Finds the index of the child pointer that should be followed to find
 * the subtree potentially containing the given search key.
 * In an internal node, keys act as separators. Keys `k_1, k_2, ..., k_n`
 * separate children pointers `p_0, p_1, ..., p_n`.
 * Child `p_0` contains keys `< k_1`.
 * Child `p_i` (for 0 < i <= n) contains keys `>= k_i` and `< k_{i+1}` (if k_{i+1} exists).
 * Child `p_n` contains keys `>= k_n`.
 * This function uses binary search (`std::upper_bound`) on the node's keys
 * to efficiently find the correct child pointer index.
 *
 * @param key The key value being searched for.
 * @return int The 0-based index of the child pointer in the childrenPageIds vector
 *         that leads towards the subtree containing the key.
 */
int BPTreeInternalNode::findFirstKeyIndex(int key)
{
    // std::upper_bound finds the first element in the range that is strictly GREATER than 'key'.
    // The range searched is the valid keys: [keys.begin(), keys.begin() + keyCount).
    auto it = std::upper_bound(keys.begin(),            // Start of the range
                               keys.begin() + keyCount, // End of the *valid* key range
                               key);                    // The value to compare against

    // The index of the child pointer we need to follow is the same as the index
    // of the first key *greater* than the search key. If the search key is greater
    // than or equal to all keys in the node, `upper_bound` returns `keys.begin() + keyCount`,
    // and the distance correctly gives us the index of the rightmost child pointer (index keyCount).
    int index = static_cast<int>(std::distance(keys.begin(), it));

    // Optional: Log the decision
    // logger.log("BPTreeInternalNode::findFirstKeyIndex for key=" + std::to_string(key) + " in node=" + std::to_string(pageId) + " -> childIndex=" + std::to_string(index));

    return index;
}
/**
 * @brief Inserts a key-pointer pair into the appropriate subtree rooted at this internal node.
 * This function delegates the actual insertion down the tree. If a child node splits
 * as a result of the insertion, this function handles inserting the promoted key
 * and the pointer to the new sibling node into this internal node. If this insertion
 * causes *this* node to overflow, it splits itself and signals the split to its parent.
 *
 * @param key The key value to insert (ultimately into a leaf node).
 * @param pointer The RecordPointer associated with the key.
 * @return int Status code:
 *         - 0: Success, insertion completed in subtree, no split propagated to this level.
 *         - 1: Success, but a split occurred *at this level* (or propagated up and caused split here), requiring parent update.
 *         - -1: An error occurred during insertion (e.g., fetch failed, memory allocation failed).
 */
int BPTreeInternalNode::insert(int key, RecordPointer pointer)
{
    // Optional: Log entry for debugging
    // logger.log("BPTreeInternalNode::insert attempting key=" + std::to_string(key) + " into subtree via pageId=" + std::to_string(pageId));

    // 1. Determine which child subtree the key belongs to.
    int childIndex = findFirstKeyIndex(key);
    BPTreeNode *childNode = nullptr; // Pointer to the fetched child node object

    try
    {
        // 2. Fetch the appropriate child node object from disk/buffer.
        childNode = fetchChild(childIndex);
        if (!childNode)
        {
            // If fetchChild returns nullptr (e.g., page not found, deserialization failed), throw an error.
            logger.log("BPTreeInternalNode::insert ERROR: fetchChild returned null for index " + std::to_string(childIndex) + " from node " + std::to_string(pageId));
            throw std::runtime_error("Fetched null child node during internal node insert.");
        }

        // 3. Recursively call insert on the fetched child node.
        int resultFromChild = childNode->insert(key, pointer);

        // 4. Check if the child node reported a split (result == 1).
        bool childDidSplit = (resultFromChild == 1);

        if (childDidSplit)
        {
            // --- Handle Child Split ---
            // *** REMOVED retrieving split info via tree->getLastSplitInfo ***
            // The mechanism to get middleKeyFromSplit and newSiblingPageIdFromSplit is missing.
            // This part needs redesign. For now, we cannot correctly handle the split propagation.
            // We'll log an error and return -1 to indicate failure until redesigned.

            logger.log("BPTreeInternalNode::insert ERROR: Child split occurred, but mechanism to retrieve split info (key/pageId) is missing. Cannot proceed with parent update. Redesign required.");
            delete childNode;
            return -1; // Indicate error due to missing split info mechanism

            /* --- Code below this point needs redesign ---
            int middleKeyFromSplit = 0; // Needs to be obtained somehow
            unsigned int newSiblingPageIdFromSplit = 0; // Needs to be obtained somehow

            // b. Insert the promoted key and the new sibling pointer into *this* node's vectors.
            try {
                keys.insert(keys.begin() + childIndex, middleKeyFromSplit);
                childrenPageIds.insert(childrenPageIds.begin() + childIndex + 1, newSiblingPageIdFromSplit);
                keyCount++;
            } catch (const std::bad_alloc& ba) {
                 logger.log("BPTreeInternalNode::insert ERROR: Memory allocation failed during vector insert (handling child split) for page " + std::to_string(pageId));
                 delete childNode;
                 throw;
            }

            // c. Check if *this* internal node now overflows due to the insertion.
            if (keyCount > tree->getFanout() - 1) {
                // --- Handle Internal Node Split ---
                 // ... (split logic) ...

                 // *** REMOVED storing split info via tree->setLastSplitInfo ***
                 // int keyToPushUp = keys[internalSplitKeyIndex]; // Key needed by parent

                 // ... (write nodes, cleanup) ...

                 // viii. Return 1 to signal that this node split and the parent needs updating.
                 return 1;

            } else {
                 // --- No Overflow in this Internal Node ---
                  try {
                     writeNode();
                     delete childNode;
                     return 0;
                  } catch (const std::exception& e) {
                     logger.log("BPTreeInternalNode::insert ERROR: Exception during writeNode after handling child split for page " + std::to_string(pageId) + ". Details: " + e.what());
                     delete childNode;
                     throw;
                  }
            }
            */
            // --- End of code needing redesign ---
        }
        else if (resultFromChild == 0)
        {
            // --- Child Did Not Split ---
            // Insertion was successful in the child's subtree without causing a split there.
            // No action is needed at this level.
            delete childNode; // Clean up the fetched child node object
            return 0;         // Signal success, no split occurred below.
        }
        else
        {
            // --- Error During Child Insertion ---
            logger.log("BPTreeInternalNode::insert - Child node " + std::to_string(childNode->pageId) + " returned error code " + std::to_string(resultFromChild));
            delete childNode; // Clean up fetched child
            return -1;        // Propagate error signal
        }
    }
    catch (const std::exception &e)
    {
        // Catch exceptions from fetchChild or potential issues within this function
        logger.log("BPTreeInternalNode::insert ERROR: Exception occurred during processing for page " + std::to_string(pageId) + ". Details: " + std::string(e.what()));
        if (childNode)
            delete childNode; // Ensure cleanup if fetch succeeded before error
        return -1;            // Indicate error
    }
    catch (...)
    {
        // Catch any other unknown exceptions
        logger.log("BPTreeInternalNode::insert ERROR: Unknown exception occurred during processing for page " + std::to_string(pageId));
        if (childNode)
            delete childNode;
        return -1; // Indicate error
    }
}

/**
 * @brief Searches for a specific key within the subtree rooted at this internal node.
 * It determines the correct child pointer to follow based on the key and recursively
 * calls search on that child node.
 *
 * @param key The key value to search for.
 * @param result Output parameter; a vector where matching RecordPointer objects (found
 *               in the leaf nodes) are appended by the recursive calls.
 * @return bool True if the key was found in the underlying leaf nodes, false otherwise.
 */
bool BPTreeInternalNode::search(int key, std::vector<RecordPointer> &result)
{
    // Optional: Log entry for debugging
    // logger.log("BPTreeInternalNode::search seeking key=" + std::to_string(key) + " in subtree via pageId=" + std::to_string(pageId));

    // 1. Find the index of the child pointer to follow using binary search on keys.
    int childIndex = findFirstKeyIndex(key);
    BPTreeNode *childNode = nullptr; // Pointer to the fetched child node object

    try
    {
        // 2. Fetch the appropriate child node object.
        childNode = fetchChild(childIndex);
        if (!childNode)
        {
            // Log error if fetch failed
            logger.log("BPTreeInternalNode::search WARNING: fetchChild returned null for index " + std::to_string(childIndex) + " from node " + std::to_string(pageId));
            return false; // Key cannot be found if we can't traverse down
        }

        // 3. Recursively call search on the child node.
        bool found = childNode->search(key, result);

        // 4. Clean up the dynamically allocated child node object.
        delete childNode;

        // 5. Return the result from the recursive call.
        return found;
    }
    catch (const std::exception &e)
    {
        // Catch exceptions from fetchChild or the recursive search call
        logger.log("BPTreeInternalNode::search ERROR: Exception occurred during search processing for page " + std::to_string(pageId) + ". Details: " + std::string(e.what()));
        if (childNode)
            delete childNode; // Ensure cleanup if fetch succeeded before error in recursive call
        return false;         // Indicate key not found due to error
    }
    catch (...)
    {
        // Catch any other unknown exceptions
        logger.log("BPTreeInternalNode::search ERROR: Unknown exception occurred during search processing for page " + std::to_string(pageId));
        if (childNode)
            delete childNode;
        return false; // Indicate key not found due to error
    }
}

/**
 * @brief Searches for all keys within the inclusive range [low, high] in the subtree
 * rooted at this internal node. It identifies the relevant child subtrees that might
 * contain keys in the range and recursively calls searchRange on them.
 *
 * @param low The lower bound of the key range (inclusive).
 * @param high The upper bound of the key range (inclusive).
 * @param result Output parameter; a vector where matching RecordPointer objects (found
 *               in the leaf nodes) are appended by the recursive calls.
 */
void BPTreeInternalNode::searchRange(int low, int high, std::vector<RecordPointer> &result)
{
    // Optional: Log entry for debugging
    // logger.log("BPTreeInternalNode::searchRange searching [" + std::to_string(low) + "," + std::to_string(high) + "] in subtree via pageId=" + std::to_string(pageId));

    // 1. Find the index of the *first* child pointer whose subtree might contain keys >= low.
    //    This is the same logic as finding the insertion point for 'low'.
    int childIndex = findFirstKeyIndex(low);
    BPTreeNode *childNode = nullptr; // Pointer to the fetched child node object

    try
    {
        // 2. Iterate through relevant children starting from childIndex.
        //    We need to check children up to the point where keys are guaranteed to be > high.
        //    An internal node has keyCount keys and keyCount + 1 children. childIndex can range from 0 to keyCount.
        while (static_cast<size_t>(childIndex) <= keyCount)
        { // Iterate through all potentially relevant child pointers

            // a. Fetch the current child node.
            childNode = fetchChild(childIndex);
            if (!childNode)
            {
                // If fetch fails, log it and stop searching down this path.
                // It might be better to throw or handle this more gracefully depending on requirements.
                logger.log("BPTreeInternalNode::searchRange WARNING: fetchChild failed for index " + std::to_string(childIndex) + " from node " + std::to_string(pageId) + ". Skipping subtree.");
                // If we can't fetch a child, we can't continue reliably down this path or subsequent ones.
                break; // Exit the while loop
            }

            // b. Recursively call searchRange on the fetched child node.
            //    The child node (whether internal or leaf) will handle further traversal or data collection.
            childNode->searchRange(low, high, result);

            // c. Clean up the fetched child node object.
            delete childNode;
            childNode = nullptr; // Avoid dangling pointer in case of loop exit/error

            // d. Decide whether to proceed to the next child.
            //    If the current childIndex points past the last key (i.e., childIndex == keyCount),
            //    we've processed the rightmost child, so we must stop.
            //    Otherwise, check the key *at* childIndex in the *current* internal node.
            //    This key acts as the *lower bound* for keys in the *next* child subtree (at index childIndex + 1).
            //    If this separating key is already greater than 'high', then the next subtree
            //    (and all subsequent ones) cannot contain keys in the range [low, high].
            if (static_cast<size_t>(childIndex) < keyCount && keys[childIndex] > high)
            {
                // logger.log("BPTreeInternalNode::searchRange stopping early after child " + std::to_string(childIndex) + " as separating key " + std::to_string(keys[childIndex]) + " > " + std::to_string(high));
                break; // Optimization: No need to check further children.
            }

            // e. Move to the next child index for the next iteration.
            childIndex++;

        } // End while loop iterating through children
    }
    catch (const std::exception &e)
    {
        // Catch exceptions from fetchChild or the recursive calls
        logger.log("BPTreeInternalNode::searchRange ERROR: Exception occurred during processing for page " + std::to_string(pageId) + ". Details: " + std::string(e.what()));
        if (childNode)
            delete childNode; // Ensure cleanup if error happened after a successful fetch
        // Decide whether to re-throw or just stop the range search. Stopping for now.
    }
    catch (...)
    {
        logger.log("BPTreeInternalNode::searchRange ERROR: Unknown exception occurred during processing for page " + std::to_string(pageId));
        if (childNode)
            delete childNode;
        // Stop searching on unknown errors.
    }
}

#include "indexing.h"
#include "logger.h"
#include "table.h"          // For TableCatalogue access and updating index roots
#include "tableCatalogue.h" // For checking table existence
#include <vector>
#include <string>
#include <stdexcept> // For std::runtime_error, etc.
#include <limits>    // Potentially needed for range queries

// --- BPTree Implementation ---

/**
 * @brief Constructs a BPTree instance for a specific table and column.
 * Initializes tree parameters based on calculated constants. If a rootId > 0 is provided,
 * it assumes an existing tree structure needs to be loaded (though actual loading happens
 * via fetchNode when needed).
 * @param tblName Name of the table the index belongs to.
 * @param colName Name of the column being indexed.
 * @param bufMgr Pointer to the shared BufferManager instance.
 * @param rootId The page ID of the root node. If 0, signifies a new, empty tree.
 */
BPTree::BPTree(const std::string &tblName, const std::string &colName, BufferManager *bufMgr, unsigned int rootId)
    : tableName(tblName),            // Store table name
      columnName(colName),           // Store column name
      bufferManager(bufMgr),         // Store buffer manager pointer
      rootPageId(rootId),            // Initialize with provided root ID
      fanout(MIN_FANOUT),            // Initialize with calculated MIN_FANOUT
      leafCapacity(MIN_LEAF_RECORDS) // Initialize with calculated MIN_LEAF_RECORDS
{
    // Log the creation event with parameters for debugging
    // logger.log("BPTree::BPTree created for " + tableName + "." + columnName +
    //            " | root=" + std::to_string(rootPageId) +
    //            " | fanout=" + std::to_string(fanout) +
    //            " | leafCap=" + std::to_string(leafCapacity));

    // Optional: Immediate validation of the rootPageId if it's not 0.
    // This can catch issues early if metadata points to a non-existent/corrupt page.
    if (rootPageId != 0)
    {
        // BPTreeNode* root = nullptr;
        // try {
        //     root = fetchNode(rootPageId); // Attempt to fetch and deserialize
        //     if (!root) {
        //         // Fetch failed (page not found, corrupt data, etc.)
        //         logger.log("BPTree::BPTree WARNING: Initial rootPageId " + std::to_string(rootPageId) +
        //                    " seems invalid (fetchNode failed). Treating tree as empty.");
        //         this->rootPageId = 0; // Reset to empty state
        //     } else {
        //         delete root; // Successfully fetched, don't need the object now
        //     }
        // } catch (const std::exception& e) {
        //      logger.log("BPTree::BPTree WARNING: Exception validating initial rootPageId " + std::to_string(rootPageId) +
        //                 ". Details: " + e.what() + ". Treating tree as empty.");
        //      this->rootPageId = 0; // Reset to empty state on error
        //      if (root) delete root; // Clean up if fetch succeeded before exception
        // }
    }
}

/**
 * @brief Destructor for the BPTree object.
 * Note: This destructor itself doesn't deallocate the *nodes* of the tree.
 * The tree structure exists primarily on disk pages managed by the BufferManager.
 * The in-memory BPTree *object* is typically owned by the Table object, and the
 * Table's destructor is responsible for deleting this BPTree object. Flushing dirty
 * pages to disk is the responsibility of the BufferManager's lifecycle management.
 */
BPTree::~BPTree()
{
    // Optional: Log destruction for debugging
    // logger.log("BPTree::~BPTree destroying in-memory object for index " + tableName + "." + columnName);
    // No explicit node cleanup needed here - relies on BufferManager and Table lifecycle.
}

/**
 * @brief Fetches a B+ Tree node (either Leaf or Internal) from its corresponding page.
 * It constructs the appropriate page filename, requests the page from the BufferManager,
 * retrieves the serialized data (using the assumed Page::getRow(0) hack), and deserializes
 * it into a new BPTreeNode object.
 * @param pageId The unique page ID of the index node to fetch.
 * @return BPTreeNode* Pointer to the newly allocated and deserialized node object.
 *         Returns nullptr if the page cannot be fetched or deserialized correctly.
 *         The caller is responsible for deleting the returned pointer.
 * @throws std::runtime_error If the BufferManager pointer is null. Exceptions from
 *         deserialization might also propagate.
 */
BPTreeNode *BPTree::fetchNode(unsigned int pageId)
{
    // logger.log("BPTree::fetchNode attempting to fetch index pageId=" + std::to_string(pageId) + " for " + tableName + "." + columnName);

    // Precondition check: Buffer Manager must be available.
    if (!bufferManager)
    {
        logger.log("BPTree::fetchNode ERROR: Buffer manager pointer is null.");
        throw std::runtime_error("Buffer manager is not initialized in BPTree::fetchNode");
    }

    // Construct the specific filename for the index page.
    std::string pageName = getIndexPageName(tableName, columnName, pageId);

    try
    {
        // Fetch the page using the BufferManager
        Page page = bufferManager->getPage(tableName, pageId); // Assuming getPage can handle index page names/logic implicitly?
                                                               // OR: Need a specific getIndexPage method?
                                                               // Let's assume getPage works for now, but it might need adjustment
                                                               // if index pages aren't named like data pages.
                                                               // *** Correction: getPage uses TableName_PageX format. We need a way to load index pages.
                                                               // *** Let's assume getPage is smart enough or we need another method.
                                                               // *** Sticking with getPage for now, but this is a potential issue.

        // Check if the fetched page is valid and corresponds to the requested index page
        // Use the getter method for pageName
        if (page.getPageName() == "" || page.getPageName() != pageName)
        {
            logger.log("BPTree::fetchNode WARNING: Fetched page name mismatch or empty page. Requested: " + pageName + ", Got: " + page.getPageName());
            // This might happen if getPage couldn't find the specific index file
            // or if the pageIndex logic in getPage doesn't apply to index files.
            return nullptr; // Indicate failure
        }

        // HACK/ASSUMPTION: Retrieve the serialized data vector<int> from the Page object.
        // Assuming the entire serialized node is stored as the first "row" in the Page.
        std::vector<int> pageData = page.getRow(0); // Get the first (and only) "row"

        if (pageData.empty())
        {
            logger.log("BPTree::fetchNode ERROR: Fetched page " + pageName + " contains no data (empty row 0).");
            return nullptr; // Indicate failure
        }

        // Deserialize the raw data into a BPTreeNode object.
        return deserializeNode(pageData, pageId, this);
    }
    catch (const std::exception &e)
    {
        // Catch errors from getPage, getRow, or deserializeNode
        logger.log("BPTree::fetchNode ERROR: Exception fetching/deserializing page " + std::to_string(pageId) + " (" + pageName + "). Details: " + e.what());
        return nullptr; // Indicate failure
    }
    catch (...)
    {
        logger.log("BPTree::fetchNode ERROR: Unknown exception fetching/deserializing page " + std::to_string(pageId) + " (" + pageName + ").");
        return nullptr; // Indicate failure
    }
}

/**
 * @brief Acquires a new, unique page ID for creating a new index node.
 * **NOTE:** This implementation uses a simple static counter, which is **NOT SUITABLE**
 * for robust, concurrent, or persistent systems. It's a placeholder. A real system
 * needs a proper page allocation mechanism, likely integrated with the BufferManager
 * or a dedicated disk space manager, that handles reuse of freed pages.
 * @return unsigned int A newly allocated page ID.
 * @throws std::runtime_error If the BufferManager pointer is null.
 */
unsigned int BPTree::getNewPageId()
{
    // logger.log("BPTree::getNewPageId requesting new index page ID for " + tableName + "." + columnName);

    if (!bufferManager)
    {
        logger.log("BPTree::getNewPageId ERROR: Buffer manager pointer is null.");
        throw std::runtime_error("Buffer manager is not initialized in BPTree::getNewPageId");
    }

    // --- HACK: Static Counter Placeholder ---
    // This is unsafe and basic. Replace with a real allocation strategy.
    static unsigned int indexPageCounter = 50000; // Start high to reduce collision risk with data pages
    unsigned int newId = indexPageCounter++;
    // logger.log("BPTree::getNewPageId allocated temporary ID: " + std::to_string(newId));
    if (newId == 0)
    { // Check for potential overflow if using unsigned int limits extensively
        logger.log("BPTree::getNewPageId ERROR: Page ID counter overflowed or wrapped to 0!");
        throw std::runtime_error("Page ID allocation failed (counter overflow).");
    }
    return newId;
    // --- End HACK ---

    // Ideal implementation would call something like:
    // return bufferManager->allocateNewIndexPage(tableName, columnName);
}

/**
 * @brief Updates the root page ID of this B+ Tree instance.
 * If the new ID is different from the current one, it updates the member variable
 * and notifies the owning Table object (via the TableCatalogue) to update its
 * persisted metadata map (indexRoots).
 * @param newRootId The new page ID to set as the root. Can be 0 if the tree becomes empty.
 */
void BPTree::updateRoot(unsigned int newRootPageId)
{
    logger.log("BPTree::updateRoot changing root from " + std::to_string(this->rootPageId) + " to " + std::to_string(newRootPageId));
    this->rootPageId = newRootPageId;
    // Persistence logic removed - Table class no longer stores root IDs directly.
    // The BPTree object itself holds the current root in memory.
    // Persistence would need saving the BPTree object's state or the root ID elsewhere.
}

/**
 * @brief Inserts a key and its corresponding data pointer into the B+ Tree.
 * Handles the initial creation of the root if the tree is empty. Otherwise,
 * it fetches the root node and delegates the insertion recursively. If the
 * root node splits during the insertion process, this method creates a new
 * root (internal node) pointing to the two nodes resulting from the split.
 * @param key The key value to insert.
 * @param pointer The RecordPointer pointing to the actual data row associated with the key.
 * @throws std::runtime_error If fetching the root node fails or if memory allocation fails.
 *         Exceptions from lower levels might also propagate.
 */
void BPTree::insert(int key, RecordPointer pointer)
{
    // logger.log("BPTree::insert key=" + std::to_string(key) + " ptr=("+ std::to_string(pointer.pageId)+","+std::to_string(pointer.rowIndex)+") into " + tableName + "." + columnName);

    // --- Case 1: Tree is Empty ---
    if (rootPageId == 0)
    {
        // logger.log("BPTree::insert - Empty tree, creating root leaf node.");
        unsigned int newRootId = 0;
        BPTreeLeafNode *rootNode = nullptr;
        try
        {
            newRootId = getNewPageId();
            rootNode = new BPTreeLeafNode(newRootId, this); // Create new leaf
            rootNode->insert(key, pointer);                 // Insert into the new leaf (this also writes it)
            updateRoot(newRootId);                          // Set this new leaf as the root
            delete rootNode;                                // Clean up in-memory object
            return;                                         // Insertion complete
        }
        catch (const std::exception &e)
        {
            logger.log("BPTree::insert ERROR: Exception creating initial root: " + std::string(e.what()));
            if (rootNode)
                delete rootNode; // Clean up if allocated before error
            // If root ID was allocated but node creation/write failed, we might have a leaked page ID.
            // Proper allocator would handle this.
            throw; // Re-throw critical error
        }
    }

    // --- Case 2: Tree Not Empty ---
    BPTreeNode *rootNode = nullptr;
    try
    {
        // Fetch the current root node
        rootNode = fetchNode(rootPageId);
        if (!rootNode)
        {
            logger.log("BPTree::insert - ERROR: Failed to fetch existing root node ID: " + std::to_string(rootPageId));
            throw std::runtime_error("B+ Tree Insert failed: Could not fetch root node " + std::to_string(rootPageId));
        }

        // Delegate insertion to the root node (recursive)
        int result = rootNode->insert(key, pointer);

        // Check if the root node itself split (result == 1)
        if (result == 1)
        {
            // logger.log("BPTree::insert - Root node " + std::to_string(rootPageId) + " split. Creating new root.");

            // a. Create a new internal node to serve as the new root.
            unsigned int newRootPageId = getNewPageId();
            BPTreeInternalNode *newRoot = new BPTreeInternalNode(newRootPageId, this);

            // b. Retrieve the key and sibling ID promoted from the split of the old root.
            int middleKey = 0;
            unsigned int newSiblingPageId = 0;
            // getLastSplitInfo(middleKey, newSiblingPageId); // Use temporary storage mechanism

            // c. Populate the new root node.
            newRoot->keys.push_back(middleKey);                   // Separating key
            newRoot->childrenPageIds.push_back(this->rootPageId); // Old root is the left child
            newRoot->childrenPageIds.push_back(newSiblingPageId); // New sibling from split is the right child
            newRoot->keyCount = 1;                                // New root has one key

            // d. Write the new root node to its page.
            newRoot->writeNode();

            // e. Update the BPTree's rootPageId and notify the Table object.
            updateRoot(newRootPageId);

            // f. Clean up the in-memory object for the new root.
            delete newRoot;
        }

        // Clean up the fetched root node object (whether it split or not)
        delete rootNode;
    }
    catch (const std::exception &e)
    {
        // Catch exceptions from fetchNode, recursive insert, or new root creation
        logger.log("BPTree::insert ERROR: Exception during insert processing: " + std::string(e.what()));
        if (rootNode)
            delete rootNode; // Ensure cleanup if fetched before error
        throw;               // Re-throw exception
    }
    catch (...)
    {
        logger.log("BPTree::insert ERROR: Unknown exception during insert processing.");
        if (rootNode)
            delete rootNode;
        throw;
    }
}

/**
 * @brief Removes a specific key-pointer pair from the B+ Tree.
 * Handles the case of an empty tree. Otherwise, fetches the root and delegates
 * the removal recursively. Manages changes to the root node if the tree height
 * decreases due to merges (e.g., if the root becomes an internal node with only
 * one child, that child becomes the new root) or if the tree becomes empty.
 * @param key The key value of the entry to remove.
 * @param pointer The specific RecordPointer to remove (required for handling duplicate keys).
 * @throws std::runtime_error If fetching the root node fails. Exceptions from lower
 *         levels or page deallocation might also propagate.
 */
void BPTree::remove(int key, RecordPointer pointer)
{
    // logger.log("BPTree::remove key=" + std::to_string(key) + " ptr=("+std::to_string(pointer.pageId)+","+std::to_string(pointer.rowIndex)+") from " + tableName + "." + columnName);

    // --- Case 1: Tree is Empty ---
    if (rootPageId == 0)
    {
        // logger.log("BPTree::remove - Cannot remove from empty tree.");
        return; // Nothing to remove
    }

    // --- Case 2: Tree Not Empty ---
    BPTreeNode *rootNode = nullptr;
    try
    {
        // Fetch the current root node
        rootNode = fetchNode(rootPageId);
        if (!rootNode)
        {
            logger.log("BPTree::remove ERROR: Failed to fetch root node ID: " + std::to_string(rootPageId));
            throw std::runtime_error("B+ Tree Remove failed: Could not fetch root node " + std::to_string(rootPageId));
        }

        // Delegate removal to the root node (recursive)
        int result = rootNode->remove(key, pointer);

        // --- Handle Root Adjustments after Removal ---

        // Check if the root node needs replacement (tree height decreased)
        // This happens if the root was internal, had its key removed (due to merge below),
        // and is left with only one child.
        if (!rootNode->isLeaf && rootNode->keyCount == 0 && this->rootPageId != 0)
        {
            logger.log("BPTree::remove - Root adjustment needed after remove.");
            BPTreeInternalNode *internalRoot = static_cast<BPTreeInternalNode *>(rootNode);
            // If root has 0 keys, it must have exactly 1 child pointer left after a merge.
            if (internalRoot->childrenPageIds.size() == 1)
            {
                unsigned int oldRootPageId = this->rootPageId;
                unsigned int newRootChildId = internalRoot->childrenPageIds[0];
                logger.log("BPTree::remove - Internal root " + std::to_string(oldRootPageId) + " becoming its only child " + std::to_string(newRootChildId) + ".");
                updateRoot(newRootChildId);    // The single child becomes the new root
                deleteNodePage(oldRootPageId); // Deallocate the old root's page
            }
            else
            {
                // This state (0 keys but != 1 child) indicates a potential logic error in merge handling.
                logger.log("BPTree::remove WARNING: Internal root " + std::to_string(this->rootPageId) + " has 0 keys but " + std::to_string(internalRoot->childrenPageIds.size()) + " children. Index might be inconsistent.");
            }
        }
        // Check if the root was a leaf and became completely empty.
        else if (rootNode->isLeaf && rootNode->keyCount == 0 && this->rootPageId != 0)
        {
            logger.log("BPTree::remove - Root leaf " + std::to_string(this->rootPageId) + " is now empty. Tree becomes empty.");
            unsigned int oldRootPageId = this->rootPageId;
            updateRoot(0);                 // Mark tree as empty
            deleteNodePage(oldRootPageId); // Deallocate the old root's page
        }

        // Clean up the fetched root node object
        delete rootNode;
    }
    catch (const std::exception &e)
    {
        logger.log("BPTree::remove ERROR: Exception during remove processing: " + std::string(e.what()));
        if (rootNode)
            delete rootNode; // Ensure cleanup
        throw;               // Re-throw exception
    }
    catch (...)
    {
        logger.log("BPTree::remove ERROR: Unknown exception during remove processing.");
        if (rootNode)
            delete rootNode;
        throw;
    }
}

/**
 * @brief Searches the B+ Tree for a specific key and retrieves all associated RecordPointers.
 * Starts the search from the root node.
 * @param key The key value to search for.
 * @return std::vector<RecordPointer> A vector containing all RecordPointers associated
 *         with the given key. The vector will be empty if the key is not found or if
 *         the tree is empty or an error occurs during search.
 */
std::vector<RecordPointer> BPTree::search(int key)
{
    // logger.log("BPTree::search key=" + std::to_string(key) + " in " + tableName + "." + columnName);
    std::vector<RecordPointer> result; // Initialize empty result vector

    // Handle empty tree case
    if (rootPageId == 0)
    {
        // logger.log("BPTree::search - Tree is empty.");
        return result;
    }

    BPTreeNode *rootNode = nullptr;
    try
    {
        // Fetch the root node
        rootNode = fetchNode(rootPageId);
        if (!rootNode)
        {
            // Log error if root cannot be fetched
            logger.log("BPTree::search - ERROR: Failed to fetch root node " + std::to_string(rootPageId) + ". Returning empty result.");
            return result; // Return empty vector indicating failure or empty tree
        }

        // Delegate the search to the root node (recursive)
        rootNode->search(key, result);

        // Clean up the fetched root node object
        delete rootNode;
    }
    catch (const std::exception &e)
    {
        // Catch exceptions during fetch or recursive search
        logger.log("BPTree::search ERROR: Exception during search processing: " + std::string(e.what()));
        if (rootNode)
            delete rootNode; // Ensure cleanup
        result.clear();      // Clear any partial results collected before the error
                             // Depending on requirements, you might re-throw here. Returning empty for now.
    }
    catch (...)
    {
        logger.log("BPTree::search ERROR: Unknown exception during search processing.");
        if (rootNode)
            delete rootNode;
        result.clear();
    }

    // logger.log("BPTree::search found " + std::to_string(result.size()) + " results for key=" + std::to_string(key));
    return result; // Return the collected results (might be empty)
}

/**
 * @brief Searches the B+ Tree for all keys within the specified range [low, high] (inclusive)
 * and retrieves all associated RecordPointers.
 * Finds the appropriate starting leaf node and then traverses the leaf linked list.
 * @param low The lower bound of the key range.
 * @param high The upper bound of the key range.
 * @return std::vector<RecordPointer> A vector containing all RecordPointers associated
 *         with keys within the specified range. Empty if no keys are found or an error occurs.
 */
std::vector<RecordPointer> BPTree::searchRange(int low, int high)
{
    // logger.log("BPTree::searchRange [" + std::to_string(low) + "," + std::to_string(high) + "] in " + tableName + "." + columnName);
    std::vector<RecordPointer> result; // Initialize empty result vector

    // Handle empty tree case
    if (rootPageId == 0)
    {
        // logger.log("BPTree::searchRange - Tree is empty.");
        return result;
    }

    BPTreeNode *rootNode = nullptr;    // To hold the initially fetched root
    BPTreeNode *currentNode = nullptr; // To traverse down the tree

    try
    {
        // Fetch the root node
        rootNode = fetchNode(rootPageId);
        if (!rootNode)
        {
            logger.log("BPTree::searchRange - ERROR: Failed to fetch root node " + std::to_string(rootPageId) + ". Returning empty result.");
            return result;
        }

        // 1. Traverse down to the leftmost leaf node that *could* contain 'low'.
        currentNode = rootNode;
        while (currentNode && !currentNode->isLeaf)
        {
            BPTreeInternalNode *internal = static_cast<BPTreeInternalNode *>(currentNode);
            int childIndex = internal->findFirstKeyIndex(low);       // Find child pointer to follow
            BPTreeNode *nextNode = internal->fetchChild(childIndex); // Fetch the child

            // Clean up the *previous* node if it wasn't the original root we fetched
            if (currentNode != rootNode)
            {
                delete currentNode;
            }
            currentNode = nextNode; // Move down to the child

            // Check if fetching the next node failed
            if (!currentNode)
            {
                logger.log("BPTree::searchRange ERROR: Failed to fetch intermediate node during descent. Aborting range search.");
                // Clean up the original root node if it's still held
                if (rootNode && rootNode != currentNode)
                    delete rootNode; // Avoid double delete if currentNode is rootNode
                return result;       // Return empty result due to traversal error
            }
        }

        // At this point, 'currentNode' should be the starting leaf node (or null if error occurred)
        if (currentNode && currentNode->isLeaf)
        {
            BPTreeLeafNode *startLeaf = static_cast<BPTreeLeafNode *>(currentNode);
            // logger.log("BPTree::searchRange starting leaf scan at pageId=" + std::to_string(startLeaf->pageId));
            // 2. Delegate the range scan along the leaf linked list starting from this leaf.
            startLeaf->searchRange(low, high, result);
        }
        else if (!currentNode)
        {
            // Error already logged during descent
        }
        else
        {
            // Should not happen if logic is correct (reached a non-leaf node unexpectedly)
            logger.log("BPTree::searchRange ERROR: Traversal ended on a non-leaf node?");
        }

        // 3. Clean up the nodes fetched during the descent.
        //    'currentNode' points to the final node reached (startLeaf or null).
        //    'rootNode' points to the original root.
        if (currentNode && currentNode != rootNode)
        {
            delete currentNode; // Clean up the final node if it wasn't the root
        }
        // Always clean up the originally fetched root object if it wasn't the final node (or if descent failed early)
        if (rootNode)
        {
            delete rootNode;
        }
    }
    catch (const std::exception &e)
    {
        logger.log("BPTree::searchRange ERROR: Exception during search processing: " + std::string(e.what()));
        // Clean up any potentially allocated nodes
        if (currentNode && currentNode != rootNode)
            delete currentNode;
        if (rootNode)
            delete rootNode;
        result.clear(); // Clear partial results
    }
    catch (...)
    {
        logger.log("BPTree::searchRange ERROR: Unknown exception during search processing.");
        if (currentNode && currentNode != rootNode)
            delete currentNode;
        if (rootNode)
            delete rootNode;
        result.clear();
    }

    // logger.log("BPTree::searchRange found " + std::to_string(result.size()) + " results for range ["+std::to_string(low)+","+std::to_string(high)+"].");
    return result; // Return collected results
}

/**
 * @brief Deletes the physical page file associated with a given index node page ID.
 * Uses the BufferManager's deletePage functionality.
 * @param pageId The ID of the index node page to delete.
 */
void BPTree::deleteNodePage(unsigned int pageId)
{
    if (!bufferManager)
    {
        logger.log("BPTree::deleteNodePage ERROR: BufferManager is null.");
        return; // Or throw
    }
    std::string pageName = getIndexPageName(tableName, columnName, pageId);
    logger.log("BPTree::deleteNodePage requesting deletion of: " + pageName);
    bufferManager->deletePage(tableName + "_" + columnName + "_idx", pageId); // Use the prefix convention expected by BufferManager
    // A more robust system might need to track allocated/freed index pages separately.
}

/**
 * @brief Removes a specific key-pointer pair from the subtree rooted at this internal node.
 * Delegates the removal to the appropriate child. If the child operation results in
 * underflow (returns 1), this function needs to handle merging or redistribution
 * between the underflowed child and its sibling(s).
 *
 * **NOTE:** Merge/redistribution logic for internal nodes is **NOT IMPLEMENTED**.
 *
 * @param key The key value to remove.
 * @param pointer The specific RecordPointer associated with the key (needed for leaf level).
 * @return int Status code:
 *         -  0: Success, removal completed in subtree, no underflow propagated to this level.
 *         -  1: Success, but an underflow occurred *at this level* (or propagated up and caused underflow here) due to merge/redistribute, requiring parent update.
 *         - -1: Key-pointer pair not found in the relevant leaf node, or an error occurred.
 */
int BPTreeInternalNode::remove(int key, const RecordPointer &pointer)
{
    // Optional: Log entry for debugging
    // logger.log("BPTreeInternalNode::remove attempting key=" + std::to_string(key) + " ptr=(" + std::to_string(pointer.pageId) + "," + std::to_string(pointer.rowIndex) + ") from subtree at pageId=" + std::to_string(pageId));

    // 1. Find the correct child index to follow.
    int childIndex = findFirstKeyIndex(key);

    // 2. Fetch the child node.
    BPTreeNode *childNode = nullptr;
    try
    {
        childNode = fetchChild(childIndex);
        if (!childNode)
        {
            logger.log("BPTreeInternalNode::remove ERROR: Failed to fetch child node at index " + std::to_string(childIndex) + " from pageId=" + std::to_string(pageId));
            return -1; // Indicate error
        }
    }
    catch (const std::exception &e)
    {
        logger.log("BPTreeInternalNode::remove EXCEPTION fetching child: " + std::string(e.what()));
        return -1; // Indicate error
    }

    // 3. Recursively call remove on the child node.
    int underflowStatus = childNode->remove(key, pointer);

    // 4. Clean up the fetched child node object.
    delete childNode;
    childNode = nullptr;

    // 5. Handle the result of the recursive removal.
    if (underflowStatus == -1)
    {
        // logger.log("BPTreeInternalNode::remove - Key-pointer not found in child subtree.");
        return -1; // Key not found, propagate -1.
    }
    else if (underflowStatus == 0)
    {
        // logger.log("BPTreeInternalNode::remove - Removal successful in child, no underflow propagated to pageId=" + std::to_string(pageId));
        return 0; // No underflow occurred in the child, this node is unchanged.
    }
    else // underflowStatus == 1 (Underflow occurred in the child)
    {
        // --- Handle Child Underflow ---
        logger.log("BPTreeInternalNode::remove - Underflow detected in child at index " + std::to_string(childIndex) + " (relative to pageId=" + std::to_string(pageId) + "). Needs merge/redistribute.");

        // ********************************************************************
        // TODO: IMPLEMENT MERGE / REDISTRIBUTE LOGIC FOR INTERNAL NODES HERE
        // This involves:
        // 1. Identifying sibling(s) using this node's childrenPageIds.
        // 2. Fetching a sibling node.
        // 3. Checking if redistribution is possible (borrowing a key/pointer from sibling through parent).
        //    - Requires updating keys in this node, the sibling, and the child.
        // 4. Checking if merging is possible (combining child, sibling, and separating key from this node).
        //    - Requires removing a key and child pointer from this node.
        //    - May cause *this* node to underflow, requiring propagation (return 1).
        //    - Requires deallocating the empty node's page.
        // Note: This is significantly more complex than leaf node merge/redistribute.
        // ********************************************************************

        // Placeholder: Return 1 to signal underflow that needs parent handling.
        // This is technically incorrect if merge/redistribute *within* this level
        // resolves the underflow without this node itself underflowing.
        // But without the logic, we must assume the worst case.
        return 1;
    }
}