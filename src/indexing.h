#ifndef INDEXING_H
#define INDEXING_H

#include "global.h" // Ensure global definitions are included first

#include "page.h"          // Defines Page class
#include "bufferManager.h" // Defines BufferManager class
#include <vector>
#include <string>
#include <utility>   // For std::pair
#include <cmath>     // For std::floor in calculations
#include <fstream>   // Used for serialization logic (even if implemented in .cpp)
#include <sstream>   // Used for serialization logic (even if implemented in .cpp)
#include <stdexcept> // For exceptions
#include <limits>    // Needed for searchRange implementation

// Forward declarations
class BPTree;
class BPTreeNode;

// --- B+ Tree Parameter Calculation ---
// Moved inside the include guard and after including global.h

// Size assumptions (in bytes) - adjust if your system or data types differ.
const unsigned int SIZEOF_INT = sizeof(int);                        // Size of a key
const unsigned int SIZEOF_UNSIGNED_INT = sizeof(unsigned int);      // Size of a page ID or row index
const unsigned int SIZEOF_BOOL = sizeof(bool);                      // Size of the isLeaf flag
const unsigned int SIZEOF_RECORD_POINTER = 2 * SIZEOF_UNSIGNED_INT; // Size of RecordPointer struct
// Use the literal value directly since BLOCK_SIZE is constexpr 1.0f
const unsigned int PAGE_SIZE_BYTES = (unsigned int)(1.0f * 1000); // Page size in bytes (as per spec: 1KB blocks = 1000 bytes)

// Estimated overhead per node page for metadata (e.g., isLeaf flag, current key count).
// This needs to accurately reflect how you store node metadata within the page.
const unsigned int NODE_METADATA_SIZE = SIZEOF_BOOL + SIZEOF_UNSIGNED_INT; // isLeaf + keyCount

// Calculate Fanout (Order 'm' for internal nodes - max number of children)
// An internal node with 'k' keys has 'k+1' children pointers.
// Structure: Meta | Ptr0 | Key1 | Ptr1 | Key2 | Ptr2 | ... | Key(m-1) | Ptr(m-1)
// PageSize >= MetaSize + m * ChildPtrSize + (m-1) * KeySize
// PageSize >= MetaSize + m * (ChildPtrSize + KeySize) - KeySize
// m <= floor( (PageSize - MetaSize + KeySize) / (ChildPtrSize + KeySize) )
const unsigned int FANOUT = static_cast<unsigned int>(
    std::floor((float)(PAGE_SIZE_BYTES - NODE_METADATA_SIZE + SIZEOF_INT) /
               (float)(SIZEOF_UNSIGNED_INT + SIZEOF_INT)));

// Calculate Leaf Capacity (Order 'L' for leaf nodes - max number of key-pointer pairs)
// Structure: Meta | NextPtr | Key1 | RecPtr1 | Key2 | RecPtr2 | ... | KeyL | RecPtrL
// PageSize >= MetaSize + NextPtrSize + L * (KeySize + RecordPtrSize)
// L <= floor( (PageSize - MetaSize - NextPtrSize) / (KeySize + RecordPtrSize) )
const unsigned int LEAF_MAX_RECORDS = static_cast<unsigned int>(
    std::floor((float)(PAGE_SIZE_BYTES - NODE_METADATA_SIZE - SIZEOF_UNSIGNED_INT) /
               (float)(SIZEOF_INT + SIZEOF_RECORD_POINTER)));

// Ensure minimum valid order for B+ Tree functionality
// Fanout must be >= 3. Leaf capacity must be >= 1.
const unsigned int MIN_FANOUT = (FANOUT >= 3) ? FANOUT : 3;
const unsigned int MIN_LEAF_RECORDS = (LEAF_MAX_RECORDS >= 1) ? LEAF_MAX_RECORDS : 1;

// --- End B+ Tree Parameter Calculation ---

// --- Helper Function Declarations (Implemented in indexing.cpp) ---

/**
 * @brief Generates the unique filename for an index node page.
 * Assumes BufferManager can handle filenames in this format.
 * Format: ../data/temp/<tableName>_<columnName>_idx_Page<pageId>
 * @param tableName Name of the table.
 * @param columnName Name of the indexed column.
 * @param pageId Page ID of the index node.
 * @return Standardized filename string.
 */
std::string getIndexPageName(const std::string &tableName, const std::string &columnName, unsigned int pageId);

/**
 * @brief Serializes a BPTreeNode object into a flat data structure suitable for writing to a page.
 * NOTE: This example uses vector<int> for simplicity. A production system would likely use
 * a char buffer (vector<char> or similar) for precise byte-level control and efficiency.
 * @param node The node object to serialize.
 * @param pageData Output vector representing the serialized page content.
 */
void serializeNode(BPTreeNode *node, std::vector<int> &pageData);

/**
 * @brief Deserializes page data back into a BPTreeNode object (Leaf or Internal).
 * @param pageData Input vector representing the serialized page content.
 * @param pageId The page ID this data belongs to.
 * @param tree Pointer to the parent BPTree object.
 * @return Pointer to the newly created BPTreeNode object (caller manages deletion).
 * @throws std::runtime_error if deserialization fails (e.g., empty data).
 */
BPTreeNode *deserializeNode(const std::vector<int> &pageData, unsigned int pageId, BPTree *tree);

// --- Core Indexing Structures ---

/**
 * @brief Structure to represent a pointer to a specific record (row)
 * within a specific data page of a table.
 */
struct RecordPointer
{
    unsigned int pageId;   // ID of the data page containing the record.
    unsigned int rowIndex; // 0-based index of the row within that page.

    // Comparison operators for sorting and searching within index nodes.
    bool operator==(const RecordPointer &other) const
    {
        return pageId == other.pageId && rowIndex == other.rowIndex;
    }
    bool operator!=(const RecordPointer &other) const
    {
        return !(*this == other);
    }
    bool operator<(const RecordPointer &other) const
    {
        if (pageId != other.pageId)
            return pageId < other.pageId;
        return rowIndex < other.rowIndex; // Important for stable ordering if keys are equal
    }
};

/**
 * @brief Abstract Base Class for nodes within the B+ Tree.
 * Defines the common interface for Leaf and Internal nodes.
 */
class BPTreeNode
{
public:
    bool isLeaf;           // Flag indicating if the node is a leaf (true) or internal (false).
    std::vector<int> keys; // Stores keys. In internal nodes, keys separate child pointers.
                           // In leaf nodes, keys correspond to data records.
    unsigned int pageId;   // The unique Page ID where this node's data is stored on disk/buffer.
    BPTree *tree;          // Pointer back to the BPTree instance this node belongs to.
                           // Used to access tree-wide info like BufferManager, constants, etc.
    unsigned int keyCount; // Current number of keys stored in this node.

    /**
     * @brief Constructor for BPTreeNode.
     * @param id The page ID for this node.
     * @param treePtr Pointer to the owning BPTree.
     * @param leafStatus True if this is a leaf node, false otherwise.
     */
    BPTreeNode(unsigned int id, BPTree *treePtr, bool leafStatus)
        : isLeaf(leafStatus), pageId(id), tree(treePtr), keyCount(0) {}

    /**
     * @brief Virtual destructor.
     */
    virtual ~BPTreeNode() = default;

    /**
     * @brief Writes the current state of this node back to its corresponding page
     * via the BufferManager. Must be implemented by derived classes.
     */
    virtual void writeNode() = 0;

    /**
     * @brief Inserts a key-value pair (key and RecordPointer) into the subtree rooted at this node.
     * Handles splitting if the node overflows.
     * @param key The key to insert.
     * @param pointer The RecordPointer associated with the key (only relevant for leaf insertion).
     * @return Status code: 0=Success, 1=Node split occurred (may require parent update), -1=Error/Duplicate(optional).
     */
    virtual int insert(int key, RecordPointer pointer) = 0;

    /**
     * @brief Removes a specific key-pointer pair from the subtree rooted at this node.
     * Handles underflow by merging or redistributing with siblings.
     * @param key The key to remove.
     * @param pointer The specific RecordPointer associated with the key to remove (needed for non-unique keys).
     * @return Status code: 0=Success, 1=Underflow/Merge occurred (may require parent update), -1=Error/Not Found.
     */
    virtual int remove(int key, const RecordPointer &pointer) = 0;

    /**
     * @brief Searches for a specific key in the subtree rooted at this node.
     * @param key The key to search for.
     * @param result Output vector where found RecordPointers are added.
     * @return True if the key was found, false otherwise.
     */
    virtual bool search(int key, std::vector<RecordPointer> &result) = 0;

    /**
     * @brief Searches for all keys within a given range [low, high] in the subtree.
     * Primarily implemented by leaf nodes traversing the linked list.
     * @param low The lower bound of the key range (inclusive).
     * @param high The upper bound of the key range (inclusive).
     * @param result Output vector where found RecordPointers are added.
     */
    virtual void searchRange(int low, int high, std::vector<RecordPointer> &result) = 0;

    /**
     * @brief Finds the index of the first key in the node that is greater than or equal to the given key.
     * Used for navigation during search, insert, and delete operations.
     * In internal nodes, determines which child pointer to follow.
     * @param key The key to compare against.
     * @return The 0-based index.
     */
    virtual int findFirstKeyIndex(int key) = 0;
};

/**
 * @brief Represents a Leaf Node in the B+ Tree.
 * Stores actual key-data (RecordPointer) pairs and pointers to sibling leaves.
 */
class BPTreeLeafNode : public BPTreeNode
{
public:
    std::vector<RecordPointer> pointers; // Stores RecordPointers corresponding to keys.
    unsigned int nextPageId;             // Page ID of the next leaf node in sequence (0 if none).

    /**
     * @brief Constructor for BPTreeLeafNode.
     * @param id The page ID for this node.
     * @param treePtr Pointer to the owning BPTree.
     */
    BPTreeLeafNode(unsigned int id, BPTree *treePtr);
    ~BPTreeLeafNode() override = default;

    // --- Overridden Virtual Methods ---
    void writeNode() override;
    int insert(int key, RecordPointer pointer) override;
    int remove(int key, const RecordPointer &pointer) override;
    bool search(int key, std::vector<RecordPointer> &result) override;
    void searchRange(int low, int high, std::vector<RecordPointer> &result) override;
    int findFirstKeyIndex(int key) override;

    // --- Leaf-Specific Logic (to be implemented in .cpp) ---
    // TODO: Implement split logic: Creates a new sibling node, moves half the data,
    //       updates next pointers, and returns the middle key to propagate upwards.
    // int split(BPTreeLeafNode* newNode);

    // TODO: Implement merge/redistribute logic for handling underflow.
};

/**
 * @brief Represents an Internal Node (Index Node) in the B+ Tree.
 * Stores keys that guide the search and pointers to child nodes.
 */
class BPTreeInternalNode : public BPTreeNode
{
public:
    // Stores Page IDs of child nodes. If there are 'k' keys, there are 'k+1' children.
    // childrenPageIds[i] points to the subtree for keys < keys[i].
    // childrenPageIds[k] points to the subtree for keys >= keys[k-1].
    std::vector<unsigned int> childrenPageIds;

    /**
     * @brief Constructor for BPTreeInternalNode.
     * @param id The page ID for this node.
     * @param treePtr Pointer to the owning BPTree.
     */
    BPTreeInternalNode(unsigned int id, BPTree *treePtr);
    ~BPTreeInternalNode() override = default;

    // --- Overridden Virtual Methods ---
    void writeNode() override;
    int insert(int key, RecordPointer pointer) override;
    int remove(int key, const RecordPointer &pointer) override;
    bool search(int key, std::vector<RecordPointer> &result) override;
    void searchRange(int low, int high, std::vector<RecordPointer> &result) override;
    int findFirstKeyIndex(int key) override; // Returns index of the child pointer to follow

    // --- Internal Node-Specific Helpers (implemented in .cpp) ---
    /**
     * @brief Fetches the child node corresponding to the given index.
     * Handles fetching the page via the BufferManager and deserializing it.
     * @param childIndex The index of the child pointer in childrenPageIds.
     * @return Pointer to the fetched child node (caller manages deletion).
     * @throws std::out_of_range if childIndex is invalid.
     * @throws std::runtime_error if fetching/deserializing fails.
     */
    BPTreeNode *fetchChild(int childIndex);

    // Removed declarations for split, merge, redistribute as they are not implemented/defined
    // bool redistributeWithLeftSibling(...);
    // bool redistributeWithRightSibling(...);
    // void mergeWithLeftSibling(...);
    // void mergeWithRightSibling(...);
    // bool handleChildUnderflow(...);
};

/**
 * @brief Main B+ Tree class managing the overall index structure for a column.
 * Owns the root node and interacts with the BufferManager and Table.
 */
class BPTree
{
    friend class BPTreeNode; // Allow nodes to access tree members (like bufferManager)
    friend class BPTreeLeafNode;
    friend class BPTreeInternalNode;

private:
    std::string tableName;           // Name of the table this index belongs to.
    std::string columnName;          // Name of the column this index is built on.
    unsigned int rootPageId;         // Page ID of the root node (0 if the tree is empty).
    BufferManager *bufferManager;    // Pointer to the shared BufferManager instance.
    const unsigned int fanout;       // Max children for internal nodes (Order m).
    const unsigned int leafCapacity; // Max key-pointer pairs for leaf nodes (Order L).

    /**
     * @brief Fetches a node (Leaf or Internal) from disk/buffer given its page ID.
     * Uses the BufferManager and node deserialization.
     * @param pageId The ID of the page containing the node data.
     * @return Pointer to the fetched BPTreeNode (caller manages deletion).
     *         Returns nullptr if the page cannot be fetched or is invalid.
     */
    BPTreeNode *fetchNode(unsigned int pageId);

    /**
     * @brief Allocates a new, unused page ID for creating a new index node.
     * Needs coordination with the BufferManager or a central page allocator.
     * @return The allocated page ID.
     * @throws std::runtime_error if allocation fails.
     */
    unsigned int getNewPageId();

    /**
     * @brief Updates the rootPageId member and notifies the owning Table object
     *        so it can update its persisted metadata.
     * @param newRootId The new page ID of the root node.
     */
    void updateRoot(unsigned int newRootId);

public:
    /**
     * @brief Constructs a BPTree instance.
     * @param tblName Name of the table.
     * @param colName Name of the indexed column.
     * @param bufMgr Pointer to the application's BufferManager.
     * @param rootId The Page ID of the root node. If 0, indicates a new/empty tree.
     */
    BPTree(const std::string &tblName, const std::string &colName, BufferManager *bufMgr, unsigned int rootId = 0);

    /**
     * @brief Destructor. (Doesn't explicitly delete nodes, assumes owning Table manages BPTree lifetime).
     */
    ~BPTree();

    void deleteNodePage(unsigned int pageId);
    /**
     * @brief Inserts a key and its corresponding data pointer into the B+ Tree.
     * Starts the insertion process from the root. Handles root splits.
     * @param key The key value to insert.
     * @param pointer The RecordPointer pointing to the actual data row.
     */
    void insert(int key, RecordPointer pointer);

    /**
     * @brief Removes a specific key-pointer pair from the B+ Tree.
     * Starts the removal process from the root. Handles root changes (e.g., shrinking height).
     * @param key The key value to remove.
     * @param pointer The specific RecordPointer to remove (handles non-unique keys).
     */
    void remove(int key, RecordPointer pointer);

    /**
     * @brief Searches for a specific key and returns all associated RecordPointers.
     * @param key The key to search for.
     * @return A vector of RecordPointers matching the key. Empty if not found.
     */
    std::vector<RecordPointer> search(int key);

    /**
     * @brief Searches for keys within a given range [low, high] (inclusive).
     * @param low The lower bound of the key range.
     * @param high The upper bound of the key range.
     * @return A vector of RecordPointers for keys within the range.
     */
    std::vector<RecordPointer> searchRange(int low, int high);

    // --- Getters ---
    unsigned int getRootPageId() const { return rootPageId; }
    std::string getTableName() const { return tableName; }
    std::string getColumnName() const { return columnName; }
    BufferManager *getBufferManager() const { return bufferManager; }
    unsigned int getFanout() const { return fanout; }
    unsigned int getLeafCapacity() const { return leafCapacity; }

    // Note: persist() method removed; persistence is handled by the Table saving its metadata (indexRoots map).
};

#endif // INDEXING_H