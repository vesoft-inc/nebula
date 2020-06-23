/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_STORAGE_CLIENT_STORAGECLIENT_H_
#define NEBULA_STORAGE_CLIENT_STORAGECLIENT_H_

#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "NebulaStorageClientValue.h"

namespace nebula {

using VertexID = int64_t;
using EdgeType = int32_t;
using Val = nebula::storage::client::value::Value;

enum class DataType {
    UNKNOWN = 0,
    BOOL = 1,
    INT = 2,
    VID = 3,
    FLOAT = 4,
    DOUBLE = 5,
    STRING = 6,
    TIMESTAMP = 7,
};

enum class PropOwner {
    EDGE = 1,
    TAG = 2,
};

struct ColumnDef {
    ColumnDef(PropOwner owner, std::string edgeOrTag, std::string column)
        : owner_(owner)
        , edgeOrTag_(std::move(edgeOrTag))
        , column_(std::move(column)) {}

    ColumnDef(PropOwner owner, std::string column)
        : owner_(owner)
        , column_(std::move(column)) {}

    PropOwner owner_;
    std::string edgeOrTag_{""};
    std::string column_{""};
};

struct PropDef {
    PropDef(PropOwner owner, std::string schemaName, std::string propName)
        : owner_(owner)
        , schemaName_(std::move(schemaName))
        , propName_(std::move(propName)) {}
    PropOwner      owner_;
    std::string    schemaName_;
    std::string    propName_;
};

struct ResultSet {
    std::vector<ColumnDef>            returnCols;
    std::vector<DataType>             columnsType;
    std::vector<std::vector<Val>>     rows;
};

struct NeighborsContext{
    NeighborsContext(std::vector<VertexID>    vertices,
                     std::vector<std::string> edges,
                     std::string              filter,
                     std::vector<PropDef>     returns)
         : vertices_(std::move(vertices))
         , edges_(std::move(edges))
         , filter_(std::move(filter))
         , returns_(std::move(returns)) {}
    std::vector<VertexID>    vertices_;
    std::vector<std::string> edges_;
    std::string              filter_;
    std::vector<PropDef>     returns_;
};

struct LookupContext {
    LookupContext(std::string index,
                  std::string filter,
                  std::vector<std::string> returnCols,
                  bool isEdge = false)
        : index_(std::move(index))
        , filter_(std::move(filter))
        , returnCols_(std::move(returnCols))
        , isEdge_(isEdge) {}

    std::string              index_{};
    std::string              filter_;
    std::vector<std::string> returnCols_;
    bool                     isEdge_{false};
};

struct VertexPropsContext {
    VertexPropsContext(std::vector<VertexID> ids,
                       std::vector<PropDef>  returns)
       : ids_(std::move(ids))
       , returns_(std::move(returns)) {}
    std::vector<VertexID>   ids_;
    std::vector<PropDef>    returns_;
};

enum ResultCode {
    SUCCEEDED = 0,

    // RPC Failure
    E_DISCONNECTED = -1,
    E_FAILED_TO_CONNECT = -2,
    E_RPC_FAILURE = -3,

    // storage failures
    E_LEADER_CHANGED = -11,
    E_KEY_HAS_EXISTS = -12,
    E_SPACE_NOT_FOUND = -13,
    E_PART_NOT_FOUND = -14,
    E_KEY_NOT_FOUND = -15,
    E_CONSENSUS_ERROR = -16,

    // meta failures
    E_EDGE_PROP_NOT_FOUND = -21,
    E_TAG_PROP_NOT_FOUND = -22,
    E_IMPROPER_DATA_TYPE = -23,
    E_EDGE_NOT_FOUND = -24,
    E_TAG_NOT_FOUND = -25,
    E_INDEX_NOT_FOUND = -26,

    // Invalid request
    E_INVALID_FILTER = -31,
    E_INVALID_UPDATER = -32,
    E_INVALID_STORE = -33,
    E_INVALID_PEER  = -34,
    E_RETRY_EXHAUSTED = -35,
    E_TRANSFER_LEADER_FAILED = -36,

    // meta client failed
    E_LOAD_META_FAILED = -41,

    // checkpoint failed
    E_FAILED_TO_CHECKPOINT = -50,
    E_CHECKPOINT_BLOCKED = -51,

    // Filter out
    E_FILTER_OUT         = -60,

    // partial result, used for kv interfaces
    E_PARTIAL_RESULT = -99,

    E_UNKNOWN = -100,

    // storage client failed
    E_DIRECTORY_NOT_FOUND = -200,
    E_OPEN_FILE_ERROR = -201
};

class NebulaStorageClientImpl;

class NebulaStorageClient {
public:
    explicit NebulaStorageClient(std::string  metaAddr);

    ~NebulaStorageClient();

    ResultCode initArgs(int argc, char** argv);

    ResultCode setLogging(const std::string& path);

    ResultCode init(const std::string& spaceName, int32_t ioHandlers = 1);

    ResultCode lookup(const LookupContext& input, ResultSet& result);

    ResultCode getNeighbors(const NeighborsContext& input, ResultSet& result);

    ResultCode getVertexProps(const VertexPropsContext& input, ResultSet& result);

private:
    std::string metaAddr_;
    NebulaStorageClientImpl* client_ = nullptr;
};

}   // namespace nebula


#endif  // NEBULA_STORAGE_CLIENT_STORAGECLIENT_H_
