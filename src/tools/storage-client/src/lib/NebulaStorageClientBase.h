/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_STORAGE_CLIENT__BASE_H_
#define NEBULA_STORAGE_CLIENT__BASE_H_

#include "storage/client/StorageClient.h"
#include "tools/storage-client/src/NebulaStorageClient.h"
#include "dataman/ResultSchemaProvider.h"
#include "dataman/RowReader.h"
#include "common/filter/Expressions.h"
#include "parser/Clauses.h"

namespace nebula {

using QueryResultRow = std::unordered_map<std::pair<int32_t, std::string>, Val>;

class NebulaStorageClientBase {
public:
    NebulaStorageClientBase(storage::StorageClient *storage,
                            meta::MetaClient *metaClient,
                            folly::IOThreadPoolExecutor *ioExecutor,
                            GraphSpaceID spaceId)
     : storageClient_(storage)
     , metaClient_(metaClient)
     , ioExecutor_(ioExecutor)
     , spaceId_(spaceId) {}

protected:
    IndexID getIndexId(bool isEdge, const std::string& name);

    int32_t getSchemaIdFromIndex(bool isEdge, IndexID indexId);

    std::shared_ptr<const meta::SchemaProviderIf>
    getLatestVersionSchema(bool isEdge, int32_t edgeOrTagId);

    int32_t getSchemaIdByName(bool isEdge, const std::string& name);

    std::shared_ptr<const meta::SchemaProviderIf>
    getSchemaByName(bool isEdge, const std::string& name);

    DataType
    getColumnTypeBySchema(bool isEdge, const std::string& schema, const std::string& column);

    DataType toClientType(cpp2::SupportedType type);

    void writeEdgeIndexResult(const nebula::storage::cpp2::Edge& edge,
                              std::shared_ptr<const meta::SchemaProviderIf> schema,
                              ResultSet& resultSet);

    void writeTagIndexResult(const nebula::storage::cpp2::VertexIndexData& vertexIndexData,
                             std::shared_ptr<const meta::SchemaProviderIf> schema,
                             ResultSet& resultSet);

    void writeQueryResult(const storage::cpp2::QueryResponse& resp, ResultSet& resultSet);

    void writeVertexPropResult(const storage::cpp2::QueryResponse& resp, ResultSet& resultSet);

    storage::cpp2::PropOwner toPropOwner(PropOwner owner);

    Val getValueFromVariant(const VariantType& v);

    nebula::ResultCode toResultCode(nebula::storage::cpp2::ErrorCode code);

private:
    QueryResultRow initEmptyResultRow(const std::vector<ColumnDef>& cols);

    void writeQueryResultCell(const std::string& val, int32_t id, QueryResultRow& row,
                              const std::unordered_map<int32_t,
                              std::shared_ptr<ResultSchemaProvider>>& schema);

    void clearResultRow(QueryResultRow& row);

    void insertRow(const QueryResultRow& row, ResultSet& resultSet);

protected:
    storage::StorageClient                     *storageClient_{nullptr};
    meta::MetaClient                           *metaClient_{nullptr};
    folly::IOThreadPoolExecutor                *ioExecutor_{nullptr};
    GraphSpaceID                               spaceId_{0};
    std::map<std::string, EdgeType>            edges_;
    std::map<std::string, TagID>               tags_;
    std::string                                targetSchemaName_;
};
}   // namespace nebula

#endif   // NEBULA_STORAGE_CLIENT__BASE_H_
