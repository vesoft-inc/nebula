/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef EXEC_STORAGECACHE_H_
#define EXEC_STORAGECACHE_H_

#include "base/Base.h"
#include "base/StatusOr.h"
#include "interface/gen-cpp2/storage_types.h"
#include "clients/meta/MetaClient.h"
#include "meta/ServerBasedSchemaManager.h"

namespace nebula {
namespace graph {

class StorageCache final {
public:
    explicit StorageCache(uint16_t metaPort);

    ~StorageCache() = default;

    Status addVertices(const storage::cpp2::AddVerticesRequest& req);

    Status addEdges(const storage::cpp2::AddEdgesRequest& req);

    StatusOr<std::vector<DataSet>> getProps(const storage::cpp2::GetPropRequest&);

private:
    std::string getEdgeKey(VertexID srcId,
                           EdgeType type,
                           EdgeRanking rank,
                           VertexID dstId) {
        std::string key;
        key.reserve(srcId.size() + sizeof(EdgeType) + sizeof(EdgeRanking) + dstId.size());
        key.append(srcId.data(), srcId.size())
           .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType))
           .append(reinterpret_cast<const char*>(&rank), sizeof(EdgeRanking))
           .append(dstId.data(), dstId.size());
        return key;
    }

    std::string srcEdgePrefix(VertexID srcId,
                              EdgeType type) {
        std::string key;
        key.reserve(srcId.size() + sizeof(EdgeType));
        key.append(srcId.data(), srcId.size())
           .append(reinterpret_cast<const char*>(&type), sizeof(EdgeType));
        return key;
    }

    std::vector<std::string> getTagPropNamesFromCache(const GraphSpaceID spaceId,
                                                      const TagID tagId);

    std::vector<std::string> getEdgePropNamesFromCache(const GraphSpaceID spaceId,
                                                       const EdgeType edgeType);

private:
    struct PropertyInfo {
        std::vector<std::string>                   propNames;
        std::vector<Value>                         propValues;
        std::unordered_map<std::string, int32_t>   propIndexes;
    };

    struct SpaceDataInfo {
        std::unordered_map<VertexID, std::unordered_map<TagID, PropertyInfo>>        vertices;
        std::unordered_map<std::string, PropertyInfo>                                edges;
    };

    std::unordered_map<GraphSpaceID, SpaceDataInfo>   cache_;
    mutable folly::RWSpinLock                         lock_;
    std::unique_ptr<meta::MetaClient>                 metaClient_;
    std::unique_ptr<meta::ServerBasedSchemaManager>   mgr_;
};

}  // namespace graph
}  // namespace nebula
#endif  // EXEC_STORAGECACHE_H_
