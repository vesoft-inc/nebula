/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef TOOLS_DBDUMPER_H_
#define TOOLS_DBDUMPER_H_

#include "base/Base.h"
#include <rocksdb/db.h>
#include "base/Status.h"
#include "meta/client/MetaClient.h"
#include "meta/ServerBasedSchemaManager.h"
#include "kvstore/RocksEngine.h"

DECLARE_string(db_path);
DECLARE_string(meta_server);
DECLARE_string(part);
DECLARE_string(scan);
DECLARE_string(tags);
DECLARE_string(edges);
DECLARE_int64(limit);
DECLARE_int32(load_data_interval_secs);

namespace nebula {
namespace storage {
class DbDumper {
public:
    DbDumper() = default;

    ~DbDumper() = default;

    Status init();

    void run();

private:
    Status initMeta();

    Status initSpace();

    Status initParams();

    Status openDb();

    void scan(kvstore::RocksPrefixIter* it);

    bool printTagKey(const folly::StringPiece& key);

    bool printEdgeKey(const folly::StringPiece& key);

    std::string getTagName(const TagID tagId);

    std::string getEdgeName(const EdgeType edgeType);

    void printValue(const folly::StringPiece& data,
                    const std::shared_ptr<const meta::SchemaProviderIf> schema);

private:
    std::unique_ptr<rocksdb::DB>                            db_;
    rocksdb::Options                                        options_;
    std::unique_ptr<meta::MetaClient>                       metaClient_;
    std::unique_ptr<meta::ServerBasedSchemaManager>         schemaMng_;
    GraphSpaceID                                            spaceId_;
    int32_t                                                 partNum_;
    std::unordered_set<PartitionID>                         parts_;
    std::unordered_set<VertexID>                            vids_;
    std::unordered_set<TagID>                               tags_;
    std::unordered_set<EdgeType>                            edges_;
    std::vector<std::function<bool(const folly::StringPiece&)>>    beforePrintVertex_;
    std::vector<std::function<bool(const folly::StringPiece&)>>    beforePrintEdge_;
    std::unordered_map<std::string, uint32_t>               tagCount_;
    std::unordered_map<std::string, uint32_t>               edgeCount_;
    int64_t                                                 count_{0};
};
}  // namespace storage
}  // namespace nebula
#endif  // TOOLS_DBDUMPER_H_
