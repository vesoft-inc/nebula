/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef TOOLS_DBDUMP_DBDUMPER_H_
#define TOOLS_DBDUMP_DBDUMPER_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/clients/meta/MetaClient.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include <rocksdb/db.h>
#include "kvstore/RocksEngine.h"
#include "codec/RowReaderWrapper.h"

DECLARE_string(space_name);
DECLARE_string(db_path);
DECLARE_string(meta_server);
DECLARE_string(parts);
DECLARE_string(mode);
DECLARE_string(vids);
DECLARE_string(tags);
DECLARE_string(edges);
DECLARE_int64(limit);

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

    void seekToFirst();

    void seek(std::string& prefix);

    void iterates(kvstore::RocksPrefixIter* it);

    inline void printTagKey(const folly::StringPiece& key);

    inline void printEdgeKey(const folly::StringPiece& key);

    std::string getTagName(const TagID tagId);

    std::string getEdgeName(const EdgeType edgeType);

    void printValue(const RowReader* reader);

    bool isValidVidLen(VertexID vid);

    Value getVertexId(const folly::StringPiece &vidStr);

private:
    std::unique_ptr<rocksdb::DB>                                   db_;
    rocksdb::Options                                               options_;
    std::unique_ptr<meta::MetaClient>                              metaClient_;
    std::unique_ptr<meta::ServerBasedSchemaManager>                schemaMng_;
    GraphSpaceID                                                   spaceId_;
    int32_t                                                        spaceVidLen_;
    meta::cpp2::PropertyType                                       spaceVidType_;
    int32_t                                                        partNum_;
    std::unordered_set<PartitionID>                                parts_;
    std::unordered_set<VertexID>                                   vids_;
    std::unordered_set<TagID>                                      tagIds_;
    std::unordered_set<EdgeType>                                   edgeTypes_;
    std::vector<std::function<bool(const folly::StringPiece&)>>    beforePrintVertex_;
    std::vector<std::function<bool(const folly::StringPiece&)>>    beforePrintEdge_;

    // For statistics
    std::unordered_map<TagID, uint32_t>                            tagStat_;
    std::unordered_map<EdgeType, uint32_t>                         edgeStat_;
    int64_t                                                        count_{0};
    int64_t                                                        vertexCount_{0};
    int64_t                                                        edgeCount_{0};
};

}  // namespace storage
}  // namespace nebula
#endif  // TOOLS_DBDUMP_DBDUMPER_H_
