/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#ifndef STORAGE_SCANINDEXBASEPROCESSOR_H
#define STORAGE_SCANINDEXBASEPROCESSOR_H

#include "base/Base.h"
#include "storage/BaseProcessor.h"
#include "stats/Stats.h"

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
class ScanIndexBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~ScanIndexBaseProcessor() = default;

protected:
    explicit ScanIndexBaseProcessor(kvstore::KVStore* kvstore,
                                    meta::SchemaManager* schemaMan,
                                    stats::Stats* stats,
                                    folly::Executor* executor = nullptr,
                                    VertexCache* cache = nullptr)
            : BaseProcessor<RESP>(kvstore, schemaMan, stats)
            , executor_(executor)
            , vertexCache_(cache) {}

    cpp2::ErrorCode buildIndexHint();

    cpp2::ErrorCode createResultSchema(bool isEdge, int32_t id,
                                       const std::vector<std::string> &returnCols);

    kvstore::ResultCode getVertexRow(PartitionID partId, TagID tagId,
                                     const folly::StringPiece& key,
                                     cpp2::VertexIndexData* data);

    kvstore::ResultCode getEdgeRow(PartitionID partId,
                                   EdgeType edgeType,
                                   const folly::StringPiece& key,
                                   cpp2::Edge* data);

    std::string getRowFromReader(RowReader* reader);

    bool checkDataValidity(bool isEdge, const folly::StringPiece& key);

protected:
    GraphSpaceID                           spaceId_;
    int64_t                                vlColNum_{0};
    bool                                   isRangeScan_;
    std::string                            prefix_;
    std::pair<std::string, std::string>    range_;
    cpp2::IndexItem                        index_;
    std::vector<cpp2::IndexHint>           hints_;
    std::shared_ptr<SchemaWriter>          schema_;
    folly::Executor*                       executor_ = nullptr;
    VertexCache*                           vertexCache_ = nullptr;
    std::vector<cpp2::VertexIndexData>     vertexRows_;
    std::vector<cpp2::Edge>                edgeRows_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/ScanIndexBaseProcessor.inl"


#endif  // STORAGE_SCANINDEXBASEPROCESSOR_H

