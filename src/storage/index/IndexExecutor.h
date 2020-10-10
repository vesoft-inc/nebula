/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INDEXEXECUTOR_H
#define STORAGE_INDEXEXECUTOR_H

#include "stats/Stats.h"
#include "storage/index/IndexPolicyMaker.h"

namespace nebula {
namespace storage {

template<typename RESP>
class IndexExecutor : public BaseProcessor<RESP>
                    , public IndexPolicyMaker {
public:
    virtual ~IndexExecutor() = default;

protected:
    explicit IndexExecutor(kvstore::KVStore* kvstore,
                           meta::SchemaManager* schemaMan,
                           meta::IndexManager* indexMan,
                           stats::Stats* stats,
                           VertexCache* cache,
                           bool isEdgeIndex = false)
        : BaseProcessor<RESP>(kvstore, schemaMan, stats)
        , IndexPolicyMaker(schemaMan, indexMan)
        , vertexCache_(cache)
        , isEdgeIndex_(isEdgeIndex) {}

    void putResultCodes(cpp2::ErrorCode code, const std::vector<PartitionID>& parts) {
        for (auto& p : parts) {
            this->pushResultCode(code, p);
        }
        this->onFinished();
    }

    cpp2::ErrorCode prepareRequest(const cpp2::LookUpIndexRequest &req);

    /**
     * Details Prepare the index scan. logic as below :
     *         1, Trigger policy generator
     *         2, Build prefix string for first n columns of index.
     *         3, Collect information needed for index scanning,
     **/
    cpp2::ErrorCode buildExecutionPlan(const std::string& filter);

    std::pair<std::string, std::string> makeScanPair(PartitionID partId, IndexID indexId);

    std::pair<std::string, std::string>
    normalizeScanPair(const nebula::cpp2::ColumnDef& field, const ScanBound& item);

    /**
     * Details Scan index part as one by one.
     **/
    kvstore::ResultCode executeExecutionPlan(PartitionID part);

private:
    cpp2::ErrorCode checkIndex(IndexID indexId);

    cpp2::ErrorCode checkReturnColumns(const std::vector<std::string> &cols);

    kvstore::ResultCode getDataRow(PartitionID partId,
                                   const folly::StringPiece& key);

    kvstore::ResultCode getVertexRow(PartitionID partId,
                                     const folly::StringPiece& key,
                                     cpp2::VertexIndexData* data);

    kvstore::ResultCode getEdgeRow(PartitionID partId,
                                   const folly::StringPiece& key,
                                   cpp2::Edge* data);

    std::string getRowFromReader(RowReader* reader);

    bool conditionsCheck(const folly::StringPiece& key);

    OptVariantType decodeValue(const folly::StringPiece& key,
                               const folly::StringPiece& prop);

    folly::StringPiece getIndexVal(const folly::StringPiece& key,
                                   const folly::StringPiece& prop);

protected:
    GraphSpaceID                           spaceId_;
    VertexCache*                           vertexCache_{nullptr};
    std::shared_ptr<SchemaWriter>          schema_{nullptr};
    std::vector<cpp2::VertexIndexData>     vertexRows_;
    std::vector<cpp2::Edge>                edgeRows_;
    bool                                   isEdgeIndex_{false};

private:
    int                                                rowNum_{0};
    int32_t                                            tagOrEdge_;
    int32_t                                            vColNum_{0};
    std::vector<PropContext>                           props_;
    std::map<std::string, nebula::cpp2::SupportedType> indexCols_;
};

}  // namespace storage
}  // namespace nebula
#include "storage/index/IndexExecutor.inl"

#endif  // STORAGE_INDEXEXECUTOR_H
