/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INDEXEXECUTOR_H
#define STORAGE_INDEXEXECUTOR_H

#include "stats/Stats.h"
#include "storage/BaseProcessor.h"
#include "storage/index/ExecutionPlan.h"

namespace nebula {
namespace storage {

template<typename RESP>
class IndexExecutor : public BaseProcessor<RESP> {
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
        , indexMan_(indexMan)
        , vertexCache_(cache)
        , isEdgeIndex_(isEdgeIndex) {}

    void putResultCodes(cpp2::ErrorCode code, const std::vector<PartitionID>& parts) {
        for (auto& p : parts) {
            this->pushResultCode(code, p);
        }
        this->onFinished();
    }

    cpp2::ErrorCode prepareRequest(const cpp2::LookUpIndexRequest &req);

    cpp2::ErrorCode buildExecutionPlan(const nebula::cpp2::Hint& hint);

    /**
     * Details Scan index or data part as one by one.
     **/
    kvstore::ResultCode executeExecutionPlan(PartitionID part);

private:
    kvstore::ResultCode executeIndexScan(int32_t hintId, PartitionID part);

    kvstore::ResultCode executeIndexRangeScan(int32_t hintId, PartitionID part);

    kvstore::ResultCode executeIndexPrefixScan(int32_t hintId, PartitionID part);

    kvstore::ResultCode executeDataScan(int32_t hintId, PartitionID part);

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

    bool conditionsCheck(int32_t hintId, const folly::StringPiece& key);

    /**
     * Details Evaluate filter conditions.
     */
    bool exprEval(int32_t hintId, Getters &getters);

    OptVariantType decodeValue(int32_t hintId,
                               const folly::StringPiece& key,
                               const folly::StringPiece& prop);

    folly::StringPiece getIndexVal(int32_t hintId,
                                   const folly::StringPiece& key,
                                   const folly::StringPiece& prop);

protected:
    GraphSpaceID                           spaceId_;
    meta::IndexManager*                    indexMan_;
    VertexCache*                           vertexCache_{nullptr};
    std::vector<cpp2::VertexIndexData>     vertexRows_;
    std::vector<cpp2::Edge>                edgeRows_;
    std::shared_ptr<SchemaWriter>          resultSchema_{nullptr};

private:
    int                                    rowNum_{0};
    int32_t                                tagOrEdgeId_{-1};
    bool                                   isEdgeIndex_{false};
    int32_t                                vColNum_{0};
    std::vector<PropContext>               props_;
    std::map<int32_t, std::unique_ptr<ExecutionPlan>>       executionPlans_;
//    std::shared_ptr<const meta::SchemaProviderIf>   schema_;
};

}  // namespace storage
}  // namespace nebula
#include "storage/index/IndexExecutor.inl"

#endif  // STORAGE_INDEXEXECUTOR_H
