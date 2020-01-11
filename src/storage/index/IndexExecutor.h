/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_INDEXEXECUTOR_H
#define STORAGE_INDEXEXECUTOR_H

<<<<<<< HEAD
=======
#include "base/Base.h"
#include "storage/BaseProcessor.h"
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
#include "stats/Stats.h"
#include "storage/index/IndexPolicyMaker.h"

namespace nebula {
namespace storage {

template<typename RESP>
<<<<<<< HEAD
class IndexExecutor : public BaseProcessor<RESP>
                    , public IndexPolicyMaker {
=======
class IndexExecutor : public BaseProcessor<RESP>,
                      public IndexPolicyMaker {
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
public:
    virtual ~IndexExecutor() = default;

protected:
    explicit IndexExecutor(kvstore::KVStore* kvstore,
                           meta::SchemaManager* schemaMan,
<<<<<<< HEAD
                           meta::IndexManager* indexMan,
=======
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
                           stats::Stats* stats,
                           VertexCache* cache,
                           bool isEdgeIndex = false)
        : BaseProcessor<RESP>(kvstore, schemaMan, stats)
<<<<<<< HEAD
        , IndexPolicyMaker(schemaMan, indexMan)
=======
        , IndexPolicyMaker(schemaMan)
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
        , vertexCache_(cache)
        , isEdgeIndex_(isEdgeIndex) {}

    void putResultCodes(cpp2::ErrorCode code, const std::vector<PartitionID>& parts) {
        for (auto& p : parts) {
            this->pushResultCode(code, p);
        }
        this->onFinished();
    }

<<<<<<< HEAD
    cpp2::ErrorCode prepareRequest(const cpp2::LookUpIndexRequest &req);

    /**
     * Details Prepare the index scan. logic as below :
     *         1, Trigger policy generator
     *         2, Build prefix string for first n columns of index.
     *         3, Collect information needed for index scanning,
     **/
    cpp2::ErrorCode buildExecutionPlan(const std::string& filter);

    /**
     * Details Scan index part as one by one.
     **/
    kvstore::ResultCode executeExecutionPlan(PartitionID part);
=======
    cpp2::ErrorCode prepareScan(const cpp2::LookUpIndexRequest& req);

    kvstore::ResultCode performScan(PartitionID part);
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.

private:
    cpp2::ErrorCode checkIndex(IndexID indexId);

<<<<<<< HEAD
    cpp2::ErrorCode checkReturnColumns(const std::vector<std::string> &cols);
=======
    cpp2::ErrorCode createResultSchema(const std::vector<std::string> &cols);

    cpp2::ErrorCode preparePrefix();

    kvstore::ResultCode accurateScan(PartitionID part);

    kvstore::ResultCode prefixScan(PartitionID part);

    kvstore::ResultCode seekScan(PartitionID part);
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.

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
<<<<<<< HEAD

private:
    int                                    rowNum_{0};
    int32_t                                tagOrEdge_;
    bool                                   isEdgeIndex_{false};
    int32_t                                vColNum_{0};
    std::vector<PropContext>               props_;
=======
    bool                                   needReturnCols_{true};

private:
    int                                    rowCount_{0};
    int32_t                                tagOrEdge_;
    bool                                   isEdgeIndex_{false};
    std::string                            prefix_;
    int32_t                                vColSize_{0};
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
    std::map<std::string, nebula::cpp2::SupportedType> indexCols_;
};

}  // namespace storage
}  // namespace nebula
#include "storage/index/IndexExecutor.inl"

#endif  // STORAGE_INDEXEXECUTOR_H
