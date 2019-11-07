/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERYBASEPROCESSOR_H_
#define STORAGE_QUERYBASEPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/Collector.h"
#include "filter/Expressions.h"
#include "storage/CommonUtils.h"

namespace nebula {
namespace storage {

const std::unordered_map<std::string, PropContext::PropInKeyType> kPropsInKey_ = {
    {"_src", PropContext::PropInKeyType::SRC},
    {"_dst", PropContext::PropInKeyType::DST},
    {"_type", PropContext::PropInKeyType::TYPE},
    {"_rank", PropContext::PropInKeyType::RANK}
};

using EdgeProcessor
    = std::function<void(RowReader* reader,
                         folly::StringPiece key,
                         const std::vector<PropContext>& props)>;
struct Bucket {
    std::vector<std::pair<PartitionID, VertexID>> vertices_;
};

using OneVertexResp = std::tuple<PartitionID, VertexID, kvstore::ResultCode>;

template<typename REQ, typename RESP>
class QueryBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~QueryBaseProcessor() = default;

    void process(const cpp2::GetNeighborsRequest& req);

protected:
    explicit QueryBaseProcessor(kvstore::KVStore* kvstore,
                                meta::SchemaManager* schemaMan,
                                StorageStats* stats,
                                folly::Executor* executor = nullptr)
        : BaseProcessor<RESP>(kvstore, schemaMan, stats)
        , executor_(executor), stop_(false) {}

    /**
     * Check whether current operation on the data is valid or not.
     * */
    bool validOperation(nebula::cpp2::SupportedType vType, cpp2::StatType statType);

    void addDefaultProps(std::vector<PropContext>& p, EdgeType eType);
    /**
     * init edge context
     **/
    void initEdgeContext(const std::vector<EdgeType> &eTypes, bool need_default_props = false);

    /**
     * Check request meta is illegal or not and build contexts for tag and edge.
     * */
    cpp2::ErrorCode checkAndBuildContexts(const REQ& req);

    /**
     * collect props in one row, you could define custom behavior by implement your own collector.
     * */
    void collectProps(RowReader* reader,
                      folly::StringPiece key,
                      const std::vector<PropContext>& props,
                      FilterContext* fcontext,
                      Collector* collector);

    virtual kvstore::ResultCode processVertex(PartitionID partId, VertexID vId) = 0;

    virtual void onProcessFinished(int32_t retNum) = 0;

    /**
     * Collect props for one vertex tag.
     * */
    kvstore::ResultCode collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            const std::vector<PropContext>& props,
                            FilterContext* fcontext,
                            Collector* collector);
    /**
     * Collect props for one vertex edge.
     * */
    kvstore::ResultCode collectEdgeProps(
                               PartitionID partId,
                               VertexID vId,
                               EdgeType edgeType,
                               const std::vector<PropContext>& props,
                               FilterContext* fcontext,
                               EdgeProcessor proc);

    std::vector<Bucket> genBuckets(const cpp2::GetNeighborsRequest& req);

    folly::Future<std::vector<OneVertexResp>> asyncProcessBucket(Bucket bucket);

    int32_t getBucketsNum(int32_t verticesNum, int32_t minVerticesPerBucket, int32_t handlerNum);

    bool checkExp(const Expression* exp);

protected:
    GraphSpaceID  spaceId_;
    std::unique_ptr<ExpressionContext> expCtx_;
    std::unique_ptr<Expression> exp_;
    std::vector<TagContext> tagContexts_;
    std::unordered_map<EdgeType, std::vector<PropContext>> edgeContexts_;
    folly::Executor* executor_ = nullptr;
    bool                    stop_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/QueryBaseProcessor.inl"

#endif  // STORAGE_QUERYBASEPROCESSOR_H_
