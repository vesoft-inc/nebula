/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_QUERYBASEPROCESSOR_H_
#define STORAGE_QUERY_QUERYBASEPROCESSOR_H_

#include "base/Base.h"
#include <folly/Optional.h>
#include "storage/BaseProcessor.h"
#include "storage/Collector.h"
#include "filter/Expressions.h"
#include "storage/CommonUtils.h"
#include "stats/Stats.h"
#include <random>

namespace nebula {
namespace storage {

const std::unordered_map<std::string, PropContext::PropInKeyType> kPropsInKey_ = {
    {"_src", PropContext::PropInKeyType::SRC},
    {"_dst", PropContext::PropInKeyType::DST},
    {"_type", PropContext::PropInKeyType::TYPE},
    {"_rank", PropContext::PropInKeyType::RANK}
};

using EdgeProcessor = std::function<void(RowReader reader, folly::StringPiece key)>;
struct Bucket {
    std::vector<std::pair<PartitionID, VertexID>> vertices_;
};

using BucketIdx = unsigned;

using OneVertexResp = std::tuple<PartitionID, VertexID, kvstore::ResultCode>;

template<typename REQ, typename RESP>
class QueryBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~QueryBaseProcessor() = default;

    void process(const cpp2::GetNeighborsRequest& req);

protected:
    explicit QueryBaseProcessor(kvstore::KVStore* kvstore,
                                meta::SchemaManager* schemaMan,
                                stats::Stats* stats,
                                folly::Executor* executor = nullptr,
                                VertexCache* cache = nullptr)
        : BaseProcessor<RESP>(kvstore, schemaMan, stats)
        , executor_(executor)
        , vertexCache_(cache) {}

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

    virtual void beforeProcess(const std::vector<Bucket>& buckets) {
        UNUSED(buckets);
    }

    virtual kvstore::ResultCode processVertex(
        BucketIdx bucketIdx, PartitionID partId, VertexID vId) = 0;

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
     * Collect props for one vertex with vid.
     * */
    kvstore::ResultCode collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            std::vector<cpp2::TagData> &tds);
    /**
     * Collect props for one vertex edge.
     * */
    kvstore::ResultCode collectEdgeProps(
                               PartitionID partId,
                               VertexID vId,
                               EdgeType edgeType,
                               FilterContext* fcontext,
                               EdgeProcessor proc);

    std::vector<Bucket> genBuckets(const cpp2::GetNeighborsRequest& req);

    folly::Future<std::vector<OneVertexResp>> asyncProcessBucket(
        BucketIdx bucketIdx, Bucket bucket);

    int32_t getBucketsNum(int32_t verticesNum, int32_t minVerticesPerBucket, int32_t handlerNum);

    bool checkExp(const Expression* exp);

    void buildTTLInfoAndRespSchema();

    folly::Optional<std::pair<std::string, int64_t>> getTagTTLInfo(TagID tagId);

    folly::Optional<std::pair<std::string, int64_t>> getEdgeTTLInfo(EdgeType edgeType);

protected:
    GraphSpaceID  spaceId_;
    std::unique_ptr<ExpressionContext> expCtx_;
    std::unique_ptr<Expression> exp_;
    std::vector<TagContext> tagContexts_;
    std::unordered_map<EdgeType, std::vector<PropContext>> edgeContexts_;

    std::unordered_map<TagID, nebula::cpp2::Schema> vertexSchemaResp_;
    std::unordered_map<EdgeType, nebula::cpp2::Schema> edgeSchemaResp_;

    std::unordered_map<TagID, std::shared_ptr<nebula::meta::SchemaProviderIf>> vertexSchema_;
    std::unordered_map<EdgeType, std::shared_ptr<nebula::meta::SchemaProviderIf>> edgeSchema_;

    std::unordered_map<EdgeType, bool> onlyStructures_;

    folly::Executor* executor_{nullptr};
    VertexCache* vertexCache_{nullptr};
    std::unordered_map<std::string, EdgeType> edgeMap_;
    bool compactDstIdProps_ = false;

    std::unordered_map<EdgeType, std::pair<std::string, int64_t>> edgeTTLInfo_;

    std::unordered_map<TagID, std::pair<std::string, int64_t>> tagTTLInfo_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/query/QueryBaseProcessor.inl"

#endif  // STORAGE_QUERY_QUERYBASEPROCESSOR_H_
