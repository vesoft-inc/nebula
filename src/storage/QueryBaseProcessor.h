/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef STORAGE_QUERYBASEPROCESSOR_H_
#define STORAGE_QUERYBASEPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"
#include "storage/Collector.h"

namespace nebula {
namespace storage {


const std::unordered_map<std::string, PropContext::PropInKeyType> kPropsInKey_ = {
    {"_src", PropContext::PropInKeyType::SRC},
    {"_dst", PropContext::PropInKeyType::DST},
    {"_type", PropContext::PropInKeyType::TYPE},
    {"_rank", PropContext::PropInKeyType::RANK}
};

enum class BoundType {
    IN_BOUND,
    OUT_BOUND,
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
    virtual kvstore::ResultCode processVertex(PartitionID partID, VertexID vId) = 0;

    virtual void onProcessFinished(int32_t retNum) = 0;

protected:
    explicit QueryBaseProcessor(kvstore::KVStore* kvstore,
                                meta::SchemaManager* schemaMan,
                                folly::Executor* executor = nullptr,
                                BoundType type = BoundType::OUT_BOUND)
        : BaseProcessor<RESP>(kvstore, schemaMan)
        , type_(type)
        , executor_(executor) {}
    /**
     * Check whether current operation on the data is valid or not.
     * */
    bool validOperation(nebula::cpp2::SupportedType vType, cpp2::StatType statType);

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
                      Collector* collector);
    /**
     * Collect props for one vertex tag.
     * */
    kvstore::ResultCode collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            const std::vector<PropContext>& props,
                            Collector* collector);
    /**
     * Collect props for one vertex edge.
     * */
    kvstore::ResultCode collectEdgeProps(
                               PartitionID partId,
                               VertexID vId,
                               EdgeType edgeType,
                               const std::vector<PropContext>& props,
                               EdgeProcessor proc);

    std::vector<Bucket> genBuckets(const cpp2::GetNeighborsRequest& req);

    folly::Future<std::vector<OneVertexResp>> asyncProcessBucket(Bucket bucket);

    int32_t getBucketsNum(int32_t verticesNum, int handlerNum);

protected:
    GraphSpaceID  spaceId_;
    BoundType     type_;
    std::vector<TagContext> tagContexts_;
    EdgeContext edgeContext_;
    folly::Executor* executor_ = nullptr;
};

}  // namespace storage
}  // namespace nebula

#include "storage/QueryBaseProcessor.inl"

#endif  // STORAGE_QUERYBASEPROCESSOR_H_
