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
                         std::vector<PropContext>& props)>;

template<typename REQ, typename RESP>
class QueryBaseProcessor : public BaseProcessor<RESP> {
public:
    virtual ~QueryBaseProcessor() = default;

    void process(const cpp2::GetNeighborsRequest& req);

protected:
    explicit QueryBaseProcessor(kvstore::KVStore* kvstore, BoundType type = BoundType::OUT_BOUND)
        : BaseProcessor<RESP>(kvstore)
        , type_(type) {}
    /**
     * Check whether current operatin on the data is valid or not.
     * */
    bool validOperation(nebula::cpp2::SupportedType vType, cpp2::StatType statType);

    /**
     * Check request meta is illegal or not. Return contexts for tag and edge.
     * */
    cpp2::ErrorCode checkAndBuildContexts(const REQ& req,
                                          std::vector<TagContext>& tagContexts,
                                          EdgeContext& edgeContext);
    /**
     * collect props in one row, you could define custom behavior by implement your own collector.
     * */
    void collectProps(RowReader* reader,
                      folly::StringPiece key,
                      std::vector<PropContext>& props,
                      Collector* collector);

    virtual kvstore::ResultCode processVertex(PartitionID partID, VertexID vId,
                                              std::vector<TagContext>& tagContexts,
                                              EdgeContext& edgeContext) = 0;

    virtual void onProcessed(std::vector<TagContext>& tagContexts,
                             EdgeContext& edgeContext,
                             int32_t retNum) = 0;

    kvstore::ResultCode collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            std::vector<PropContext>& props,
                            Collector* collector);

    kvstore::ResultCode collectEdgeProps(
                               PartitionID partId,
                               VertexID vId,
                               EdgeType edgeType,
                               std::vector<PropContext>& props,
                               EdgeProcessor proc);

protected:
    GraphSpaceID  spaceId_;
    BoundType     type_;
};

}  // namespace storage
}  // namespace nebula

#include "storage/QueryBaseProcessor.inl"

#endif  // STORAGE_QUERYBASEPROCESSOR_H_
