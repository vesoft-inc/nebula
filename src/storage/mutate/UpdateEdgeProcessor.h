/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
#define STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_

#include "storage/query/QueryBaseProcessor.h"
#include "dataman/RowReader.h"
#include "dataman/RowUpdater.h"

namespace nebula {
namespace storage {

class UpdateEdgeProcessor
    : public QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse> {
public:
    static UpdateEdgeProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan,
                                         stats::Stats* stats) {
        return new UpdateEdgeProcessor(kvstore, schemaMan, stats);
    }

    void process(const cpp2::UpdateEdgeRequest& req);

private:
    explicit UpdateEdgeProcessor(kvstore::KVStore* kvstore,
                                 meta::SchemaManager* schemaMan,
                                 stats::Stats* stats)
        : QueryBaseProcessor<cpp2::UpdateEdgeRequest,
                             cpp2::UpdateResponse>(kvstore, schemaMan, stats) {}

    kvstore::ResultCode processVertex(PartitionID, VertexID) override {
        LOG(FATAL) << "Unimplement!";
        return kvstore::ResultCode::SUCCEEDED;
    }

    void onProcessFinished(int32_t retNum) override;

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req);

    kvstore::ResultCode collectVertexProps(
                            const PartitionID partId,
                            const VertexID vId,
                            const TagID tagId,
                            const std::vector<PropContext>& props);

    kvstore::ResultCode collectEdgesProps(const PartitionID partId,
                                          const cpp2::EdgeKey& edgeKey);

    bool checkFilter(const PartitionID partId, const cpp2::EdgeKey& edgeKey);

    std::string updateAndWriteBack();

private:
    bool                                                            insertable_{false};
    std::vector<storage::cpp2::UpdateItem>                          updateItems_;
    std::vector<std::unique_ptr<Expression>>                        returnColumnsExp_;
    std::unordered_map<std::pair<TagID, std::string>, VariantType>  tagFilters_;
    std::unordered_map<std::string, VariantType>                    edgeFilters_;
    std::string                                                     key_;
    std::unique_ptr<RowUpdater>                                     updater_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
