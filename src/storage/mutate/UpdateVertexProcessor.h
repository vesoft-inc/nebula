/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_MUTATE_UPDATEVERTEXROCESSOR_H_
#define STORAGE_MUTATE_UPDATEVERTEXROCESSOR_H_

#include "storage/query/QueryBaseProcessor.h"
#include "dataman/RowReader.h"
#include "dataman/RowUpdater.h"

namespace nebula {
namespace storage {

struct KeyUpdaterPair {
    std::string key;
    std::unique_ptr<RowUpdater> updater;
};

class UpdateVertexProcessor
    : public QueryBaseProcessor<cpp2::UpdateVertexRequest, cpp2::UpdateResponse> {
public:
    static UpdateVertexProcessor* instance(kvstore::KVStore* kvstore,
                                           meta::SchemaManager* schemaMan,
                                           stats::Stats* stats,
                                           VertexCache* cache = nullptr) {
        return new UpdateVertexProcessor(kvstore, schemaMan, stats, cache);
    }

    void process(const cpp2::UpdateVertexRequest& req);

private:
    explicit UpdateVertexProcessor(kvstore::KVStore* kvstore,
                                   meta::SchemaManager* schemaMan,
                                   stats::Stats* stats,
                                   VertexCache* cache)
        : QueryBaseProcessor<cpp2::UpdateVertexRequest,
                             cpp2::UpdateResponse>(kvstore, schemaMan, stats, nullptr, cache) {}

    kvstore::ResultCode processVertex(PartitionID, VertexID) override {
        LOG(FATAL) << "Unimplement!";
        return kvstore::ResultCode::SUCCEEDED;
    }

    void onProcessFinished(int32_t retNum) override;

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::UpdateVertexRequest& req);

    kvstore::ResultCode collectVertexProps(
                            const PartitionID partId,
                            const VertexID vId,
                            const TagID tagId,
                            const std::vector<PropContext>& props);

    bool checkFilter(const PartitionID partId, const VertexID vId);

    std::string updateAndWriteBack();

private:
    bool                                                            insertable_{false};
    std::vector<storage::cpp2::UpdateItem>                          updateItems_;
    std::vector<std::unique_ptr<Expression>>                        returnColumnsExp_;
    std::set<TagID>                                                 updateTagIds_;
    std::unordered_map<std::pair<TagID, std::string>, VariantType>  tagFilters_;
    std::unordered_map<TagID, std::unique_ptr<KeyUpdaterPair>>      tagUpdaters_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_UPDATEVERTEXROCESSOR_H_
