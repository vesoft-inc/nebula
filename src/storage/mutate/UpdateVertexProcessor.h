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
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

struct KeyUpdaterPair {
    std::pair<std::string, std::string> kv;
    std::unique_ptr<RowUpdater> updater;
};

class UpdateVertexProcessor
    : public QueryBaseProcessor<cpp2::UpdateVertexRequest, cpp2::UpdateResponse> {
public:
    static UpdateVertexProcessor* instance(kvstore::KVStore* kvstore,
                                           meta::SchemaManager* schemaMan,
                                           meta::IndexManager* indexMan,
                                           stats::Stats* stats,
                                           VertexCache* cache = nullptr) {
        return new UpdateVertexProcessor(kvstore, schemaMan, indexMan, stats, cache);
    }

    void process(const cpp2::UpdateVertexRequest& req);

private:
    explicit UpdateVertexProcessor(kvstore::KVStore* kvstore,
                                   meta::SchemaManager* schemaMan,
                                   meta::IndexManager* indexMan,
                                   stats::Stats* stats,
                                   VertexCache* cache)
        : QueryBaseProcessor<cpp2::UpdateVertexRequest,
                             cpp2::UpdateResponse>(kvstore, schemaMan, stats, nullptr, cache)
        , indexMan_(indexMan) {}

    kvstore::ResultCode processVertex(BucketIdx, PartitionID, VertexID) override {
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

    cpp2::ErrorCode checkFilter(const PartitionID partId, const VertexID vId);

    folly::Optional<std::string> updateAndWriteBack(const PartitionID partId, const VertexID vId);

private:
    bool                                                            insertable_{false};
    std::vector<storage::cpp2::UpdateItem>                          updateItems_;
    std::vector<std::unique_ptr<Expression>>                        returnColumnsExp_;
    std::set<TagID>                                                 updateTagIds_;
    std::unordered_map<std::pair<TagID, std::string>, VariantType>  tagFilters_;
    std::unordered_map<TagID, std::unique_ptr<KeyUpdaterPair>>      tagUpdaters_;
    meta::IndexManager*                                             indexMan_{nullptr};
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>           indexes_;
    std::atomic<cpp2::ErrorCode>                          filterResult_{cpp2::ErrorCode::SUCCEEDED};
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_UPDATEVERTEXROCESSOR_H_
