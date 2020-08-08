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
#include "storage/StorageFlags.h"

namespace nebula {
namespace storage {

class UpdateEdgeProcessor
    : public QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse> {
public:
    static UpdateEdgeProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan,
                                         meta::IndexManager* indexMan,
                                         stats::Stats* stats) {
        return new UpdateEdgeProcessor(kvstore, schemaMan, indexMan, stats);
    }

    void process(const cpp2::UpdateEdgeRequest& req);

private:
    explicit UpdateEdgeProcessor(kvstore::KVStore* kvstore,
                                 meta::SchemaManager* schemaMan,
                                 meta::IndexManager* indexMan,
                                 stats::Stats* stats)
        : QueryBaseProcessor<cpp2::UpdateEdgeRequest,
                             cpp2::UpdateResponse>(kvstore, schemaMan, stats)
        , indexMan_(indexMan) {}

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

    kvstore::ResultCode checkField(const std::string& edgeName, const std::string& propName,
                                   const meta::SchemaProviderIf* schema);

    kvstore::ResultCode checkAndGetDefault(const std::string& edgeName,
                                           const std::string& propNamem,
                                           const meta::SchemaProviderIf* schema);

    kvstore::ResultCode buildDependProps(const cpp2::EdgeKey& edgeKey,
                                         const meta::SchemaProviderIf* schema);

    kvstore::ResultCode collectEdgesProps(const PartitionID partId,
                                          const cpp2::EdgeKey& edgeKey);

    cpp2::ErrorCode checkFilter(const PartitionID partId, const cpp2::EdgeKey& edgeKey);

    folly::Optional<std::string> updateAndWriteBack(PartitionID partId,
                                                    const cpp2::EdgeKey& edgeKey);

private:
    bool                                                            insertable_{false};
    std::vector<storage::cpp2::UpdateItem>                          updateItems_;
    std::vector<std::unique_ptr<Expression>>                        returnColumnsExp_;
    std::unordered_map<std::pair<TagID, std::string>, VariantType>  tagFilters_;
    std::unordered_map<std::string, VariantType>                    edgeFilters_;
    std::string                                                     key_;
    std::string                                                     val_;
    std::unique_ptr<RowUpdater>                                     updater_;
    meta::IndexManager*                                             indexMan_{nullptr};
    std::vector<std::shared_ptr<nebula::cpp2::IndexItem>>           indexes_;
    std::atomic<cpp2::ErrorCode>                          filterResult_{cpp2::ErrorCode::SUCCEEDED};

    // Each UpdateItem in updateItems_ depend props in value expression
    // <edgename, propNmae> -> unordered_set<edgename,propNmae>
    using PropPair = std::pair<std::string, std::string>;
    std::vector<std::pair<PropPair, std::unordered_set<PropPair>>>  depPropMap_;


    // Checked props
    std::unordered_set<std::string>                                 checkedProp_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_MUTATE_UPDATEEDGEROCESSOR_H_
