/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_UPDATEVERTEXROCESSOR_H_
#define STORAGE_UPDATEVERTEXROCESSOR_H_

#include "storage/QueryBaseProcessor.h"
#include "dataman/RowReader.h"
#include "dataman/RowUpdater.h"

namespace nebula {
namespace storage {

struct KeyReaderPair {
    bool insert{false};
    std::string key;
    std::unique_ptr<RowReader> reader;
};

struct KeyUpdaterPair {
    std::string key;
    std::unique_ptr<RowUpdater> updater;
};

class UpdateVertexProcessor
    : public QueryBaseProcessor<cpp2::UpdateVertexRequest, cpp2::UpdateResponse> {
public:
    static UpdateVertexProcessor* instance(kvstore::KVStore* kvstore,
                                           meta::SchemaManager* schemaMan) {
        return new UpdateVertexProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::UpdateVertexRequest& req);

private:
    explicit UpdateVertexProcessor(kvstore::KVStore* kvstore,
                                   meta::SchemaManager* schemaMan)
        : QueryBaseProcessor<cpp2::UpdateVertexRequest,
                             cpp2::UpdateResponse>(kvstore, schemaMan) {}

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::UpdateVertexRequest& req);

    kvstore::ResultCode processVertex(PartitionID partId, VertexID vId) override;

    kvstore::ResultCode collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            const std::vector<PropContext>& props,
                            FilterContext* fcontext,
                            Collector* collector);

    std::vector<kvstore::KV> processData();

    void onProcessFinished(int32_t retNum) override;

private:
    int32_t                                                     returnColumnsNum_{0};
    std::vector<VariantType>                                    record_;
    storage::cpp2::UpdateRespData                               rowRespData_;
    std::vector<storage::cpp2::UpdateRespData>                  respData_;

    bool                                                        insertable_{false};
    std::vector<storage::cpp2::UpdateItem>                      updateItems_;
    std::set<TagID>                                             updateTagIDs_;
    std::unordered_map<TagID, std::unique_ptr<KeyReaderPair>>   tagReader_;
    std::unordered_map<TagID, std::unique_ptr<KeyUpdaterPair>>  pendingUpdater_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_UPDATEVERTEXROCESSOR_H_
