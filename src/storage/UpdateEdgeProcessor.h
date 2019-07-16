/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_UPDATEEDGEROCESSOR_H_
#define STORAGE_UPDATEEDGEROCESSOR_H_

#include "storage/QueryBaseProcessor.h"
#include "dataman/RowReader.h"
#include "dataman/RowUpdater.h"

namespace nebula {
namespace storage {

class UpdateEdgeProcessor
    : public QueryBaseProcessor<cpp2::UpdateEdgeRequest, cpp2::UpdateResponse> {
public:
    static UpdateEdgeProcessor* instance(kvstore::KVStore* kvstore,
                                         meta::SchemaManager* schemaMan) {
        return new UpdateEdgeProcessor(kvstore, schemaMan);
    }

    void process(const cpp2::UpdateEdgeRequest& req);

private:
    explicit UpdateEdgeProcessor(kvstore::KVStore* kvstore,
                                 meta::SchemaManager* schemaMan)
        : QueryBaseProcessor<cpp2::UpdateEdgeRequest,
                             cpp2::UpdateResponse>(kvstore, schemaMan) {}

    kvstore::ResultCode processVertex(PartitionID partId, VertexID vId) override {
        UNUSED(partId);
        UNUSED(vId);
        LOG(FATAL) << "Unimplement!";
        return kvstore::ResultCode::SUCCEEDED;
    }

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::UpdateEdgeRequest& req);

    kvstore::ResultCode processEdge(PartitionID partId,
                                    const cpp2::EdgeKey& edgeKey);

    kvstore::ResultCode collectEdgesProps(PartitionID partId,
                                          const cpp2::EdgeKey& edgeKey,
                                          const std::vector<PropContext>& props,
                                          FilterContext* fcontext,
                                          Collector* collector);

    std::vector<kvstore::KV> processData();

    void onProcessFinished(int32_t retNum) override;

private:
    bool                                                    insertEdge_{false};
    int32_t                                                 returnColumnsNum_{0};
    std::vector<VariantType>                                record_;
    storage::cpp2::UpdateRespData                           rowRespData_;
    std::vector<storage::cpp2::UpdateRespData>              respData_;

    bool                                                    insertable_{false};
    std::vector<storage::cpp2::UpdateItem>                  updateItems_;
    std::string                                             key_;
    std::unique_ptr<RowReader>                              filterReader_;
    std::unique_ptr<RowReader>                              edgeReader_;
    std::unique_ptr<RowUpdater>                             updater_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_UPDATEEDGEROCESSOR_H_
