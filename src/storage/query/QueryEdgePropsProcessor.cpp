/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/query/QueryEdgePropsProcessor.h"
#include "base/NebulaKeyUtils.h"
#include <algorithm>
#include "time/Duration.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

kvstore::ResultCode QueryEdgePropsProcessor::collectEdgesProps(
                                       PartitionID partId,
                                       const cpp2::EdgeKey& edgeKey,
                                       std::vector<PropContext>& props,
                                       RowSetWriter& rsWriter) {
    auto prefix = NebulaKeyUtils::prefix(partId, edgeKey.src, edgeKey.edge_type,
                                         edgeKey.ranking, edgeKey.dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (kvstore::ResultCode::SUCCEEDED != ret) {
        return ret;
    }

    // Only use the latest version.
    if (iter && iter->valid()) {
        RowWriter writer(rsWriter.schema());
        PropsCollector collector(&writer);
        auto reader = RowReader::getEdgePropReader(schemaMan_,
                                                   iter->val(),
                                                   spaceId_,
                                                   edgeKey.edge_type);
        this->collectProps(reader.get(), iter->key(), props, nullptr, &collector);
        rsWriter.addRow(writer);

        iter->next();
    }
    return ret;
}

void QueryEdgePropsProcessor::process(const cpp2::EdgePropRequest& req) {
    if (executor_ != nullptr) {
        executor_->add([req, this] () {
            this->doProcess(req);
        });
    } else {
        doProcess(req);
    }
}

void QueryEdgePropsProcessor::doProcess(const cpp2::EdgePropRequest& req) {
    spaceId_ = req.get_space_id();

    std::vector<EdgeType> e = {req.edge_type};
    initEdgeContext(e, true);

    auto retCode = this->checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }

    auto schema = edgeSchema_.find(req.edge_type);
    if (schema == edgeSchema_.end()) {
        LOG(ERROR) << "Not found the edge type: " << req.edge_type;
        this->onFinished();
        return;
    }
    RowSetWriter rsWriter(std::move(schema)->second);
    std::for_each(req.get_parts().begin(), req.get_parts().end(), [&](auto& partE) {
        auto partId = partE.first;
        kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
        for (auto& edgeKey : partE.second) {
            for (auto& ec : edgeContexts_) {
                ret = this->collectEdgesProps(
                        partId, edgeKey, ec.second, rsWriter);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    break;
                }
            }
            if (kvstore::ResultCode::SUCCEEDED != ret) {
                LOG(ERROR) << "It means the part has something wrong now. ErrorCode is " << ret;
                break;
            }
        }
        if (kvstore::ResultCode::ERR_LEADER_CHANGED == ret) {
            this->handleLeaderChanged(spaceId_, partId);
        } else {
            this->pushResultCode(this->to(ret), partId);
        }
    });
    resp_.set_data(std::move(rsWriter.data()));

    auto schemaResp = edgeSchemaResp_.find(req.edge_type);
    if (!(schemaResp == edgeSchemaResp_.end())) {
        resp_.set_schema(std::move(schemaResp)->second);
    }
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
