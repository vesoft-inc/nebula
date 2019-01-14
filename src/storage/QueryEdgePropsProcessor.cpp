/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/QueryEdgePropsProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "storage/KeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"

namespace nebula {
namespace storage {

kvstore::ResultCode QueryEdgePropsProcessor::collectEdgesProps(
                                       PartitionID partId,
                                       const cpp2::EdgeKey& edgeKey,
                                       SchemaProviderIf* edgeSchema,
                                       std::vector<PropContext>& props,
                                       RowSetWriter& rsWriter) {
    auto prefix = KeyUtils::prefix(partId, edgeKey.src, edgeKey.edge_type, edgeKey.dst, edgeKey.ranking);
    std::unique_ptr<kvstore::StorageIter> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, &iter);
    // Only use the latest version.
    if (iter && iter->valid()) {
        RowWriter writer;
        auto key = iter->key();
        auto val = iter->val();
        PropsCollector collector(&writer);
        this->collectProps(edgeSchema, key, val, props, &collector);
        iter->next();
        rsWriter.addRow(writer);
    }
    return ret;
}

void QueryEdgePropsProcessor::process(const cpp2::EdgePropRequest& req) {
    spaceId_ = req.get_space_id();
    int32_t returnColumnsNum = req.get_return_columns().size();
    EdgeContext edgeContext;
    std::vector<TagContext> tagContexts;
    auto retCode = this->checkAndBuildContexts(req, tagContexts, edgeContext);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        this->pushResultCode(retCode, -1);
        this->resp_.latency_in_ms = duration_.elapsedInMSec();
        this->onFinished();
        return;
    }

    RowSetWriter rsWriter;
    std::for_each(req.get_edges().begin(), req.get_edges().end(), [&](auto& partE) {
        auto partId = partE.first;
        kvstore::ResultCode ret;
        for (auto& edgeKey : partE.second) {
            ret = this->collectEdgesProps(partId, edgeKey, edgeContext.schema_,
                                          edgeContext.props_, rsWriter);
            if (ret != kvstore::ResultCode::SUCCESSED) {
                break;
            }
        };
        // TODO handle failures
        this->pushResultCode(this->to(ret), partId);
    });
    resp_.data = std::move(rsWriter.data());

    std::vector<PropContext> props;
    props.reserve(returnColumnsNum);
    for (auto& prop : edgeContext.props_) {
        props.emplace_back(std::move(prop));
    }
    std::sort(props.begin(), props.end(), [](auto& l, auto& r){
        return l.retIndex_ < r.retIndex_;
    });
    for (auto& prop : props) {
        resp_.schema.columns.emplace_back(
                columnDef(std::move(prop.prop_.name),
                          prop.type_.type));
    }
    resp_.latency_in_ms = duration_.elapsedInMSec();
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
