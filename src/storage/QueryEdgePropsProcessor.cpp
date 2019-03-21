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
                                       std::vector<PropContext>& props,
                                       RowSetWriter& rsWriter) {
    auto prefix = KeyUtils::prefix(partId, edgeKey.src, edgeKey.edge_type,
                                   edgeKey.ranking, edgeKey.dst);
    auto res = kvstore_->prefix(spaceId_, partId, prefix);
    if (!ok(res)) {
        VLOG(2) << "Failed to retrieve keys (error = " << error(res) << ")";
        return error(res);
    }

    std::unique_ptr<kvstore::KVIterator> iter = value(std::move(res));
    // Only use the latest version.
    if (iter && iter->valid()) {
        RowWriter writer(rsWriter.schema());
        PropsCollector collector(&writer);
        auto reader = RowReader::getEdgePropReader(schemaMan_,
                                                   iter->val(),
                                                   spaceId_,
                                                   edgeKey.edge_type);
        this->collectProps(reader.get(), iter->key(), props, &collector);
        rsWriter.addRow(writer);

        iter->next();
    }

    return kvstore::ResultCode::SUCCEEDED;
}


void QueryEdgePropsProcessor::addDefaultProps(EdgeContext& edgeContext) {
    edgeContext.props_.emplace_back("_src", 0, PropContext::PropInKeyType::SRC);
    edgeContext.props_.emplace_back("_rank", 1, PropContext::PropInKeyType::RANK);
    edgeContext.props_.emplace_back("_dst", 2, PropContext::PropInKeyType::DST);
}


void QueryEdgePropsProcessor::process(const cpp2::EdgePropRequest& req) {
    spaceId_ = req.get_space_id();
    EdgeContext edgeContext;
    std::vector<TagContext> tagContexts;
    // By default, _src, _rank, _dst will be returned as the first 3 fields
    addDefaultProps(edgeContext);
    int32_t returnColumnsNum = req.get_return_columns().size() + edgeContext.props_.size();
    auto retCode = this->checkAndBuildContexts(req, tagContexts, edgeContext);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }

    RowSetWriter rsWriter;
    std::for_each(req.get_parts().begin(), req.get_parts().end(), [&](auto& partE) {
        auto partId = partE.first;
        kvstore::ResultCode ret;
        for (auto& edgeKey : partE.second) {
            ret = this->collectEdgesProps(partId, edgeKey, edgeContext.props_, rsWriter);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                break;
            }
        }
        // TODO handle failures
        this->pushResultCode(this->to(ret), partId);
    });
    resp_.set_data(std::move(rsWriter.data()));

    std::vector<PropContext> props;
    props.reserve(returnColumnsNum);
    for (auto& prop : edgeContext.props_) {
        props.emplace_back(std::move(prop));
    }
    std::sort(props.begin(), props.end(), [](auto& l, auto& r){
        return l.retIndex_ < r.retIndex_;
    });
    decltype(resp_.schema) s;
    decltype(resp_.schema.columns) cols;
    for (auto& prop : props) {
        VLOG(3) << prop.prop_.name << "," << static_cast<int8_t>(prop.type_.type);
        cols.emplace_back(
                columnDef(std::move(prop.prop_.name),
                          prop.type_.type));
    }
    s.set_columns(std::move(cols));
    resp_.set_schema(std::move(s));
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
