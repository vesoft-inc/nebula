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
    if (ret != kvstore::ResultCode::SUCCEEDED) {
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

    int32_t edgeSize = std::accumulate(edgeContexts_.cbegin(), edgeContexts_.cend(), 0,
                                       [](int ac, auto& ec) { return ac + ec.second.size(); });

    int32_t returnColumnsNum = req.get_return_columns().size() + edgeSize;
    RowSetWriter rsWriter;
    std::for_each(req.get_parts().begin(), req.get_parts().end(), [&](auto& partE) {
        auto partId = partE.first;
        kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
        for (auto& edgeKey : partE.second) {
            for (auto& ec : edgeContexts_) {
                ret = this->collectEdgesProps(partId, edgeKey, ec.second, rsWriter);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    break;
                }
            }
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "It means the part has something wrong now";
                break;
            }
        }
        if (ret == kvstore::ResultCode::ERR_LEADER_CHANGED) {
            this->handleLeaderChanged(spaceId_, partId);
        } else {
            this->pushResultCode(this->to(ret), partId);
        }
    });
    resp_.set_data(std::move(rsWriter.data()));

    std::vector<PropContext> props;
    props.reserve(returnColumnsNum);
     for (auto& ec : this->edgeContexts_) {
        auto p = ec.second;
        for (auto& prop : p) {
            props.emplace_back(std::move(prop));
        }
    }

    std::sort(props.begin(), props.end(), [](auto& l, auto& r){
        return l.retIndex_ < r.retIndex_;
    });
    decltype(resp_.schema) s;
    decltype(resp_.schema.columns) cols;
    for (auto& prop : props) {
        VLOG(3) << prop.prop_.name << "," << static_cast<int32_t>(prop.type_.type);
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
