/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "storage/QueryStatsProcessor.h"
#include <algorithm>
#include "time/Duration.h"
#include "storage/KeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"


namespace nebula {
namespace storage {

kvstore::ResultCode
QueryStatsProcessor::collectVertexStats(PartitionID partId, VertexID vId, TagID tagId,
                                        SchemaProviderIf* tagSchema,
                                        std::vector<PropContext>& props) {
    auto prefix = KeyUtils::prefix(partId, vId, tagId);
    std::unique_ptr<kvstore::StorageIter> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, iter);
    if (ret != kvstore::ResultCode::SUCCESSED) {
        return ret;
    }
    // Only get the latest version.
    if (iter && iter->valid()) {
        auto key = iter->val();
        auto val = iter->val();
        collectProps(tagSchema, key, val, props, &collector_);
    }
    return ret;
}

kvstore::ResultCode
QueryStatsProcessor::collectEdgesStats(PartitionID partId, VertexID vId, EdgeType edgeType,
                                       SchemaProviderIf* edgeSchema,
                                       std::vector<PropContext>& props) {
    auto prefix = KeyUtils::prefix(partId, vId, edgeType);
    std::unique_ptr<kvstore::StorageIter> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, iter);
    if (ret != kvstore::ResultCode::SUCCESSED || !iter) {
        return ret;
    }
    while(iter->valid()) {
        auto key = iter->key();
        auto val = iter->val();
        collectProps(edgeSchema, key, val, props, &collector_);
        iter->next();
    }
    return ret;
}

void QueryStatsProcessor::calcResult(std::vector<PropContext>&& props) {
    RowWriter writer;
    for (auto& prop : props) {
        switch(prop.prop_.stat) {
            case cpp2::StatType::SUM: {
                switch (prop.sum_.which()) {
                    case 0:
                        writer << boost::get<int64_t>(prop.sum_);
                        resp_.schema.columns.emplace_back(
                                columnDef(std::move(prop.prop_.name),
                                          cpp2::SupportedType::INT));
                        break;
                    case 1:
                        writer << boost::get<double>(prop.sum_);
                        resp_.schema.columns.emplace_back(
                                columnDef(std::move(prop.prop_.name),
                                          cpp2::SupportedType::DOUBLE));
                        break;
                }
                break;
            }
            case cpp2::StatType::COUNT: {
                writer << prop.count_;
                resp_.schema.columns.emplace_back(
                            columnDef(std::move(prop.prop_.name),
                                      cpp2::SupportedType::INT));
                break;
            }
            case cpp2::StatType::AVG: {
                switch (prop.sum_.which()) {
                    case 0:
                        writer << (double)boost::get<int64_t>(prop.sum_) / prop.count_;
                        break;
                    case 1:
                        writer << boost::get<double>(prop.sum_) / prop.count_;
                        break;
                }
                resp_.schema.columns.emplace_back(
                        columnDef(std::move(prop.prop_.name),
                                  cpp2::SupportedType::DOUBLE));
                break;
            }
        }
    }
    resp_.data = std::move(writer.encode());
}

kvstore::ResultCode QueryStatsProcessor::processVertex(PartitionID partId, VertexID vId,
                                                       std::vector<TagContext>& tagContexts,
                                                       EdgeContext& edgeContext) {
    for (auto& tc : tagContexts) {
        auto ret = collectVertexStats(partId, vId, tc.tagId_, tc.schema_, tc.props_);
        if (ret != kvstore::ResultCode::SUCCESSED) {
            return ret;
        }
    }
    if (edgeContext.schema_ != nullptr) {
        auto ret = collectEdgesStats(partId, vId, edgeContext.edgeType_,
                                     edgeContext.schema_, edgeContext.props_);
        if (ret != kvstore::ResultCode::SUCCESSED) {
            return ret;
        }
    }
    return kvstore::ResultCode::SUCCESSED;
}

void QueryStatsProcessor::onProcessed(std::vector<TagContext>& tagContexts,
                                      EdgeContext& edgeContext, int32_t retNum) {
    std::vector<PropContext> props;
    props.reserve(retNum);
    for (auto& tc : tagContexts) {
        for (auto& prop : tc.props_) {
            props.emplace_back(std::move(prop));
        }
    }
    for (auto& prop : edgeContext.props_) {
        props.emplace_back(std::move(prop));
    }
    std::sort(props.begin(), props.end(), [](auto& l, auto& r){
        return l.retIndex_ < r.retIndex_;
    });
    calcResult(std::move(props));
}

}  // namespace storage
}  // namespace nebula
