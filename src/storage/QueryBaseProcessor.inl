/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#include "storage/QueryBaseProcessor.h"
#include <algorithm>
#include "storage/KeyUtils.h"
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"


namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
bool QueryBaseProcessor<REQ, RESP>::validOperation(cpp2::SupportedType vType,
                                              cpp2::StatType statType) {
    switch (statType) {
        case cpp2::StatType::SUM:
        case cpp2::StatType::AVG: {
            return vType == cpp2::SupportedType::INT
                    || vType == cpp2::SupportedType::VID
                    || vType == cpp2::SupportedType::FLOAT
                    || vType == cpp2::SupportedType::DOUBLE;
        }
        case cpp2::StatType::COUNT: {
             break;
        }
    }
    return true;
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::collectProps(
                                       SchemaProviderIf* rowSchema,
                                       folly::StringPiece& key,
                                       folly::StringPiece& val,
                                       std::vector<PropContext>& props,
                                       Collector* collector) {
    UNUSED(key);
    RowReader reader(rowSchema, val);
    for (auto& prop : props) {
        const auto& name = prop.prop_.get_name();
        VLOG(3) << "collect " << name;
        switch (prop.type_.type) {
            case cpp2::SupportedType::INT: {
                int64_t v;
                auto ret = reader.getInt<int64_t>(name, v);
                collector->collectInt64(ret, v, prop);
                break;
            }
            case cpp2::SupportedType::VID: {
                int64_t v;
                auto ret = reader.getVid(name, v);
                collector->collectInt64(ret, v, prop);
                break;
            }
            case cpp2::SupportedType::FLOAT: {
                float v;
                auto ret = reader.getFloat(name, v);
                collector->collectFloat(ret, v, prop);
                break;
            }
            case cpp2::SupportedType::DOUBLE: {
                double v;
                auto ret = reader.getDouble(name, v);
                collector->collectDouble(ret, v, prop);
                break;
            }
            case cpp2::SupportedType::STRING: {
                folly::StringPiece v;
                auto ret = reader.getString(name, v);
                collector->collectString(ret, v, prop);
                break;
            }
            default: {
                VLOG(1) << "Unsupport stats!";
                break;
            }
        }  // switch
    }  // for
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::checkAndBuildContexts(
                                              const REQ& req,
                                              std::vector<TagContext>& tagContexts,
                                              EdgeContext& edgeContext) {
    if (req.__isset.edge_type) {
        edgeContext.edgeType_ = req.edge_type;
        edgeContext.schema_ = schemaMan_->edgeSchema(spaceId_, req.edge_type);
        if (edgeContext.schema_ == nullptr) {
            return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
        }
    }
    int32_t index = 0;
    std::unordered_map<TagID, int32_t> tagIndex;
    for (auto& col : req.get_return_columns()) {
        PropContext prop;
        switch (col.owner) {
            case cpp2::PropOwner::SOURCE:
            case cpp2::PropOwner::DEST: {
                auto tagId = col.tag_id;
                auto* schema = schemaMan_->tagSchema(spaceId_, tagId);
                if (schema == nullptr) {
                    return cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
                }
                const auto* ftype = schema->getFieldType(col.name, 0);
                prop.type_ = *ftype;
                prop.retIndex_ = index;
                if (col.__isset.stat && !validOperation(ftype->type, col.stat)) {
                    return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                }
                VLOG(3) << "tagId " << tagId << ", prop " << col.name;
                prop.prop_ = std::move(col);
                auto it = tagIndex.find(tagId);
                if (it == tagIndex.end()) {
                    TagContext tc;
                    tc.schema_ = schema;
                    tc.tagId_ = tagId;
                    tc.props_.emplace_back(std::move(prop));
                    tagContexts.emplace_back(std::move(tc));
                    tagIndex.emplace(tagId, tagContexts.size() - 1);
                } else {
                    tagContexts[it->second].props_.emplace_back(std::move(prop));
                }
                break;
            }
            case cpp2::PropOwner::EDGE: {
                if (edgeContext.schema_ != nullptr) {
                    const auto* ftype = edgeContext.schema_->getFieldType(col.name, 0);
                    prop.type_ = *ftype;
                    if (col.__isset.stat && !validOperation(ftype->type, col.stat)) {
                        return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                    }
                    prop.retIndex_ = index;
                    prop.prop_ = std::move(col);
                    edgeContext.props_.emplace_back(std::move(prop));
                }
                break;
            }
        }
        index++;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::process(const cpp2::GetNeighborsRequest& req) {
    spaceId_ = req.get_space_id();
    int32_t returnColumnsNum = req.get_return_columns().size();
    std::vector<TagContext> tagContexts;
    EdgeContext edgeContext;
    auto retCode = checkAndBuildContexts(req, tagContexts, edgeContext);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        this->pushResultCode(retCode, -1);
        this->resp_.result.set_latency_in_ms(this->duration_.elapsedInMSec());
        this->onFinished();
        return;
    }

//    const auto& filter = req.get_filter();
    std::for_each(req.get_parts().begin(), req.get_parts().end(), [&](auto& partV) {
        auto partId = partV.first;
        kvstore::ResultCode ret;
        for (auto& vId : partV.second) {
            VLOG(3) << "Process part " << partId << ", vertex " << vId;
            ret = processVertex(partId, vId, tagContexts, edgeContext);
            if (ret != kvstore::ResultCode::SUCCESSED) {
                break;
            }
        }
        this->pushResultCode(this->to(ret), partId);
    });

    onProcessed(tagContexts, edgeContext, returnColumnsNum);
    this->resp_.result.set_latency_in_ms(this->duration_.elapsedInMSec());
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
