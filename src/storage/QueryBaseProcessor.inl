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
bool QueryBaseProcessor<REQ, RESP>::validOperation(nebula::cpp2::SupportedType vType,
                                                   cpp2::StatType statType) {
    switch (statType) {
        case cpp2::StatType::SUM:
        case cpp2::StatType::AVG: {
            return vType == nebula::cpp2::SupportedType::INT
                    || vType == nebula::cpp2::SupportedType::VID
                    || vType == nebula::cpp2::SupportedType::FLOAT
                    || vType == nebula::cpp2::SupportedType::DOUBLE;
        }
        case cpp2::StatType::COUNT: {
             break;
        }
    }
    return true;
}


template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::collectProps(RowReader* reader,
                                                 folly::StringPiece key,
                                                 std::vector<PropContext>& props,
                                                 Collector* collector) {
    for (auto& prop : props) {
        switch (prop.pikType_) {
            case PropContext::PropInKeyType::NONE:
                break;
            case PropContext::PropInKeyType::SRC:
                VLOG(3) << "collect _src, value = " << KeyUtils::getSrcId(key);
                collector->collectInt64(ResultType::SUCCEEDED, KeyUtils::getSrcId(key), prop);
                continue;
            case PropContext::PropInKeyType::DST:
                VLOG(3) << "collect _dst, value = " << KeyUtils::getDstId(key);
                collector->collectInt64(ResultType::SUCCEEDED, KeyUtils::getDstId(key), prop);
                continue;
            case PropContext::PropInKeyType::TYPE:
                VLOG(3) << "collect _type, value = " << KeyUtils::getEdgeType(key);
                collector->collectInt32(ResultType::SUCCEEDED, KeyUtils::getEdgeType(key), prop);
                continue;
            case PropContext::PropInKeyType::RANK:
                VLOG(3) << "collect _rank, value = " << KeyUtils::getRank(key);
                collector->collectInt64(ResultType::SUCCEEDED, KeyUtils::getRank(key), prop);
                continue;
        }
        if (reader != nullptr) {
            const auto& name = prop.prop_.get_name();
            switch (prop.type_.type) {
                case nebula::cpp2::SupportedType::INT: {
                    int64_t v;
                    auto ret = reader->getInt<int64_t>(name, v);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    collector->collectInt64(ret, v, prop);
                    break;
                }
                case nebula::cpp2::SupportedType::VID: {
                    int64_t v;
                    auto ret = reader->getVid(name, v);
                    collector->collectInt64(ret, v, prop);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    break;
                }
                case nebula::cpp2::SupportedType::FLOAT: {
                    float v;
                    auto ret = reader->getFloat(name, v);
                    collector->collectFloat(ret, v, prop);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    break;
                }
                case nebula::cpp2::SupportedType::DOUBLE: {
                    double v;
                    auto ret = reader->getDouble(name, v);
                    collector->collectDouble(ret, v, prop);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    break;
                }
                case nebula::cpp2::SupportedType::STRING: {
                    folly::StringPiece v;
                    auto ret = reader->getString(name, v);
                    collector->collectString(ret, v, prop);
                    VLOG(3) << "collect " << name << ", value = " << v;
                    break;
                }
                default: {
                    VLOG(1) << "Unsupport stats!";
                    break;
                }
            }  // switch
        }  // if
    }  // for
}


template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::checkAndBuildContexts(
        const REQ& req,
        std::vector<TagContext>& tagContexts,
        EdgeContext& edgeContext) {
    if (req.__isset.edge_type) {
        edgeContext.edgeType_ = req.edge_type;
    }
    // Handle the case for query edges which should return some columns by default.
    int32_t index = edgeContext.props_.size();
    std::unordered_map<TagID, int32_t> tagIndex;
    for (auto& col : req.get_return_columns()) {
        PropContext prop;
        switch (col.owner) {
            case cpp2::PropOwner::SOURCE:
            case cpp2::PropOwner::DEST: {
                auto tagId = col.tag_id;
                auto schema = meta::SchemaManager::getTagSchema(spaceId_, tagId);
                if (!schema) {
                    VLOG(3) << "Can't find spaceId " << spaceId_ << ", tagId " << tagId;
                    return cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
                }
                const auto& ftype = schema->getFieldType(col.name);
                prop.type_ = ftype;
                prop.retIndex_ = index++;
                if (col.__isset.stat && !validOperation(ftype.type, col.stat)) {
                    return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                }
                VLOG(3) << "tagId " << tagId << ", prop " << col.name;
                prop.prop_ = std::move(col);
                auto it = tagIndex.find(tagId);
                if (it == tagIndex.end()) {
                    TagContext tc;
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
                auto it = kPropsInKey_.find(col.name);
                if (it != kPropsInKey_.end()) {
                    prop.pikType_ = it->second;
                    prop.type_.type = nebula::cpp2::SupportedType::INT;
                } else if (type_ == BoundType::OUT_BOUND) {
                    // Only outBound have properties on edge.
                    auto schema = meta::SchemaManager::getEdgeSchema(spaceId_,
                                                                     edgeContext.edgeType_);
                    if (!schema) {
                        return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                    }
                    const auto& ftype = schema->getFieldType(col.name);
                    prop.type_ = ftype;
                } else {
                    VLOG(3) << "InBound has none props, skip it!";
                    break;
                }
                if (col.__isset.stat && !validOperation(prop.type_.type, col.stat)) {
                    return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                }
                prop.retIndex_ = index++;
                prop.prop_ = std::move(col);
                edgeContext.props_.emplace_back(std::move(prop));
                break;
            }
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            std::vector<PropContext>& props,
                            Collector* collector) {
    auto prefix = KeyUtils::prefix(partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    // Will decode the properties according to teh schema version
    // stored along with the properties
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(iter->val(), spaceId_, tagId);
        this->collectProps(reader.get(), iter->key(), props, collector);
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
    }
    return ret;
}

template<typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::collectEdgeProps(
                                               PartitionID partId,
                                               VertexID vId,
                                               EdgeType edgeType,
                                               std::vector<PropContext>& props,
                                               EdgeProcessor proc) {
    auto prefix = KeyUtils::prefix(partId, vId, edgeType);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED || !iter) {
        return ret;
    }
    EdgeRanking lastRank = -1;
    for (; iter->valid(); iter->next()) {
        auto key = iter->key();
        auto val = iter->val();
        auto rank = KeyUtils::getRank(key);
        if (rank == lastRank) {
            VLOG(3) << "Only get the latest version for each edge.";
            continue;
        }
        lastRank = rank;
        std::unique_ptr<RowReader> reader;
        if (type_ == BoundType::OUT_BOUND && !val.empty()) {
            reader = RowReader::getEdgePropReader(val, spaceId_, edgeType);
        }
        proc(reader.get(), key, props);
    }
    return ret;
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::process(const cpp2::GetNeighborsRequest& req) {
    spaceId_ = req.get_space_id();
    int32_t returnColumnsNum = req.get_return_columns().size();
    VLOG(3) << "Receive request, spaceId " << spaceId_ << ", return cols " << returnColumnsNum;
    std::vector<TagContext> tagContexts;
    tagContexts.reserve(returnColumnsNum);
    EdgeContext edgeContext;

    auto retCode = checkAndBuildContexts(req, tagContexts, edgeContext);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
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
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                break;
            }
        }
        this->pushResultCode(this->to(ret), partId);
    });

    onProcessed(tagContexts, edgeContext, returnColumnsNum);
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
