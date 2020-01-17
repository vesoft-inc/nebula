/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "storage/index/IndexExecutor.h"

DECLARE_int32(max_rows_returned_per_lookup);
DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {
template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::prepareScan(const cpp2::LookUpIndexRequest& req) {
    spaceId_ = req.get_space_id();
    auto ret = policyPrepare(req.get_filter());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    ret = checkIndex(req.get_index_id());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    policyGenerate();
    ret = createResultSchema(req.get_return_columns());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    preparePrefix();
    return ret;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::performScan(PartitionID part) {
    kvstore::ResultCode ret;
    switch (policyScanType_) {
        case PolicyScanType::ACCURATE_SCAN : {
            ret = accurateScan(part);
            break;
        }
        case PolicyScanType::PREFIX_SCAN : {
            ret = prefixScan(part);
            break;
        }
        case PolicyScanType::SEEK_SCAN : {
            ret = seekScan(part);
            break;
        }
    }
    return ret;
}

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::checkIndex(IndexID indexId) {
    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>> index;
    if (isEdgeIndex_) {
        index = indexMan_->getEdgeIndex(spaceId_, indexId);
    } else {
        index = indexMan_->getTagIndex(spaceId_, indexId);
    }
    if (!index.ok()) {
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    index_ = std::move(index).value();
    tagOrEdge_ = (isEdgeIndex_) ? index_->get_schema_id().get_edge_type() :
                                  index_->get_schema_id().get_tag_id();
    for (const auto& col : index_->get_fields()) {
        indexCols_[col.get_name()] = col.get_type().get_type();
        if (col.get_type().get_type() == nebula::cpp2::SupportedType::STRING) {
            vColSize_++;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::createResultSchema(const std::vector<std::string> &cols) {
    if (!cols.empty()) {
        schema_ = std::make_shared<SchemaWriter>();
        auto schema = isEdgeIndex_ ?
                      schemaMan_->getEdgeSchema(spaceId_, tagOrEdge_) :
                      schemaMan_->getTagSchema(spaceId_, tagOrEdge_);
        if (!schema) {
            VLOG(3) << "Can't find schema, spaceId "
                    << spaceId_ << ", id " << tagOrEdge_;
            return isEdgeIndex_ ? cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND :
                                  cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
        }
        for (auto& col : cols) {
            const auto& ftype = schema->getFieldType(col);
            if (UNLIKELY(ftype == CommonConstants::kInvalidValueType())) {
                return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
            }
            schema_->appendCol(col, std::move(ftype).get_type());
        }   // end for
    } else {
        needReturnCols_ = false;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
void IndexExecutor<RESP>::preparePrefix() {
    prefix_.reserve(256);
    for (const auto& v : policies_) {
        prefix_.append(NebulaKeyUtils::encodeVariant(v));
    }
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::accurateScan(PartitionID part) {
    std::string prefix = NebulaKeyUtils::indexPrefix(part, index_->get_index_id())
                         .append(prefix_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_,
                                      part,
                                      prefix,
                                      &iter);
    if (ret != nebula::kvstore::SUCCEEDED) {
        return ret;
    }
    while (iter->valid() &&
           rowCount_ < FLAGS_max_rows_returned_per_lookup) {
        auto key = iter->key();
        ret = getDataRow(part, key);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        iter->next();
    }
    return ret;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::prefixScan(PartitionID part) {
    std::string prefix = NebulaKeyUtils::indexPrefix(part, index_->get_index_id())
                        .append(prefix_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_,
                                      part,
                                      prefix,
                                      &iter);
    if (ret != nebula::kvstore::SUCCEEDED) {
        return ret;
    }
    while (iter->valid() &&
           rowCount_ < FLAGS_max_rows_returned_per_lookup) {
        auto key = iter->key();
        if (!conditionsCheck(key)) {
            iter->next();
            continue;
        }
        ret = getDataRow(part, key);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
        iter->next();
    }
    return ret;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::seekScan(PartitionID part) {
    /**
     * TODO sky : seekScan used conditional comparison with greater than , etc.
     *            now reuse the prefixScan.
     */
    return prefixScan(part);
}

template<typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::getDataRow(PartitionID partId,
                                                    const folly::StringPiece& key) {
    kvstore::ResultCode ret;
    if (isEdgeIndex_) {
        cpp2::Edge data;
        ret = getEdgeRow(partId, key, &data);
        if (ret == kvstore::SUCCEEDED) {
            edgeRows_.emplace_back(std::move(data));
            ++rowCount_;
        }
    } else {
        cpp2::VertexIndexData data;
        ret = getVertexRow(partId, key, &data);
        if (ret == kvstore::SUCCEEDED) {
            vertexRows_.emplace_back(std::move(data));
            ++rowCount_;
        }
    }
    return ret;
}

template<typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::getVertexRow(PartitionID partId,
                                                      const folly::StringPiece& key,
                                                      cpp2::VertexIndexData* data) {
    auto vId = NebulaKeyUtils::getIndexVertexID(key);
    data->set_vertex_id(vId);
    if (!needReturnCols_) {
        return kvstore::ResultCode::SUCCEEDED;
    }
    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
        auto result = vertexCache_->get(std::make_pair(vId, tagOrEdge_), partId);
        if (result.ok()) {
            auto v = std::move(result).value();
            auto reader = RowReader::getTagPropReader(schemaMan_, v, spaceId_, tagOrEdge_);
            auto row = getRowFromReader(reader.get());
            if (row.empty()) {
                return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
            }
            data->set_props(std::move(row));
            VLOG(3) << "Hit cache for vId " << vId << ", tagId " << tagOrEdge_;
            return kvstore::ResultCode::SUCCEEDED;
        } else {
            VLOG(3) << "Miss cache for vId " << vId << ", tagId " << tagOrEdge_;
        }
    }
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, tagOrEdge_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(schemaMan_,
                                                  iter->val(),
                                                  spaceId_,
                                                  tagOrEdge_);
        auto row = getRowFromReader(reader.get());
        if (row.empty()) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
        data->set_props(std::move(row));
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            vertexCache_->insert(std::make_pair(vId, tagOrEdge_),
                                 iter->val().str(), partId);
            VLOG(3) << "Insert cache for vId " << vId << ", tagId " << tagOrEdge_;
        }
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagOrEdge_;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

template<typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::getEdgeRow(PartitionID partId,
                                                    const folly::StringPiece& key,
                                                    cpp2::Edge* data) {
    auto src = NebulaKeyUtils::getIndexSrcId(key);
    auto rank = NebulaKeyUtils::getIndexRank(key);
    auto dst = NebulaKeyUtils::getIndexDstId(key);
    cpp2::EdgeKey edge;
    edge.set_src(src);
    edge.set_edge_type(tagOrEdge_);
    edge.set_ranking(rank);
    edge.set_dst(dst);
    if (!needReturnCols_) {
        return kvstore::ResultCode::SUCCEEDED;
    }
    auto prefix = NebulaKeyUtils::edgePrefix(partId, src, tagOrEdge_, rank, dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = "
                << static_cast<int32_t>(ret)
                << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getEdgePropReader(schemaMan_,
                                                   iter->val(),
                                                   spaceId_,
                                                   tagOrEdge_);
        auto row = getRowFromReader(reader.get());
        if (row.empty()) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
        data->set_props(std::move(row));
    } else {
        VLOG(3) << "Missed partId " << partId
                << ", src " << src << ", edgeType "
                << tagOrEdge_ << ", Rank "
                << rank << ", dst " << dst;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

template<typename RESP>
std::string IndexExecutor<RESP>::getRowFromReader(RowReader* reader) {
    std::string encode;
    RowWriter writer(schema_);
    for (auto i = 0; i < static_cast<int64_t>(schema_->getNumFields()); i++) {
        const auto& name = schema_->getFieldName(i);
        auto res = RowReader::getPropByName(reader, name);
        if (!ok(res)) {
            VLOG(1) << "Skip the bad value for prop " << name;
            continue;
        }
        auto&& v = value(std::move(res));
        switch (v.which()) {
            case VAR_INT64:
                writer << boost::get<int64_t>(v);
                break;
            case VAR_DOUBLE:
                writer << boost::get<double>(v);
                break;
            case VAR_BOOL:
                writer << boost::get<bool>(v);
                break;
            case VAR_STR:
                writer << boost::get<std::string>(v);
                break;
            default:
                LOG(FATAL) << "Unknown VariantType: " << v.which();
        }  // switch
    }
    return writer.encode();
}

template<typename RESP>
bool IndexExecutor<RESP>::conditionsCheck(const folly::StringPiece& key) {
    UNUSED(key);
    Getters getters;
    getters.getAliasProp = [this, &key](const std::string&,
                                        const std::string &prop) -> OptVariantType {
            return decodeValue(key, prop);
    };
    return exprEval(getters);
}

template<typename RESP>
OptVariantType IndexExecutor<RESP>::decodeValue(const folly::StringPiece& key,
                                                const folly::StringPiece& prop) {
    using nebula::cpp2::SupportedType;
    auto type = indexCols_[prop.str()];
    return NebulaKeyUtils::decodeVariant(getIndexVal(key, prop), type);
}

template<typename RESP>
folly::StringPiece IndexExecutor<RESP>::getIndexVal(const folly::StringPiece& key,
                                                    const folly::StringPiece& prop) {
    auto tailLen = (!isEdgeIndex_) ? sizeof(VertexID) :
                                     sizeof(VertexID) * 2 + sizeof(EdgeRanking);
    using nebula::cpp2::SupportedType;
    size_t offset = sizeof(PartitionID) + sizeof(IndexID);
    int32_t len = 0;
    int32_t vCount = vColSize_;
    for (const auto& col : index_->get_fields()) {
        switch (col.get_type().get_type()) {
            case SupportedType::BOOL: {
                len = sizeof(bool);
                break;
            }
            case SupportedType::TIMESTAMP:
            case SupportedType::INT: {
                len = sizeof(int64_t);
                break;
            }
            case SupportedType::FLOAT:
            case SupportedType::DOUBLE: {
                len = sizeof(double);
                break;
            }
            case SupportedType::STRING: {
                auto off = key.size() - vCount * sizeof(int32_t) - tailLen;
                len = *reinterpret_cast<const int32_t*>(key.data() + off);
                --vCount;
                break;
            }
            default:
                len = 0;
        }
        if (col.get_name() == prop.str()) {
            break;
        }
        offset += len;
    }
    return key.subpiece(offset, len);
}

}  // namespace storage
}  // namespace nebula
