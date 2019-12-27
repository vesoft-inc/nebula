/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <interface/gen-cpp2/storage_types.h>
#include <interface/gen-cpp2/common_types.h>

DECLARE_int32(max_row_returned_per_index_scan);
DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {
template<typename REQ, typename RESP>
cpp2::ErrorCode ScanIndexBaseProcessor<REQ, RESP>::buildIndexHint() {
    // TODO(sky) : Currently only supported :
    //             1) Range query for the first column
    //             2) Equivalent query for all columns
    if (hint_.is_range) {
        if (hint_.hint_items.size() != 1) {
            return cpp2::ErrorCode::E_INVALID_INDEX_HINT;
        }
        range_ = std::make_pair(hint_.hint_items[0].get_first_str(),
                                hint_.hint_items[0].get_second_str());
    } else {
        prefix_.reserve(128);
        for (auto& hint : hint_.hint_items) {
                prefix_.append(hint.get_first_str());
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode ScanIndexBaseProcessor<REQ, RESP>::createResultSchema(
        bool isEdge, int32_t id, const std::vector<std::string> &returnCols) {
    if (schema_ == nullptr) {
        schema_ = std::make_shared<SchemaWriter>();
        auto schema = (isEdge) ? this->schemaMan_->getEdgeSchema(spaceId_, id) :
                                 this->schemaMan_->getTagSchema(spaceId_, id);
        if (!schema) {
            VLOG(3) << "Can't find spaceId " << spaceId_ << ", id " << id;
            return (isEdge) ? cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND :
                              cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
        }
        for (auto& col : returnCols) {
            const auto& ftype = schema->getFieldType(col);
            if (UNLIKELY(ftype == CommonConstants::kInvalidValueType())) {
                return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
            }
            schema_->appendCol(col, std::move(ftype).get_type());
        }   // end for
    }   // end if
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
kvstore::ResultCode ScanIndexBaseProcessor<REQ, RESP>::getVertexRow(PartitionID partId,
                                                                    TagID tagId,
                                                                    const folly::StringPiece& key,
                                                                    cpp2::VertexIndexData* data) {
    auto vId = NebulaKeyUtils::getIndexVertexID(key);
    data->set_vertex_id(vId);

    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
        auto result = vertexCache_->get(std::make_pair(vId, tagId), partId);
        if (result.ok()) {
            auto v = std::move(result).value();
            auto reader = RowReader::getTagPropReader(this->schemaMan_, v, spaceId_, tagId);
            auto row = getRowFromReader(reader.get());
            if (row.empty()) {
                return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
            }
            data->set_props(std::move(row));
            VLOG(3) << "Hit cache for vId " << vId << ", tagId " << tagId;
            return kvstore::ResultCode::SUCCEEDED;
        } else {
            VLOG(3) << "Miss cache for vId " << vId << ", tagId " << tagId;
        }
    }
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(this->schemaMan_, iter->val(), spaceId_, tagId);
        auto row = getRowFromReader(reader.get());
        if (row.empty()) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
        data->set_props(std::move(row));
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            vertexCache_->insert(std::make_pair(vId, tagId),
                                 iter->val().str(), partId);
            VLOG(3) << "Insert cache for vId " << vId << ", tagId " << tagId;
        }
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

template<typename REQ, typename RESP>
kvstore::ResultCode ScanIndexBaseProcessor<REQ, RESP>::getEdgeRow(PartitionID partId,
                                                                  EdgeType edgeType,
                                                                  const folly::StringPiece& key,
                                                                  cpp2::Edge* data) {
    auto src = NebulaKeyUtils::getIndexSrcId(key);
    auto rank = NebulaKeyUtils::getIndexRank(key);
    auto dst = NebulaKeyUtils::getIndexDstId(key);

    cpp2::EdgeKey edgeKey;
    edgeKey.set_src(src);
    edgeKey.set_edge_type(edgeType);
    edgeKey.set_ranking(rank);
    edgeKey.set_dst(dst);

    data->set_key(std::move(edgeKey));

    auto prefix = NebulaKeyUtils::edgePrefix(partId, src, edgeType, rank, dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                   iter->val(),
                                                   spaceId_,
                                                   edgeType);
        auto row = getRowFromReader(reader.get());
        if (row.empty()) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
        data->set_props(std::move(row));
    } else {
        VLOG(3) << "Missed partId " << partId << ", src " << src << ", edgeType " << edgeType
                << ", Rank " << rank << ", dst " << dst;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

template<typename REQ, typename RESP>
std::string ScanIndexBaseProcessor<REQ, RESP>::getRowFromReader(RowReader* reader) {
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

template<typename REQ, typename RESP>
bool ScanIndexBaseProcessor<REQ, RESP>::checkDataValidity(bool isEdge,
                                                          const folly::StringPiece& key) {
    auto offset = (isEdge) ? key.size() - sizeof(VertexID) * 2 - sizeof(EdgeRanking) - vlColNum_ :
                             key.size() - sizeof(VertexID) - vlColNum_ * sizeof(int32_t);
    for (auto & col : hint_.hint_items) {
        if (col.type == nebula::cpp2::SupportedType::STRING) {
            auto len = static_cast<size_t >
                       (*reinterpret_cast<const int32_t *>(key.begin() + offset));
            if (len < col.get_first_str().size() || len < col.get_second_str().size()) {
                return false;
            }
            offset += sizeof(int32_t);
        }
    }
    return true;
}

}  // namespace storage
}  // namespace nebula
