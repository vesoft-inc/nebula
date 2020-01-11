/* Copyright (c) 2020 vesoft inc. All rights reserved.
*
* This source code is licensed under Apache 2.0 License,
* attached with Common Clause Condition 1.0, found in the LICENSES directory.
*/

#include "storage/index/IndexExecutor.h"

<<<<<<< HEAD
DECLARE_int32(max_rows_returned_per_lookup);
=======
DECLARE_int32(max_row_returned_per_index_scan);
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {
<<<<<<< HEAD

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::prepareRequest(const cpp2::LookUpIndexRequest &req) {
    spaceId_ = req.get_space_id();
    /**
     * step 1 , check index meta , and collect index variable-length type of columns.
     */
    auto ret = checkIndex(req.get_index_id());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    /**
     * step 2 , check the return columns, and create schema for row binary.
     */
    ret = checkReturnColumns(req.get_return_columns());
=======
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
    ret = policyGenerate();
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    ret = createResultSchema(req.get_return_columns());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    ret = preparePrefix();
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
    return ret;
}

template <typename RESP>
<<<<<<< HEAD
cpp2::ErrorCode IndexExecutor<RESP>::buildExecutionPlan(const std::string& filter) {
    auto ret = preparePolicy(filter);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    buildPolicy();
=======
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
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
    return ret;
}

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::checkIndex(IndexID indexId) {
<<<<<<< HEAD
    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>> index;
    if (isEdgeIndex_) {
        index = indexMan_->getEdgeIndex(spaceId_, indexId);
    } else {
        index = indexMan_->getTagIndex(spaceId_, indexId);
=======
    StatusOr<nebula::cpp2::IndexItem> index;
    if (isEdgeIndex_) {
        index = schemaMan_->getEdgeIndex(spaceId_, indexId);
    } else {
        index = schemaMan_->getTagIndex(spaceId_, indexId);
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
    }
    if (!index.ok()) {
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
<<<<<<< HEAD
    index_ = std::move(index).value();
    tagOrEdge_ = (isEdgeIndex_) ? index_->get_schema_id().get_edge_type() :
                                  index_->get_schema_id().get_tag_id();
    for (const auto& col : index_->get_fields()) {
        indexCols_[col.get_name()] = col.get_type().get_type();
        if (col.get_type().get_type() == nebula::cpp2::SupportedType::STRING) {
            vColNum_++;
=======
    this->index_ = index.value();
    this->tagOrEdge_ = index_.tagOrEdge;
    for (const auto& col : this->index_.get_cols()) {
        indexCols_[col.get_name()] = col.get_type().get_type();
        if (col.get_type().get_type() == nebula::cpp2::SupportedType::STRING) {
            vColSize_++;
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
<<<<<<< HEAD
cpp2::ErrorCode IndexExecutor<RESP>::checkReturnColumns(const std::vector<std::string> &cols) {
    int32_t index = 0;
=======
cpp2::ErrorCode IndexExecutor<RESP>::createResultSchema(const std::vector<std::string> &cols) {
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
    if (!cols.empty()) {
        schema_ = std::make_shared<SchemaWriter>();
        auto schema = isEdgeIndex_ ?
                      schemaMan_->getEdgeSchema(spaceId_, tagOrEdge_) :
                      schemaMan_->getTagSchema(spaceId_, tagOrEdge_);
        if (!schema) {
<<<<<<< HEAD
            LOG(ERROR) << "Can't find schema, spaceId "
                       << spaceId_ << ", id " << tagOrEdge_;
            return isEdgeIndex_ ? cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND :
                                  cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
        }

        for (auto i = 0; i < static_cast<int64_t>(schema_->getNumFields()); i++) {
            PropContext prop;
            const auto& name = schema_->getFieldName(i);
            auto ftype = schema_->getFieldType(name);
            prop.type_ = ftype;
            prop.retIndex_ = index++;
            storage::cpp2::PropDef pd;
            pd.set_name(name);
            prop.prop_ = std::move(pd);
            prop.returned_ = true;
            props_.emplace_back(std::move(prop));
        }
=======
            VLOG(3) << "Can't find schema, spaceId "
                    << spaceId_ << ", id " << tagOrEdge_;
            return isEdgeIndex_ ? cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND :
                                  cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
        }
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
        for (auto& col : cols) {
            const auto& ftype = schema->getFieldType(col);
            if (UNLIKELY(ftype == CommonConstants::kInvalidValueType())) {
                return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
            }
<<<<<<< HEAD
            /**
             * Create PropContext for getRowFromReader
             */
            {
                PropContext prop;
                prop.type_ = ftype;
                prop.retIndex_ = index++;
                storage::cpp2::PropDef pd;
                pd.set_name(col);
                prop.prop_ = std::move(pd);
                prop.returned_ = true;
                props_.emplace_back(std::move(prop));
            }
            schema_->appendCol(col, std::move(ftype).get_type());
        }   // end for
=======
            schema_->appendCol(col, std::move(ftype).get_type());
        }   // end for
    } else {
        needReturnCols_ = false;
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
<<<<<<< HEAD
kvstore::ResultCode IndexExecutor<RESP>::executeExecutionPlan(PartitionID part) {
    std::string prefix = NebulaKeyUtils::indexPrefix(part, index_->get_index_id())
                        .append(prefix_);
    std::unique_ptr<kvstore::KVIterator> iter;
    std::vector<std::string> keys;
=======
cpp2::ErrorCode IndexExecutor<RESP>::preparePrefix() {
    prefix_.reserve(256);
    auto indexId = index_.get_index_id();
    switch (this->policyScanType_) {
        case PolicyScanType::SEEK_SCAN : {
            prefix_.append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
        }
        case PolicyScanType::ACCURATE_SCAN :
        case PolicyScanType::PREFIX_SCAN: {
            prefix_.append(reinterpret_cast<const char*>(&indexId), sizeof(IndexID));
            for (const auto& v : policies_) {
                prefix_.append(NebulaKeyUtils::encodeVariant(v));
            }
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::accurateScan(PartitionID part) {
    std::string prefix = NebulaKeyUtils::prefix(part) + prefix_;
    std::unique_ptr<kvstore::KVIterator> iter;
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
    auto ret = this->kvstore_->prefix(spaceId_,
                                      part,
                                      prefix,
                                      &iter);
<<<<<<< HEAD
    if (ret != nebula::kvstore::SUCCEEDED) {
        return ret;
    }
    while (iter->valid() &&
           rowNum_ < FLAGS_max_rows_returned_per_lookup) {
        auto key = iter->key();
        /**
         * Need to filter result with expression if is not accurate scan.
         */
        if (requiredFilter_ && !conditionsCheck(key)) {
            iter->next();
            continue;
        }
        keys.emplace_back(key);
        iter->next();
    }
    for (auto& item : keys) {
        ret = getDataRow(part, item);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
=======
    if (ret != kvstore::SUCCEEDED) {
        return ret;
    }
    while (iter->valid() &&
           rowCount_ < FLAGS_max_row_returned_per_index_scan) {
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
    std::string prefix = NebulaKeyUtils::prefix(part) + prefix_;
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_,
                                      part,
                                      prefix,
                                      &iter);
    if (ret != kvstore::SUCCEEDED) {
        return ret;
    }
    while (iter->valid() &&
           rowCount_ < FLAGS_max_row_returned_per_index_scan) {
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
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
    }
    return ret;
}

<<<<<<< HEAD
=======
template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::seekScan(PartitionID part) {
    /**
     * TODO sky : seekScan used conditional comparison with greater than , etc.
     *            new reuse the prefixScan.
     */
    return prefixScan(part);
}

>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
template<typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::getDataRow(PartitionID partId,
                                                    const folly::StringPiece& key) {
    kvstore::ResultCode ret;
    if (isEdgeIndex_) {
        cpp2::Edge data;
        ret = getEdgeRow(partId, key, &data);
        if (ret == kvstore::SUCCEEDED) {
            edgeRows_.emplace_back(std::move(data));
<<<<<<< HEAD
            ++rowNum_;
=======
            ++rowCount_;
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
        }
    } else {
        cpp2::VertexIndexData data;
        ret = getVertexRow(partId, key, &data);
        if (ret == kvstore::SUCCEEDED) {
            vertexRows_.emplace_back(std::move(data));
<<<<<<< HEAD
            ++rowNum_;
=======
            ++rowCount_;
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
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
<<<<<<< HEAD
    if (schema_ == nullptr) {
=======
    if (!needReturnCols_) {
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
        return kvstore::ResultCode::SUCCEEDED;
    }
    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
        auto result = vertexCache_->get(std::make_pair(vId, tagOrEdge_), partId);
        if (result.ok()) {
            auto v = std::move(result).value();
            auto reader = RowReader::getTagPropReader(schemaMan_, v, spaceId_, tagOrEdge_);
            auto row = getRowFromReader(reader.get());
<<<<<<< HEAD
=======
            if (row.empty()) {
                return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
            }
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
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
<<<<<<< HEAD
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
=======
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(schemaMan_,
                                                  iter->val(),
                                                  spaceId_,
                                                  tagOrEdge_);
        auto row = getRowFromReader(reader.get());
<<<<<<< HEAD
=======
        if (row.empty()) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
        data->set_props(std::move(row));
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            vertexCache_->insert(std::make_pair(vId, tagOrEdge_),
                                 iter->val().str(), partId);
            VLOG(3) << "Insert cache for vId " << vId << ", tagId " << tagOrEdge_;
        }
    } else {
<<<<<<< HEAD
        LOG(ERROR) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagOrEdge_;
=======
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagOrEdge_;
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
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
<<<<<<< HEAD
    data->set_key(edge);
    if (schema_ == nullptr) {
=======
    if (!needReturnCols_) {
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
        return kvstore::ResultCode::SUCCEEDED;
    }
    auto prefix = NebulaKeyUtils::edgePrefix(partId, src, tagOrEdge_, rank, dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
<<<<<<< HEAD
        LOG(ERROR) << "Error! ret = "
=======
        VLOG(3) << "Error! ret = "
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
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
<<<<<<< HEAD
        data->set_props(std::move(row));
    } else {
        LOG(ERROR) << "Missed partId " << partId
                   << ", src " << src << ", edgeType "
                   << tagOrEdge_ << ", Rank "
                   << rank << ", dst " << dst;
=======
        if (row.empty()) {
            return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
        }
        data->set_props(std::move(row));
    } else {
        VLOG(3) << "Missed partId " << partId
                << ", src " << src << ", edgeType "
                << tagOrEdge_ << ", Rank "
                << rank << ", dst " << dst;
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

template<typename RESP>
std::string IndexExecutor<RESP>::getRowFromReader(RowReader* reader) {
<<<<<<< HEAD
    RowWriter writer;
    PropsCollector collector(&writer);
    this->collectProps(reader, props_, &collector);
=======
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
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
    return writer.encode();
}

template<typename RESP>
bool IndexExecutor<RESP>::conditionsCheck(const folly::StringPiece& key) {
<<<<<<< HEAD
    UNUSED(key);
    Getters getters;
    getters.getAliasProp = [this, &key](const std::string&,
                                        const std::string &prop) -> OptVariantType {
            return decodeValue(key, prop);
    };
    return exprEval(getters);
=======
    Getters getters;
    if (this->expr_ != nullptr) {
        getters.getAliasProp = [this, &key](const std::string&,
                                            const std::string &prop) -> OptVariantType {
                return decodeValue(key, prop);
        };
        auto value = this->expr_->eval(getters);
        return (value.ok() && !Expression::asBool(value.value()));
    }
    return true;
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
}

template<typename RESP>
OptVariantType IndexExecutor<RESP>::decodeValue(const folly::StringPiece& key,
                                                const folly::StringPiece& prop) {
    using nebula::cpp2::SupportedType;
    auto type = indexCols_[prop.str()];
<<<<<<< HEAD
    /**
     * Here need a string copy to avoid memory change
     */
    auto propVal = getIndexVal(key, prop).str();
    return NebulaKeyUtils::decodeVariant(std::move(propVal), type);
=======
    return NebulaKeyUtils::decodeVariant(getIndexVal(key, prop), type);
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
}

template<typename RESP>
folly::StringPiece IndexExecutor<RESP>::getIndexVal(const folly::StringPiece& key,
                                                    const folly::StringPiece& prop) {
    auto tailLen = (!isEdgeIndex_) ? sizeof(VertexID) :
                                     sizeof(VertexID) * 2 + sizeof(EdgeRanking);
    using nebula::cpp2::SupportedType;
    size_t offset = sizeof(PartitionID) + sizeof(IndexID);
<<<<<<< HEAD
    size_t len = 0;
    int32_t vCount = vColNum_;
    for (const auto& col : index_->get_fields()) {
=======
    int32_t len = 0;
    int32_t vCount = vColSize_;
    for (const auto& col : index_.get_cols()) {
>>>>>>> 1, Addressed comments. 2, Optimized index scan logic.
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
