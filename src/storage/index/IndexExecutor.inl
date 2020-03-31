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
cpp2::ErrorCode IndexExecutor<RESP>::prepareRequest(const cpp2::LookUpIndexRequest &req) {
    spaceId_ = req.get_space_id();

    tagOrEdgeId_ = req.get_tag_or_edge_id();

    const auto& hints = req.get_hints();
    for (const auto& hint : hints) {
        auto ret = buildExecutionPlan(hint);
        if (ret != cpp2::ErrorCode::SUCCEEDED) {
            LOG(ERROR) << "Build execution plan error , index id : " << hint.get_index_id();
            return ret;
        }
    }

    /**
    * step 2 , check the return columns, and create schema for row binary.
    */
    if (req.__isset.return_columns) {
        return checkReturnColumns(*req.get_return_columns());
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::buildExecutionPlan(const nebula::cpp2::Hint& hint) {
    auto hintId = hint.get_hint_id();
    auto indexId = hint.get_index_id();

    /**
     * If indexId == -1 , means this scan is a full table data scan. Is not a index can.
     */
    if (indexId == -1) {
        auto executionPlan = std::make_unique<ExecutionPlan>();
        if (hint.__isset.filter && !hint.get_filter().empty()) {
            auto expRet = executionPlan->decodeExp(hint.get_filter());
            if (expRet != cpp2::ErrorCode::SUCCEEDED) {
                return expRet;
            }
        }
        executionPlans_[hintId] = std::move(executionPlan);
        return cpp2::ErrorCode::SUCCEEDED;
    }

    StatusOr<std::shared_ptr<nebula::cpp2::IndexItem>> index;
    if (isEdgeIndex_) {
        index = indexMan_->getEdgeIndex(spaceId_, indexId);
    } else {
        index = indexMan_->getTagIndex(spaceId_, indexId);
    }
    if (!index.ok()) {
        return cpp2::ErrorCode::E_INDEX_NOT_FOUND;
    }
    std::map<std::string, nebula::cpp2::SupportedType> indexCols;
    for (const auto& col : index.value()->get_fields()) {
        indexCols[col.get_name()] = col.get_type().get_type();
    }

    auto columnHints = hint.get_column_hints();
    auto executionPlan = std::make_unique<ExecutionPlan>(std::move(index).value(),
                                                         std::move(indexCols),
                                                         std::move(columnHints));
    if (hint.__isset.filter && !hint.get_filter().empty()) {
        auto expRet = executionPlan->decodeExp(hint.get_filter());
        if (expRet != cpp2::ErrorCode::SUCCEEDED) {
            return expRet;
        }
    }
    executionPlans_[hintId] = std::move(executionPlan);

    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::checkReturnColumns(const std::vector<std::string> &cols) {
    int32_t index = 0;
    if (!cols.empty()) {
        resultSchema_ = std::make_shared<SchemaWriter>();
        auto schema = isEdgeIndex_ ?
                      this->schemaMan_->getEdgeSchema(spaceId_, tagOrEdgeId_) :
                      this->schemaMan_->getTagSchema(spaceId_, tagOrEdgeId_);
        if (!schema) {
            LOG(ERROR) << "Can't find schema, spaceId "
                       << spaceId_ << ", id " << tagOrEdgeId_;
            return isEdgeIndex_ ? cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND :
                                  cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
        }

        for (auto i = 0; i < static_cast<int64_t>(resultSchema_->getNumFields()); i++) {
            PropContext prop;
            const auto& name = resultSchema_->getFieldName(i);
            auto ftype = resultSchema_->getFieldType(name);
            prop.type_ = ftype;
            prop.retIndex_ = index++;
            storage::cpp2::PropDef pd;
            pd.set_name(name);
            prop.prop_ = std::move(pd);
            prop.returned_ = true;
            props_.emplace_back(std::move(prop));
        }
        for (auto& col : cols) {
            const auto& ftype = schema->getFieldType(col);
            if (UNLIKELY(ftype == CommonConstants::kInvalidValueType())) {
                return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
            }
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
            resultSchema_->appendCol(col, std::move(ftype).get_type());
        }   // end for
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeExecutionPlan(PartitionID part) {
    for (const auto& executionPlan : executionPlans_) {
        switch (executionPlan.second->scanType()) {
            case ExecutionPlan::ScanType::INDEX_SCAN : {
                auto ret = executeIndexScan(executionPlan.first, part);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    return ret;
                }
                break;
            }
            case ExecutionPlan::ScanType::DATA_SCAN : {
                auto ret = executeDataScan(executionPlan.first, part);
                if (ret != kvstore::ResultCode::SUCCEEDED) {
                    return ret;
                }
                break;
            }
        }
    }
    return nebula::kvstore::SUCCEEDED;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeIndexScan(int32_t hintId, PartitionID part) {
    const auto& hint = executionPlans_[hintId];
    if (hint->isRangeScan()) {
        return executeIndexRangeScan(hintId, part);
    } else {
        return executeIndexPrefixScan(hintId, part);
    }
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeIndexRangeScan(int32_t hintId, PartitionID part) {
    const auto& hint = executionPlans_[hintId];
    auto rangeRet = hint->getRangeStartStr(part);
    if (!rangeRet.ok()) {
        return kvstore::ResultCode::ERR_UNKNOWN;
    }
    auto rang = std::move(rangeRet).value();
    std::unique_ptr<kvstore::KVIterator> iter;
    std::vector<std::string> keys;
    auto ret = this->kvstore_->range(spaceId_,
                                      part,
                                      rang.first,
                                      rang.second,
                                      &iter);
    if (ret != nebula::kvstore::SUCCEEDED) {
        return ret;
    }
    while (iter->valid() &&
           rowNum_ < FLAGS_max_rows_returned_per_lookup) {
        auto key = iter->key();
        /**
         * Need to filter result with expression if is not accurate scan.
         */
        if (hint->getExp() != nullptr &&  !conditionsCheck(hintId, key)) {
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
    }
    return ret;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeIndexPrefixScan(int32_t hintId, PartitionID part) {
    const auto& hint = executionPlans_[hintId];
    auto prefixRet = hint->getPrefixStr(part);
    if (!prefixRet.ok()) {
        return kvstore::ResultCode::ERR_UNKNOWN;
    }
    std::unique_ptr<kvstore::KVIterator> iter;
    std::vector<std::string> keys;
    auto ret = this->kvstore_->prefix(spaceId_,
                                      part,
                                      prefixRet.value(),
                                      &iter);
    if (ret != nebula::kvstore::SUCCEEDED) {
        return ret;
    }
    while (iter->valid() &&
           rowNum_ < FLAGS_max_rows_returned_per_lookup) {
        auto key = iter->key();
        /**
         * Need to filter result with expression if is not accurate scan.
         */
        if (hint->getExp() != nullptr &&  !conditionsCheck(hintId, key)) {
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
    }
    return ret;
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeDataScan(int32_t, PartitionID) {
    return kvstore::ResultCode::ERR_UNSUPPORTED;
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
            ++rowNum_;
        }
    } else {
        cpp2::VertexIndexData data;
        ret = getVertexRow(partId, key, &data);
        if (ret == kvstore::SUCCEEDED) {
            vertexRows_.emplace_back(std::move(data));
            ++rowNum_;
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
    if (resultSchema_ == nullptr) {
        return kvstore::ResultCode::SUCCEEDED;
    }
    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
        auto result = vertexCache_->get(std::make_pair(vId, tagOrEdgeId_), partId);
        if (result.ok()) {
            auto v = std::move(result).value();
            auto reader = RowReader::getTagPropReader(this->schemaMan_, v, spaceId_, tagOrEdgeId_);
            auto row = getRowFromReader(reader.get());
            data->set_props(std::move(row));
            VLOG(3) << "Hit cache for vId " << vId << ", tagId " << tagOrEdgeId_;
            return kvstore::ResultCode::SUCCEEDED;
        } else {
            VLOG(3) << "Miss cache for vId " << vId << ", tagId " << tagOrEdgeId_;
        }
    }
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, tagOrEdgeId_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(this->schemaMan_,
                                                  iter->val(),
                                                  spaceId_,
                                                  tagOrEdgeId_);
        auto row = getRowFromReader(reader.get());
        data->set_props(std::move(row));
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            vertexCache_->insert(std::make_pair(vId, tagOrEdgeId_),
                                 iter->val().str(), partId);
            VLOG(3) << "Insert cache for vId " << vId << ", tagId " << tagOrEdgeId_;
        }
    } else {
        LOG(ERROR) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagOrEdgeId_;
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
    edge.set_edge_type(tagOrEdgeId_);
    edge.set_ranking(rank);
    edge.set_dst(dst);
    data->set_key(edge);
    if (resultSchema_ == nullptr) {
        return kvstore::ResultCode::SUCCEEDED;
    }
    auto prefix = NebulaKeyUtils::edgePrefix(partId, src, tagOrEdgeId_, rank, dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = "
                << static_cast<int32_t>(ret)
                << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                   iter->val(),
                                                   spaceId_,
                                                   tagOrEdgeId_);
        auto row = getRowFromReader(reader.get());
        data->set_props(std::move(row));
    } else {
        LOG(ERROR) << "Missed partId " << partId
                   << ", src " << src << ", edgeType "
                   << tagOrEdgeId_ << ", Rank "
                   << rank << ", dst " << dst;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

template<typename RESP>
std::string IndexExecutor<RESP>::getRowFromReader(RowReader* reader) {
    RowWriter writer;
    PropsCollector collector(&writer);
    this->collectProps(reader, props_, &collector);
    return writer.encode();
}

template<typename RESP>
bool IndexExecutor<RESP>::conditionsCheck(int32_t hintId, const folly::StringPiece& key) {
    Getters getters;
    getters.getAliasProp = [this, &key, &hintId](const std::string&,
                                                 const std::string &prop) -> OptVariantType {
            return decodeValue(hintId, key, prop);
    };
    return exprEval(hintId, getters);
}

template<typename RESP>
bool IndexExecutor<RESP>::exprEval(int32_t hintId, Getters &getters) {
    if (executionPlans_[hintId]->getExp() != nullptr) {
        auto value = executionPlans_[hintId]->getExp()->eval(getters);
        return (value.ok() && Expression::asBool(value.value()));
    }
    return true;
}

template<typename RESP>
OptVariantType IndexExecutor<RESP>::decodeValue(int32_t hintId,
                                                const folly::StringPiece& key,
                                                const folly::StringPiece& prop) {
    using nebula::cpp2::SupportedType;
    auto type = executionPlans_[hintId]->getIndexColType(prop);
    /**
     * Here need a string copy to avoid memory change
     */
    auto propVal = getIndexVal(hintId, key, prop).str();
    return NebulaKeyUtils::decodeVariant(std::move(propVal), type);
}

template<typename RESP>
folly::StringPiece IndexExecutor<RESP>::getIndexVal(int32_t hintId,
                                                    const folly::StringPiece& key,
                                                    const folly::StringPiece& prop) {
    auto tailLen = (!isEdgeIndex_) ? sizeof(VertexID) :
                                     sizeof(VertexID) * 2 + sizeof(EdgeRanking);
    using nebula::cpp2::SupportedType;
    size_t offset = sizeof(PartitionID) + sizeof(IndexID);
    size_t len = 0;
    int32_t vCount = vColNum_;
    for (const auto& col : executionPlans_[hintId]->getIndex()->get_fields()) {
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
