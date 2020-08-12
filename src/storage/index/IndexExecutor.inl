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
    isEdgeIndex_ = req.get_is_edge();

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
    return ret;
}

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::buildExecutionPlan(const std::string& filter) {
    auto ret = preparePolicy(filter);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    if (!buildPolicy()) {
        return cpp2::ErrorCode::E_INVALID_FILTER;
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
            vColNum_++;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename RESP>
cpp2::ErrorCode IndexExecutor<RESP>::checkReturnColumns(const std::vector<std::string> &cols) {
    int32_t index = 0;
    if (!cols.empty()) {
        schema_ = std::make_shared<SchemaWriter>();
        auto schema = isEdgeIndex_ ?
                      schemaMan_->getEdgeSchema(spaceId_, tagOrEdge_) :
                      schemaMan_->getTagSchema(spaceId_, tagOrEdge_);
        if (!schema) {
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
            schema_->appendCol(col, std::move(ftype).get_type());
        }   // end for
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

// TODO (sky) : String range scan was disabled graph layer. it is not support in storage layer.
template <typename RESP>
std::pair<std::string, std::string>
IndexExecutor<RESP>::makeScanPair(PartitionID partId, IndexID indexId) {
    std::string beginStr = NebulaKeyUtils::indexPrefix(partId, indexId);
    std::string endStr = NebulaKeyUtils::indexPrefix(partId, indexId);
    const auto& fields = index_->get_fields();
    for (const auto& field : fields) {
        auto item = scanItems_.find(field.get_name());
        if (item == scanItems_.end()) {
            break;
        }
        // here need check the value type, maybe different data types appear.
        // for example:
        // index (c1 double)
        // where c1 > abs(1) , FunctionCallExpression->eval(abs(1))
        // should be cast type from int to double.
        bool suc = true;
        if (item->second.beginBound_.rel_ != RelationType::kNull) {
            suc = NebulaKeyUtils::checkAndCastVariant(field.get_type().type,
                                                      item->second.beginBound_.val_);
        }
        if (item->second.endBound_.rel_ != RelationType::kNull) {
            suc = NebulaKeyUtils::checkAndCastVariant(field.get_type().type,
                                                      item->second.endBound_.val_);
        }
        if (!suc) {
            VLOG(1) << "Unknown VariantType";
            return {};
        }
        auto pair = normalizeScanPair(field, (*item).second);
        beginStr.append(pair.first);
        endStr.append(pair.second);
    }
    return std::make_pair(beginStr, endStr);
}

template <typename RESP>
std::pair<std::string, std::string>
IndexExecutor<RESP>::normalizeScanPair(const nebula::cpp2::ColumnDef& field,
                                       const ScanBound& item) {
    std::string begin, end;
    auto type = field.get_type().type;
    // if begin == end, means the scan is equivalent scan.
    if (item.beginBound_.rel_ != RelationType::kGTRel &&
        item.endBound_.rel_ != RelationType::kGTRel &&
        item.beginBound_.rel_ != RelationType::kNull &&
        item.endBound_.rel_ != RelationType::kNull &&
        item.beginBound_.val_ == item.endBound_.val_) {
        begin = end = NebulaKeyUtils::encodeVariant(item.beginBound_.val_);
        return std::make_pair(begin, end);
    }
    // normalize begin and end value using ScanItem. for example :
    // 1 < c1 < 5   --> ScanItem:{(GT, 1), (LT, 5)} --> {2, 5}
    // 1 <= c1 <= 5 --> ScanItem:{(GE, 1), (LE, 5)} --> {1, 6}
    // c1 > 5 --> ScanItem:{(GT, 5), (NULL)} --> {6, max}
    // c1 >= 5 --> ScanItem:{(GE, 5), (NULL)} --> {5, max}
    // c1 < 6 --> ScanItem:{(NULL), (LT, 6)} --> {min, 6}
    // c1 <= 6 --> ScanItem:{(NULL), (LE, 6)} --> {min, 7}
    if (item.beginBound_.rel_ == RelationType::kNull) {
        begin = NebulaKeyUtils::boundVariant(type, NebulaBoundValueType::kMin);
    } else if (item.beginBound_.rel_ == RelationType::kGTRel) {
        begin = NebulaKeyUtils::boundVariant(type, NebulaBoundValueType::kAddition,
                                             item.beginBound_.val_);
    } else {
        begin = NebulaKeyUtils::encodeVariant(item.beginBound_.val_);
    }

    if (item.endBound_.rel_ == RelationType::kNull) {
        end = NebulaKeyUtils::boundVariant(type, NebulaBoundValueType::kMax);
    } else if (item.endBound_.rel_ == RelationType::kLTRel) {
        end = NebulaKeyUtils::encodeVariant(item.endBound_.val_);
    } else {
        end = NebulaKeyUtils::boundVariant(type, NebulaBoundValueType::kAddition,
                                           item.endBound_.val_);
    }
    return std::make_pair(begin, end);
}

template <typename RESP>
kvstore::ResultCode IndexExecutor<RESP>::executeExecutionPlan(PartitionID part) {
    std::unique_ptr<kvstore::KVIterator> iter;
    std::vector<std::string> keys;
    auto pair = makeScanPair(part, index_->get_index_id());
    if (pair.first.empty() || pair.second.empty()) {
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    auto ret = (pair.first == pair.second)
               ? this->doPrefix(spaceId_, part, pair.first, &iter)
               : this->doRange(spaceId_, part, pair.first, pair.second, &iter);
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
    }
    return ret;
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
    if (schema_ == nullptr) {
        return kvstore::ResultCode::SUCCEEDED;
    }
    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
        auto result = vertexCache_->get(std::make_pair(vId, tagOrEdge_));
        if (result.ok()) {
            auto v = std::move(result).value();
            auto reader = RowReader::getTagPropReader(schemaMan_, v, spaceId_, tagOrEdge_);
            if (reader == nullptr) {
                return kvstore::ResultCode::ERR_CORRUPT_DATA;
            }
            auto row = getRowFromReader(reader.get());
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
        LOG(ERROR) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(schemaMan_,
                                                  iter->val(),
                                                  spaceId_,
                                                  tagOrEdge_);
        if (reader == nullptr) {
            return kvstore::ResultCode::ERR_CORRUPT_DATA;
        }
        auto row = getRowFromReader(reader.get());
        data->set_props(std::move(row));
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            vertexCache_->insert(std::make_pair(vId, tagOrEdge_),
                                 iter->val().str());
            VLOG(3) << "Insert cache for vId " << vId << ", tagId " << tagOrEdge_;
        }
    } else {
        LOG(ERROR) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagOrEdge_;
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
    data->set_key(edge);
    if (schema_ == nullptr) {
        return kvstore::ResultCode::SUCCEEDED;
    }
    auto prefix = NebulaKeyUtils::edgePrefix(partId, src, tagOrEdge_, rank, dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Error! ret = "
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
        data->set_props(std::move(row));
    } else {
        LOG(ERROR) << "Missed partId " << partId
                   << ", src " << src << ", edgeType "
                   << tagOrEdge_ << ", Rank "
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
bool IndexExecutor<RESP>::conditionsCheck(const folly::StringPiece& key) {
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
    /**
     * Here need a string copy to avoid memory change
     */
    auto propVal = getIndexVal(key, prop).str();
    return NebulaKeyUtils::decodeVariant(std::move(propVal), type);
}

template<typename RESP>
folly::StringPiece IndexExecutor<RESP>::getIndexVal(const folly::StringPiece& key,
                                                    const folly::StringPiece& prop) {
    auto tailLen = (!isEdgeIndex_) ? sizeof(VertexID) :
                                     sizeof(VertexID) * 2 + sizeof(EdgeRanking);
    using nebula::cpp2::SupportedType;
    size_t offset = sizeof(PartitionID) + sizeof(IndexID);
    size_t len = 0;
    int32_t vCount = vColNum_;
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
