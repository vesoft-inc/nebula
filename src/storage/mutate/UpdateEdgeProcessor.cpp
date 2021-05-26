/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/UpdateEdgeProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/ConvertTimeType.h"
#include "dataman/RowWriter.h"
#include "kvstore/LogEncoder.h"
#include "meta/NebulaSchemaProvider.h"

namespace nebula {
namespace storage {

void UpdateEdgeProcessor::onProcessFinished(int32_t retNum) {
    if (retNum > 0) {
        nebula::cpp2::Schema respScheam;
        respScheam.columns.reserve(retNum);
        RowWriter writer(nullptr);
        Getters getters;
        getters.getSrcTagProp = [&, this] (const std::string& tagName,
                                        const std::string& prop) -> OptVariantType {
            auto tagRet = this->schemaMan_->toTagID(this->spaceId_, tagName);
            if (!tagRet.ok()) {
                VLOG(1) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
                return Status::Error("Invalid Filter Tag: " + tagName);
            }
            auto tagId = tagRet.value();
            auto it = tagFilters_.find(std::make_pair(tagId, prop));
            if (it == tagFilters_.end()) {
                LOG(ERROR) << "Invalid Tag Filter, tagID: " << tagId << ", propName: " << prop;
                return Status::Error("Invalid Tag Filter");
            }
            VLOG(1) << "Hit srcProp filter for tag: " << tagName
                    << ", prop: " << prop << ", value: " << it->second;
            return it->second;
        };
        getters.getAliasProp = [&, this] (const std::string&,
                                        const std::string& prop) -> OptVariantType {
            auto it = this->edgeFilters_.find(prop);
            if (it == this->edgeFilters_.end()) {
                return Status::Error("Invalid Edge Filter");
            }
            VLOG(1) << "Hit edgeProp for prop: " << prop << ", value: " << it->second;
            return it->second;
        };
        for (auto& exp : returnColumnsExp_) {
            if (!exp->prepare().ok()) {
                LOG(ERROR) << "Expression::prepare failed";
                return;
            }
            auto value = exp->eval(getters);
            if (!value.ok()) {
                LOG(ERROR) << value.status();
                return;
            }
            nebula::cpp2::ColumnDef column;
            auto v = std::move(value.value());
            switch (v.which()) {
               case VAR_INT64: {
                   writer << boost::get<int64_t>(v);
                   column = this->columnDef(std::string("anonymous"),
                                            nebula::cpp2::SupportedType::INT);
                   break;
               }
               case VAR_DOUBLE: {
                   writer << boost::get<double>(v);
                   column = this->columnDef(std::string("anonymous"),
                                            nebula::cpp2::SupportedType::DOUBLE);
                   break;
               }
               case VAR_BOOL: {
                   writer << boost::get<bool>(v);
                   column = this->columnDef(std::string("anonymous"),
                                            nebula::cpp2::SupportedType::BOOL);
                   break;
               }
               case VAR_STR: {
                   writer << boost::get<std::string>(v);
                   column = this->columnDef(std::string("anonymous"),
                                            nebula::cpp2::SupportedType::STRING);
                   break;
               }
               default: {
                   LOG(FATAL) << "Unknown VariantType: " << v.which();
               }
           }
           respScheam.columns.emplace_back(std::move(column));
        }
        resp_.set_schema(std::move(respScheam));
        resp_.set_data(writer.encode());
    }
}


kvstore::ResultCode UpdateEdgeProcessor::collectVertexProps(
                            const PartitionID partId,
                            const VertexID vId,
                            const TagID tagId,
                            const std::vector<PropContext>& props) {
    auto prefix = NebulaKeyUtils::vertexPrefix(partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                << ", spaceId " << this->spaceId_;
        return ret;
    }

    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(this->schemaMan_,
                                                  iter->val(),
                                                  this->spaceId_,
                                                  tagId);
        if (reader == nullptr) {
            LOG(WARNING) << "Can't find the schema for tagId " << tagId;
            // It offen happens after updating schema but current storaged has not
            // load it. To protect the data, we just return failed to graphd.
            return kvstore::ResultCode::ERR_UNKNOWN;
        }
        const auto schema = this->schemaMan_->getTagSchema(this->spaceId_, tagId);
        if (schema == nullptr) {
            LOG(WARNING) << "Can't find the schema for tagId " << tagId;
            return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
        }
        for (auto& prop : props) {
            VariantType v;
            auto res = RowReader::getPropByName(reader.get(), prop.prop_.name);
            if (!ok(res)) {
                auto defaultVal = schema->getDefaultValue(prop.prop_.name);
                if (!defaultVal.ok()) {
                    LOG(WARNING) << "No default value of "
                                 << tagId << ", prop " << prop.prop_.name;
                    return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
                }
                v = std::move(defaultVal).value();
            } else {
                v = value(std::move(res));
            }
            tagFilters_.emplace(std::make_pair(tagId, prop.prop_.name), v);
        }
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}


kvstore::ResultCode UpdateEdgeProcessor::collectEdgesProps(
                            const PartitionID partId,
                            const cpp2::EdgeKey& edgeKey) {
    auto prefix = NebulaKeyUtils::prefix(partId, edgeKey.src, edgeKey.edge_type,
                                         edgeKey.ranking, edgeKey.dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(this->spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret)
                << ", spaceId " << this->spaceId_;
        return ret;
    }
    // Only use the latest version.
    if (iter && iter->valid()) {
        key_ = iter->key().str();
        val_ = iter->val().str();
        auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                   val_,
                                                   this->spaceId_,
                                                   std::abs(edgeKey.edge_type));
        if (reader == nullptr) {
            LOG(WARNING) << "Can't find related edge "
                         << edgeKey.edge_type << " schema";
            // TODO(heng)
            // The case offen happens when updating the reverse edge but it is not exist.
            // Because we don't ensure the consistency when inserting edges (Bidirect).
            // So we leave the issue here. Now we just return failed to graphd.
            return kvstore::ResultCode::ERR_CORRUPT_DATA;
        }
        const auto constSchema = this->schemaMan_->getEdgeSchema(this->spaceId_,
                                                                 std::abs(edgeKey.edge_type));
        if (constSchema == nullptr) {
            LOG(ERROR) << "Can't find the schema for edge " << edgeKey.edge_type;
            return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
        }
        for (auto index = 0UL; index < constSchema->getNumFields(); index++) {
            auto propName = std::string(constSchema->getFieldName(index));
            auto res = RowReader::getPropByName(reader.get(), propName);
            VariantType v;
            if (!ok(res)) {
                auto defaultVal = constSchema->getDefaultValue(propName);
                if (!defaultVal.ok()) {
                    LOG(WARNING) << "No default value of "
                                 << edgeKey.edge_type << ", prop " << propName;
                    return kvstore::ResultCode::ERR_EDGE_NOT_FOUND;
                }
                v = std::move(defaultVal).value();
            } else {
                v = value(std::move(res));
            }
            edgeFilters_.emplace(propName, std::move(v));
        }
        updater_ = std::unique_ptr<RowUpdater>(new RowUpdater(std::move(reader), constSchema));
    } else if (insertable_) {
        resp_.set_upsert(true);
        auto version = FLAGS_enable_multi_versions ?
            std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec() : 0L;
        // Switch version to big-endian, make sure the key is in ordered.
        version = folly::Endian::big(version);
        key_ = NebulaKeyUtils::edgeKey(partId, edgeKey.src, edgeKey.edge_type,
                                       edgeKey.ranking, edgeKey.dst, version);
        const auto constSchema = this->schemaMan_->getEdgeSchema(this->spaceId_,
                                                                 std::abs(edgeKey.edge_type));
        if (constSchema == nullptr) {
            LOG(ERROR) << "Get nullptr schema";
            return kvstore::ResultCode::ERR_UNKNOWN;
        }
        auto updater = std::make_unique<RowUpdater>(constSchema);

        for (auto index = 0UL; index < constSchema->getNumFields(); index++) {
            auto propName = std::string(constSchema->getFieldName(index));
            auto findIter = std::find_if(updateItems_.cbegin(), updateItems_.cend(),
                    [&propName](auto &item) { return item.prop == propName; });
            OptVariantType value;
            if (findIter == updateItems_.end()) {
                // When the schema field is not in update field
                // need to get default value from schema. If nonexistent return error.
                value = constSchema->getDefaultValue(index);
                if (!value.ok()) {
                    LOG(ERROR) << "EdgeType: " << edgeKey.edge_type
                               << ", prop: " << propName << " without default value";
                    return kvstore::ResultCode::ERR_UNKNOWN;
                }
                auto v = std::move(value.value());
                edgeFilters_.emplace(propName, v);
            } else {
                // When the update item has src item,
                // need to get default value from schema. If nonexistent return error.
                auto expStatus = Expression::decode(findIter->get_value());
                if (!expStatus.ok()) {
                    LOG(ERROR) << "Expression decode failed, propName: " << propName;
                    return kvstore::ResultCode::ERR_UNKNOWN;
                }

                auto expression = std::move(expStatus).value();
                ExpressionContext expCtx;
                expression->setContext(&expCtx);
                if (!expression->prepare().ok()) {
                    LOG(ERROR) << "Expression::prepare failed";
                    return kvstore::ResultCode::ERR_UNKNOWN;
                }
                if (expCtx.hasEdgeProp()) {
                    auto aliasProps = expCtx.aliasProps();
                    for (auto& prop : aliasProps) {
                        value = constSchema->getDefaultValue(prop.second);
                        if (!value.ok()) {
                            LOG(ERROR) << "EdgeType: " << edgeKey.edge_type
                                       << ", prop: " << prop.second << " without default value";
                            return kvstore::ResultCode::ERR_UNKNOWN;
                        }
                        VLOG(2) << "UpdateItem on propName: " << prop.second << " has edge prop";
                        auto v = std::move(value.value());
                        edgeFilters_.emplace(prop.second, v);
                    }
                } else {
                    VLOG(2) << "Nothing set on propName: " << propName;
                }
            }
        }
        updater_ = std::unique_ptr<RowUpdater>(std::move(updater));
    } else {
        VLOG(3) << "Missed edge, spaceId: " << this->spaceId_ << ", partId: " << partId
                << ", src: " << edgeKey.src << ", type: " << edgeKey.edge_type
                << ", ranking: " << edgeKey.ranking << ", dst: "<< edgeKey.dst;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}


folly::Optional<std::string> UpdateEdgeProcessor::updateAndWriteBack(PartitionID partId,
                                                                     const cpp2::EdgeKey& edgeKey) {
    Getters getters;
    getters.getSrcTagProp = [&, this] (const std::string& tagName,
                                       const std::string& prop) -> OptVariantType {
        auto tagRet = this->schemaMan_->toTagID(this->spaceId_, tagName);
        if (!tagRet.ok()) {
            VLOG(1) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
            return Status::Error("Invalid Filter Tag: " + tagName);
        }
        auto tagId = tagRet.value();
        auto it = tagFilters_.find(std::make_pair(tagId, prop));
        if (it == tagFilters_.end()) {
            return Status::Error("Invalid Tag Filter");
        }
        VLOG(1) << "Hit srcProp filter for tag: " << tagName
                << ", prop: " << prop << ", value: " << it->second;
        return it->second;
    };
    getters.getAliasProp = [&, this] (const std::string&,
                                      const std::string& prop) -> OptVariantType {
        auto it = this->edgeFilters_.find(prop);
        if (it == this->edgeFilters_.end()) {
            return Status::Error("Invalid Edge Filter");
        }
        VLOG(1) << "Hit edgeProp for prop: " << prop << ", value: " << it->second;
        return it->second;
    };
    for (auto& item : updateItems_) {
        auto prop = item.get_prop();
        auto exp = Expression::decode(item.get_value());
        if (!exp.ok()) {
            LOG(ERROR) << "Decode item expr failed";
            return folly::none;
        }
        auto vexp = std::move(exp).value();
        vexp->setContext(this->expCtx_.get());
        if (!vexp->prepare().ok()) {
            LOG(ERROR) << "Expression::prepare failed";
            return folly::none;
        }
        auto value = vexp->eval(getters);
        if (!value.ok()) {
            LOG(ERROR) << "Eval item expr failed";
            return folly::none;
        }
        auto expValue = value.value();
        edgeFilters_[prop] = expValue;

        auto schema = updater_->schema();
        switch (expValue.which()) {
            case VAR_INT64: {
                if (schema->getFieldType(prop).type != nebula::cpp2::SupportedType::INT &&
                    schema->getFieldType(prop).type != nebula::cpp2::SupportedType::TIMESTAMP) {
                    LOG(ERROR) << "Field: `" << prop << "' type is "
                               << static_cast<int32_t>(schema->getFieldType(prop).type)
                               << ", not INT type or TIMESTAMP";
                    return folly::none;
                }
                auto v = boost::get<int64_t>(expValue);
                updater_->setInt(prop, v);
                break;
            }
            case VAR_DOUBLE: {
                if (schema->getFieldType(prop).type != nebula::cpp2::SupportedType::DOUBLE) {
                    LOG(ERROR) << "Field: `" << prop << "' type is "
                               << static_cast<int32_t>(schema->getFieldType(prop).type)
                               << ", not DOUBLE type";
                    return folly::none;
                }
                auto v = boost::get<double>(expValue);
                updater_->setDouble(prop, v);
                break;
            }
            case VAR_BOOL: {
                if (schema->getFieldType(prop).type != nebula::cpp2::SupportedType::BOOL) {
                    LOG(ERROR) << "Field: `" << prop << "' type is "
                               << static_cast<int32_t>(schema->getFieldType(prop).type)
                               << ", not BOOL type";
                    return folly::none;
                }
                auto v = boost::get<bool>(expValue);
                updater_->setBool(prop, v);
                break;
            }
            case VAR_STR: {
                auto v = boost::get<std::string>(expValue);
                if (schema->getFieldType(prop).type == nebula::cpp2::SupportedType::TIMESTAMP) {
                    auto timestamp = ConvertTimeType::toTimestamp(v);
                    if (!timestamp.ok()) {
                        LOG(ERROR) << "Field: `" << prop
                                   << " with wrong type: " << timestamp.status();
                        return folly::none;
                    }

                    updater_->setInt(prop, timestamp.value());
                    edgeFilters_[prop] = timestamp.value();
                } else {
                    if (schema->getFieldType(prop).type != nebula::cpp2::SupportedType::STRING) {
                        LOG(ERROR) << "Field: `" << prop << "' type is "
                                   << static_cast<int32_t>(schema->getFieldType(prop).type)
                                   << ", not STRING type";
                        return folly::none;
                    }
                    updater_->setString(prop, v);
                }
                break;
             }
            default: {
                LOG(ERROR) << "Unknown VariantType: " << expValue.which();
                return folly::none;
            }
        }
    }
    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    auto status = updater_->encode();
    if (!status.ok()) {
        LOG(ERROR) << status.status();
        return folly::none;
    }
    auto nVal = std::move(status.value());
    // TODO(heng) we don't update the index for reverse edge.
    if (!indexes_.empty() && edgeKey.edge_type > 0) {
        RowReader reader = RowReader::getEmptyRowReader();
        RowReader rReader = RowReader::getEmptyRowReader();
        for (auto& index : indexes_) {
            auto indexId = index->get_index_id();
            if (index->get_schema_id().get_edge_type() == edgeKey.edge_type) {
                if (!val_.empty()) {
                    if (rReader == nullptr) {
                        rReader = RowReader::getEdgePropReader(this->schemaMan_,
                                                               val_,
                                                               spaceId_,
                                                               edgeKey.edge_type);
                    }
                    auto rValues = collectIndexValues(rReader.get(),
                                                      index->get_fields());
                    if (rValues.ok()) {
                        auto rIndexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                                      indexId,
                                                                      edgeKey.src,
                                                                      edgeKey.ranking,
                                                                      edgeKey.dst,
                                                                      rValues.value());
                        batchHolder->remove(std::move(rIndexKey));
                    }
                }
                if (reader == nullptr) {
                    reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                          nVal,
                                                          this->spaceId_,
                                                          edgeKey.edge_type);
                }

                auto values = collectIndexValues(reader.get(),
                                                 index->get_fields());
                if (values.ok()) {
                    auto indexKey = NebulaKeyUtils::edgeIndexKey(partId,
                                                                 indexId,
                                                                 edgeKey.src,
                                                                 edgeKey.ranking,
                                                                 edgeKey.dst,
                                                                 values.value());
                    batchHolder->put(std::move(indexKey), "");
                }
            }
        }
    }
    batchHolder->put(std::move(key_), std::move(nVal));
    return encodeBatchValue(batchHolder->getBatch());
}


cpp2::ErrorCode UpdateEdgeProcessor::checkFilter(const PartitionID partId,
                                                 const cpp2::EdgeKey& edgeKey) {
    auto ret = collectEdgesProps(partId, edgeKey);
    if (ret == kvstore::ResultCode::ERR_CORRUPT_DATA) {
        return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
    } else if (ret != kvstore::ResultCode::SUCCEEDED) {
        return to(ret);
    }
    for (auto& tc : this->tagContexts_) {
        VLOG(3) << "partId " << partId << ", vId " << edgeKey.src
                << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
        ret = collectVertexProps(partId, edgeKey.src, tc.tagId_, tc.props_);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return to(ret);
        }
    }

    Getters getters;
    getters.getSrcTagProp = [&, this] (const std::string& tagName,
                                       const std::string& prop) -> OptVariantType {
        auto tagRet = this->schemaMan_->toTagID(this->spaceId_, tagName);
        if (!tagRet.ok()) {
            VLOG(1) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
            return Status::Error("Invalid Filter Tag: " + tagName);
        }
        auto tagId = tagRet.value();
        auto it = tagFilters_.find(std::make_pair(tagId, prop));
        if (it == tagFilters_.end()) {
            return Status::Error("Invalid Tag Filter");
        }
        VLOG(1) << "Hit srcProp filter for tag: " << tagName
                << ", prop: " << prop << ", value: " << it->second;
        return it->second;
    };
    getters.getAliasProp = [&, this] (const std::string&,
                                      const std::string& prop) -> OptVariantType {
        auto it = this->edgeFilters_.find(prop);
        if (it == this->edgeFilters_.end()) {
            return Status::Error("Invalid Edge Filter");
        }
        VLOG(1) << "Hit edgeProp for prop: " << prop << ", value: " << it->second;
        return it->second;
    };

    if (!resp_.upsert && this->exp_ != nullptr) {
        if (!this->exp_->prepare().ok()) {
            LOG(ERROR) << "Expression::prepare failed";
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        auto filterResult = this->exp_->eval(getters);
        if (!filterResult.ok()) {
            VLOG(1) << "Invalid filter expression";
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        if (!Expression::asBool(filterResult.value())) {
            VLOG(1) << "Filter skips the update";
            return cpp2::ErrorCode::E_FILTER_OUT;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}


cpp2::ErrorCode UpdateEdgeProcessor::checkAndBuildContexts(
        const cpp2::UpdateEdgeRequest& req) {
    if (this->expCtx_ == nullptr) {
        this->expCtx_ = std::make_unique<ExpressionContext>();
    }
    // return columns
    for (auto& col : req.get_return_columns()) {
        auto colExpRet = Expression::decode(col);
        if (!colExpRet.ok()) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto colExp = std::move(colExpRet).value();
        colExp->setContext(this->expCtx_.get());
        auto status = colExp->prepare();
        if (!status.ok() || !this->checkExp(colExp.get())) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        returnColumnsExp_.emplace_back(std::move(colExp));
    }
    // filter(where/when)
    const auto& filterStr = req.get_filter();
    if (!filterStr.empty()) {
        auto expRet = Expression::decode(filterStr);
        if (!expRet.ok()) {
            VLOG(1) << "Can't decode the filter " << filterStr;
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        this->exp_ = std::move(expRet).value();
        this->exp_->setContext(this->expCtx_.get());
        auto status = this->exp_->prepare();
        if (!status.ok() || !this->checkExp(this->exp_.get())) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
    }
    // build context of the update items
    for (auto& item : req.get_update_items()) {
        AliasPropertyExpression edgePropExp(new std::string(""),
                                            new std::string(item.get_name()),
                                            new std::string(item.get_prop()));
        edgePropExp.setContext(this->expCtx_.get());
        auto status = edgePropExp.prepare();
        if (!status.ok() || !this->checkExp(&edgePropExp)) {
            VLOG(1) << "Invalid item expression!";
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto exp = Expression::decode(item.get_value());
        if (!exp.ok()) {
            VLOG(1) << "Can't decode the item's value " << item.get_value();
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto vexp = std::move(exp).value();
        vexp->setContext(this->expCtx_.get());
        status = vexp->prepare();
        if (!status.ok() || !this->checkExp(vexp.get())) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
    }
    if (this->expCtx_->hasDstTagProp() || this->expCtx_->hasVariableProp()
        || this->expCtx_->hasInputProp()) {
        LOG(ERROR) << "should only contain SrcTagProp or EdgeProp expression!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}


void UpdateEdgeProcessor::process(const cpp2::UpdateEdgeRequest& req) {
    VLOG(3) << "Receive UpdateEdgeRequest...";
    this->spaceId_ = req.get_space_id();
    insertable_ = req.get_insertable();
    auto partId = req.get_part_id();
    auto edgeKey = req.get_edge_key();
    std::vector<EdgeType> eTypes;
    eTypes.emplace_back(edgeKey.get_edge_type());
    this->initEdgeContext(eTypes);
    auto retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Failure build contexts!";
        this->pushResultCode(retCode, partId);
        this->onFinished();
        return;
    }
    updateItems_ = req.get_update_items();

    auto iRet = indexMan_->getEdgeIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    VLOG(3) << "Update edge, spaceId: " << this->spaceId_ << ", partId:  " << partId
            << ", src: " << edgeKey.get_src() << ", edge_type: " << edgeKey.get_edge_type()
            << ", dst: " << edgeKey.get_dst() << ", ranking: " << edgeKey.get_ranking();
    CHECK_NOTNULL(kvstore_);
    this->kvstore_->asyncAtomicOp(this->spaceId_, partId,
        [partId, edgeKey, this] () -> folly::Optional<std::string> {
            // TODO(shylock) the AtomicOP can't return various error
            // so put it in the processor
            filterResult_ = checkFilter(partId, edgeKey);
            if (filterResult_ == cpp2::ErrorCode::SUCCEEDED) {
                return updateAndWriteBack(partId, edgeKey);
            } else {
                return folly::none;
            }
        },
        [this, partId, edgeKey, req] (kvstore::ResultCode code) {
            while (true) {
                if (code == kvstore::ResultCode::SUCCEEDED) {
                    onProcessFinished(req.get_return_columns().size());
                    break;
                }
                LOG(ERROR) << "Fail to update edge, spaceId: " << this->spaceId_
                           << ", partId: " << partId
                           << ", src: " << edgeKey.get_src()
                           << ", edge_type: " << edgeKey.get_edge_type()
                           << ", dst: " << edgeKey.get_dst()
                           << ", ranking: " << edgeKey.get_ranking();
                if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                    handleLeaderChanged(this->spaceId_, partId);
                    break;
                }
                if (code == kvstore::ResultCode::ERR_ATOMIC_OP_FAILED) {
                    // https://github.com/vesoft-inc/nebula/issues/1888
                    // Only filter out so we still return the data
                    if (filterResult_ == cpp2::ErrorCode::E_FILTER_OUT) {
                        onProcessFinished(req.get_return_columns().size());
                    }
                    if (filterResult_ != cpp2::ErrorCode::SUCCEEDED) {
                        this->pushResultCode(filterResult_, partId);
                    } else {
                        this->pushResultCode(to(code), partId);
                    }
                } else {
                    this->pushResultCode(to(code), partId);
                }
                break;
            }
            this->onFinished();
        });
}

}  // namespace storage
}  // namespace nebula
