/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/mutate/UpdateVertexProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include "utils/ConvertTimeType.h"
#include "dataman/RowWriter.h"
#include "kvstore/LogEncoder.h"
#include "meta/NebulaSchemaProvider.h"

namespace nebula {
namespace storage {

void UpdateVertexProcessor::onProcessFinished(int32_t retNum) {
    if (retNum > 0) {
        nebula::cpp2::Schema respScheam;
        respScheam.columns.reserve(retNum);
        RowWriter writer(nullptr);
        Getters getters;
        getters.getSrcTagProp = [this] (const std::string& tagName,
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
        for (auto& exp : returnColumnsExp_) {
            auto value = exp->eval(getters);
            if (!exp->prepare().ok()) {
                LOG(ERROR) << "Expression::prepare failed";
                return;
            }
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
                   return;
               }
           }
           respScheam.columns.emplace_back(std::move(column));
        }
        resp_.set_schema(std::move(respScheam));
        resp_.set_data(writer.encode());
    }
}

kvstore::ResultCode UpdateVertexProcessor::checkField(const std::string& tagName,
                                                      const std::string& propName) {
    auto tagRet = this->schemaMan_->toTagID(spaceId_, tagName);
    if (!tagRet.ok()) {
        VLOG(3) << "toTagID failed, tagName: " << tagName << ", in space " << spaceId_;
        return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
    }
    tagId_ = tagRet.value();
    schema_ = this->schemaMan_->getTagSchema(spaceId_, tagId_).get();
    if (schema_ == nullptr) {
        VLOG(3) << "Can't find the schema for tagId " << tagId_;
        return kvstore::ResultCode::ERR_TAG_NOT_FOUND;
    }

    auto field = schema_->field(propName);
    if (!field) {
        VLOG(3) << "Can't find the schema prop  " << propName
                << " in tag " << tagName;
        return kvstore::ResultCode::ERR_UNKNOWN;
    }

    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode UpdateVertexProcessor::checkAndGetDefault(const std::string& tagName,
                                                              const std::string& propName) {
    auto ret = checkField(tagName, propName);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }

    auto value = schema_->getDefaultValue(propName);
    if (!value.ok()) {
        VLOG(3) << "TagId: " << tagId_ << ", prop: " << propName
                << " without default value";
        return kvstore::ResultCode::ERR_UNKNOWN;
    }
    auto v = std::move(value.value());
    tagFilters_.emplace(std::make_pair(tagId_, propName), v);
    return kvstore::ResultCode::SUCCEEDED;
}

kvstore::ResultCode UpdateVertexProcessor::collectVertexProps(
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

    auto status = this->schemaMan_->toTagName(this->spaceId_, tagId);
    if (!status.ok()) {
        LOG(ERROR) << "toTagName failed, tagId: " << tagId;
        return kvstore::ResultCode::ERR_UNKNOWN;
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
            return kvstore::ResultCode::ERR_CORRUPT_DATA;
        }
        const auto schema = this->schemaMan_->getTagSchema(this->spaceId_, tagId);
        if (schema == nullptr) {
            LOG(ERROR) << "Can't find the schema for tagId " << tagId;
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
        if (updateTagIds_.find(tagId) != updateTagIds_.end()) {
            auto updater =
                    std::make_unique<RowUpdater>(std::move(reader), schema);
            tagUpdaters_[tagId] = std::make_unique<KeyUpdaterPair>();
            auto& tagUpdater = tagUpdaters_[tagId];
            tagUpdater->kv = std::make_pair(iter->key().str(), iter->val().str());
            tagUpdater->updater = std::move(updater);
        }
    } else if (insertable_ && updateTagIds_.find(tagId) != updateTagIds_.end()) {
        resp_.set_upsert(true);
        const auto schema = this->schemaMan_->getTagSchema(this->spaceId_, tagId);
        if (schema == nullptr) {
            LOG(ERROR) << "Get nullptr schema";
            return kvstore::ResultCode::ERR_UNKNOWN;
        }
        auto tagStatus = this->schemaMan_->toTagName(this->spaceId_, tagId);
        if (!tagStatus.ok()) {
            LOG(ERROR) << "toTagName failed, tagId: " << tagId;
            return kvstore::ResultCode::ERR_UNKNOWN;
        }
        auto tagName = std::move(tagStatus.value());

        auto updater = std::make_unique<RowUpdater>(schema);

        // When the schema field is not in update field or the update item has src item,
        // need to get default value from schema. If nonexistent return error.
        std::unordered_set<std::pair<std::string, std::string>> checkedProp;
        // check depPropMap_ in set clause
        // this props must have default value or nullable, or set int UpdateItems_
        for (auto& prop : depPropMap_) {
            for (auto& p : prop.second) {
                auto it = checkedProp.find(p);
                if (it == checkedProp.end()) {
                    ret = checkAndGetDefault(p.first, p.second);
                    if (ret != kvstore::ResultCode::SUCCEEDED) {
                        return kvstore::ResultCode::ERR_UNKNOWN;
                    }
                    checkedProp.emplace(p);
                }
            }

            // set field not need default value or nullable
            ret = checkField(prop.first.first, prop.first.second);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return kvstore::ResultCode::ERR_UNKNOWN;
            }
            checkedProp.emplace(prop.first);
        }

        // props not in set clause must have default value or nullable
        for (auto index = 0UL; index < schema->getNumFields(); index++) {
            auto propName = std::string(schema->getFieldName(index));
            auto propIter = checkedProp.find(std::make_pair(tagName, propName));
            if (propIter == checkedProp.end()) {
                auto value = schema->getDefaultValue(propName);
                if (!value.ok()) {
                    LOG(ERROR) << "TagId: " << tagId << ", prop: " << propName
                               << " without default value";
                    return kvstore::ResultCode::ERR_UNKNOWN;
                }
                auto v = std::move(value.value());
                tagFilters_.emplace(std::make_pair(tagId, propName), v);
            }
        }

        tagUpdaters_[tagId] = std::make_unique<KeyUpdaterPair>();
        auto& tagUpdater = tagUpdaters_[tagId];
        auto version = FLAGS_enable_multi_versions ?
            std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec() : 0L;
        // Switch version to big-endian, make sure the key is in ordered.
        version = folly::Endian::big(version);
        auto key = NebulaKeyUtils::vertexKey(partId, vId, tagId, version);
        tagUpdater->kv = std::make_pair(std::move(key), "");
        tagUpdater->updater = std::move(updater);
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}


cpp2::ErrorCode UpdateVertexProcessor::checkFilter(const PartitionID partId, const VertexID vId) {
    for (auto& tc : this->tagContexts_) {
        VLOG(3) << "partId " << partId << ", vId " << vId
                << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
        auto ret = collectVertexProps(partId, vId, tc.tagId_, tc.props_);
        if (ret == kvstore::ResultCode::ERR_CORRUPT_DATA) {
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        } else if (ret != kvstore::ResultCode::SUCCEEDED) {
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
            LOG(ERROR) << "Invalid Tag Filter, tagID: " << tagId << ", propName: " << prop;
            return Status::Error("Invalid Tag Filter");
        }
        VLOG(1) << "Hit srcProp filter for tag: " << tagName
                << ", prop: " << prop << ", value: " << it->second;
        return it->second;
    };

    if (!resp_.upsert && this->exp_ != nullptr) {
        auto filterResult = this->exp_->eval(getters);
        if (!this->exp_->prepare().ok()) {
            LOG(ERROR) << "Expression::prepare failed";
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        if (!filterResult.ok()) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        if (!Expression::asBool(filterResult.value())) {
            VLOG(1) << "Filter skips the update";
            return cpp2::ErrorCode::E_FILTER_OUT;
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}


folly::Optional<std::string> UpdateVertexProcessor::updateAndWriteBack(const PartitionID partId,
                                                                       const VertexID vId) {
    Getters getters;
    getters.getSrcTagProp = [this] (const std::string& tagName,
                                       const std::string& prop) -> OptVariantType {
        auto tagRet = this->schemaMan_->toTagID(this->spaceId_, tagName);
        if (!tagRet.ok()) {
            LOG(ERROR) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
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

    for (auto& item : updateItems_) {
        const auto &tagName = item.get_name();
        auto tagRet = this->schemaMan_->toTagID(this->spaceId_, tagName);
        if (!tagRet.ok()) {
            LOG(ERROR) << "Can't find tag " << tagName << ", in space " << this->spaceId_;
            return folly::none;
        }
        auto tagId = tagRet.value();
        auto prop = item.get_prop();
        auto exp = Expression::decode(item.get_value());
        if (!exp.ok()) {
            LOG(ERROR) << exp.status();
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
            LOG(ERROR) << value.status();
            return folly::none;
        }
        auto expValue = value.value();
        tagFilters_[std::make_pair(tagId, prop)] = expValue;

        auto schema = tagUpdaters_[tagId]->updater->schema();
        if (schema == nullptr) {
            LOG(ERROR) << "Get schema from updater is nullptr";
            return folly::none;
        }
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
                tagUpdaters_[tagId]->updater->setInt(prop, v);
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
                tagUpdaters_[tagId]->updater->setDouble(prop, v);
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
                tagUpdaters_[tagId]->updater->setBool(prop, v);
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
                    tagUpdaters_[tagId]->updater->setInt(prop, timestamp.value());
                    tagFilters_[std::make_pair(tagId, prop)] = timestamp.value();
                } else {
                    if (schema->getFieldType(prop).type != nebula::cpp2::SupportedType::STRING) {
                        LOG(ERROR) << "Field: `" << prop << "' type is "
                                   << static_cast<int32_t>(schema->getFieldType(prop).type)
                                   << ", not STRING type";
                        return folly::none;
                    }
                    tagUpdaters_[tagId]->updater->setString(prop, v);
                }
                break;
             }
            default: {
                LOG(FATAL) << "Unknown VariantType: " << expValue.which();
                return folly::none;
            }
        }
    }

    std::unique_ptr<kvstore::BatchHolder> batchHolder = std::make_unique<kvstore::BatchHolder>();
    for (const auto& u : tagUpdaters_) {
        auto nKey = u.second->kv.first;
        auto status = u.second->updater->encode();
        if (!status.ok()) {
            LOG(ERROR) << status.status();
            return std::string("");
        }
        auto nVal = std::move(status.value());
        if (!indexes_.empty()) {
            RowReader reader = RowReader::getEmptyRowReader();
            RowReader oReader = RowReader::getEmptyRowReader();
            for (auto &index : indexes_) {
                if (index->get_schema_id().get_tag_id() == u.first) {
                    if (!(u.second->kv.second.empty())) {
                        if (oReader == nullptr) {
                            oReader = RowReader::getTagPropReader(this->schemaMan_,
                                                                  u.second->kv.second,
                                                                  spaceId_,
                                                                  u.first);
                        }
                        const auto &oCols = index->get_fields();
                        auto oValues = collectIndexValues(oReader.get(), oCols);
                        if (oValues.ok()) {
                            auto oIndexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                                            index->index_id,
                                                                            vId,
                                                                            oValues.value());
                            batchHolder->remove(std::move(oIndexKey));
                        }
                    }
                    if (reader == nullptr) {
                        reader = RowReader::getTagPropReader(this->schemaMan_,
                                                             nVal,
                                                             spaceId_,
                                                             u.first);
                    }
                    const auto &cols = index->get_fields();
                    auto values = collectIndexValues(reader.get(), cols);
                    if (values.ok()) {
                        auto indexKey = NebulaKeyUtils::vertexIndexKey(partId,
                                                                       index->get_index_id(),
                                                                       vId,
                                                                       values.value());
                        batchHolder->put(std::move(indexKey), "");
                    }
                }
            }
        }
        batchHolder->put(std::move(nKey), std::move(nVal));
    }
    return encodeBatchValue(batchHolder->getBatch());
}


cpp2::ErrorCode UpdateVertexProcessor::checkAndBuildContexts(
        const cpp2::UpdateVertexRequest& req) {
    if (this->expCtx_ == nullptr) {
        this->expCtx_ = std::make_unique<ExpressionContext>();
    }

    // return columns
    for (auto& col : req.get_return_columns()) {
        StatusOr<std::unique_ptr<Expression>> colExpRet = Expression::decode(col);
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
        StatusOr<std::unique_ptr<Expression>> expRet = Expression::decode(filterStr);
        if (!expRet.ok()) {
            LOG(ERROR) << "Can't decode the filter " << filterStr;
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        this->exp_ = std::move(expRet).value();
        this->exp_->setContext(this->expCtx_.get());
        auto status = this->exp_->prepare();
        if (!status.ok() || !this->checkExp(this->exp_.get())) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
    }
    auto vId = req.get_vertex_id();
    // build context of the update items
    for (auto& item : req.get_update_items()) {
        const auto &name = item.get_name();
        const auto &propName = item.get_prop();
        SourcePropertyExpression sourcePropExp(new std::string(name),
                                               new std::string(propName));
        sourcePropExp.setContext(this->expCtx_.get());
        auto status = sourcePropExp.prepare();
        if (!status.ok() || !this->checkExp(&sourcePropExp)) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto tagRet = this->schemaMan_->toTagID(this->spaceId_, name);
        if (!tagRet.ok()) {
            LOG(ERROR) << "Can't find tag " << name << ", in space " << this->spaceId_;
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto tagId = tagRet.value();
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            VLOG(3) << "Evict cache for vId " << vId << ", tagId " << tagId;
            vertexCache_->evict(std::make_pair(req.get_vertex_id(), tagId));
        }
        updateTagIds_.emplace(tagId);
        auto exp = Expression::decode(item.get_value());
        if (!exp.ok()) {
            LOG(ERROR) << "Can't decode the item's value " << item.get_value();
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto vexp = std::move(exp).value();

        // Get dependent prop
        valueProps_.clear();
        vexp->setContext(this->expCtx_.get());
        status = vexp->prepare();
        if (!status.ok() || !this->checkExp(vexp.get(), true)) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        if (insertable_) {
            depPropMap_.emplace_back(std::make_pair(std::make_pair(name, propName), valueProps_));
        }
    }

    if (this->expCtx_->hasDstTagProp() || this->expCtx_->hasEdgeProp()
        || this->expCtx_->hasVariableProp() || this->expCtx_->hasInputProp()) {
        LOG(ERROR) << "should only contain SrcTagProp expression!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }
    if (tagContexts_.size() != 1) {
        LOG(ERROR) << "should only contain one tag!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}


void UpdateVertexProcessor::process(const cpp2::UpdateVertexRequest& req) {
    VLOG(3) << "Receive UpdateVertexRequest...";
    this->spaceId_ = req.get_space_id();
    insertable_ = req.get_insertable();
    auto partId = req.get_part_id();
    auto retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Failure build contexts!";
        this->pushResultCode(retCode, partId);
        this->onFinished();
        return;
    }
    auto vId = req.get_vertex_id();
    updateItems_ = req.get_update_items();
    auto iRet = indexMan_->getTagIndexes(spaceId_);
    if (iRet.ok()) {
        indexes_ = std::move(iRet).value();
    }

    VLOG(3) << "Update vertex, spaceId: " << this->spaceId_
            << ", partId: " << partId << ", vId: " << vId;
    CHECK_NOTNULL(kvstore_);
    this->kvstore_->asyncAtomicOp(this->spaceId_, partId,
        [partId, vId, this] () -> folly::Optional<std::string> {
            // TODO(shylock) the AtomicOP can't return various error
            // so put it in the processor
            filterResult_ = checkFilter(partId, vId);
            if (filterResult_ == cpp2::ErrorCode::SUCCEEDED) {
                return updateAndWriteBack(partId, vId);
            } else {
                return folly::none;
            }
        },
        [this, partId, vId, req] (kvstore::ResultCode code) {
            while (true) {
                if (code == kvstore::ResultCode::SUCCEEDED) {
                    onProcessFinished(req.get_return_columns().size());
                    break;
                }
                LOG(ERROR) << "Fail to update vertex, spaceId: " << this->spaceId_
                           << ", partId: " << partId << ", vId: " << vId;
                if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                    handleLeaderChanged(this->spaceId_, partId);
                    break;
                }
                if (code == kvstore::ResultCode::ERR_ATOMIC_OP_FAILED) {
                    if (filterResult_ == cpp2::ErrorCode::E_FILTER_OUT) {
                        // Filter out
                        // https://github.com/vesoft-inc/nebula/issues/1888
                        // Only filter out so we still return the data
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
