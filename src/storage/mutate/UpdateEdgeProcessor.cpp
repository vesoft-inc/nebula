/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/mutate/UpdateEdgeProcessor.h"
#include "base/NebulaKeyUtils.h"
#include "dataman/RowWriter.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

void UpdateEdgeProcessor::onProcessFinished(int32_t retNum) {
    if (retNum > 0) {
        nebula::cpp2::Schema respScheam;
        respScheam.columns.reserve(retNum);
        RowWriter writer(nullptr);
        for (auto& exp : returnColumnsExp_) {
            auto value = exp->eval();
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
        const auto constSchema = reader->getSchema();
        for (auto& prop : props) {
            auto res = RowReader::getPropByName(reader.get(), prop.prop_.name);
            if (!ok(res)) {
                VLOG(1) << "Skip the bad value for tag: "
                        << tagId << ", prop " << prop.prop_.name;
                return kvstore::ResultCode::ERR_UNKNOWN;
            }
            auto&& v = value(std::move(res));
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
        key_ = iter->key().toString();
        auto reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                   iter->val(),
                                                   this->spaceId_,
                                                   edgeKey.edge_type);
        const auto constSchema = reader->getSchema();
        for (auto index = 0UL; index < constSchema->getNumFields(); index++) {
            auto propName = std::string(constSchema->getFieldName(index));
            auto res = RowReader::getPropByName(reader.get(), propName);
            if (!ok(res)) {
                VLOG(1) << "Skip the bad edge value for prop " << propName;
                return kvstore::ResultCode::ERR_UNKNOWN;
            }
            auto&& v = value(std::move(res));
            edgeFilters_.emplace(propName, v);
        }
        auto schema = std::const_pointer_cast<meta::SchemaProviderIf>(constSchema);
        updater_ = std::unique_ptr<RowUpdater>(new RowUpdater(std::move(reader), schema));
    } else if (insertable_) {
        resp_.set_upsert(true);
        int64_t ms = time::WallClock::fastNowInMicroSec();
        auto now = std::numeric_limits<int64_t>::max() - ms;
        key_ = NebulaKeyUtils::edgeKey(partId, edgeKey.src, edgeKey.edge_type,
                                       edgeKey.ranking, edgeKey.dst, now);
        const auto constSchema = this->schemaMan_->getEdgeSchema(this->spaceId_,
                                                                 edgeKey.edge_type);
        if (constSchema == nullptr) {
            return kvstore::ResultCode::ERR_UNKNOWN;
        }
        for (auto index = 0UL; index < constSchema->getNumFields(); index++) {
            auto propName = std::string(constSchema->getFieldName(index));
            OptVariantType value = RowReader::getDefaultProp(constSchema.get(), propName);
            if (!value.ok()) {
                return kvstore::ResultCode::ERR_UNKNOWN;
            }
            auto v = std::move(value.value());
            edgeFilters_.emplace(propName, v);
        }
        auto schema = std::const_pointer_cast<meta::SchemaProviderIf>(constSchema);
        updater_ = std::unique_ptr<RowUpdater>(new RowUpdater(schema));
    } else {
        VLOG(3) << "Missed edge, spaceId: " << this->spaceId_ << ", partId: " << partId
                << ", src: " << edgeKey.src << ", type: " << edgeKey.edge_type
                << ", ranking: " << edgeKey.ranking << ", dst: "<< edgeKey.dst;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}


std::string UpdateEdgeProcessor::updateAndWriteBack() {
    for (auto& item : updateItems_) {
        auto prop = item.get_prop();
        auto exp = Expression::decode(item.get_value());
        if (!exp.ok()) {
            return std::string("");
        }
        auto vexp = std::move(exp).value();
        vexp->setContext(this->expCtx_.get());
        auto value = vexp->eval();
        if (!value.ok()) {
            return std::string("");
        }
        auto expValue = value.value();
        edgeFilters_[prop] = expValue;

        switch (expValue.which()) {
            case VAR_INT64: {
                auto v = boost::get<int64_t>(expValue);
                updater_->setInt(prop, v);
                break;
            }
            case VAR_DOUBLE: {
                auto v = boost::get<double>(expValue);
                updater_->setDouble(prop, v);
                break;
            }
            case VAR_BOOL: {
                auto v = boost::get<bool>(expValue);
                updater_->setBool(prop, v);
                break;
            }
            case VAR_STR: {
                auto v = boost::get<std::string>(expValue);
                updater_->setString(prop, v);
                break;
             }
            default: {
                LOG(FATAL) << "Unknown VariantType: " << expValue.which();
                return std::string("");
            }
        }
    }

    std::vector<kvstore::KV> data;
    data.emplace_back(key_, updater_->encode());
    auto log = kvstore::encodeMultiValues(kvstore::OP_MULTI_PUT, data);
    return log;
}


bool UpdateEdgeProcessor::checkFilter(const PartitionID partId,
                                      const cpp2::EdgeKey& edgeKey) {
    auto ret = collectEdgesProps(partId, edgeKey);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return false;
    }
    for (auto& tc : this->tagContexts_) {
        VLOG(3) << "partId " << partId << ", vId " << edgeKey.src
                << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
        ret = collectVertexProps(partId, edgeKey.src, tc.tagId_, tc.props_);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return false;
        }
    }

    auto& getters = this->expCtx_->getters();
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

    if (this->exp_ != nullptr) {
        auto filterResult = this->exp_->eval();
        if (!filterResult.ok() || !Expression::asBool(filterResult.value())) {
            VLOG(1) << "Filter skips the update";
            return false;
        }
    }
    return true;
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
    auto edgeKey = std::move(req).get_edge_key();
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
    updateItems_ = std::move(req).get_update_items();

    VLOG(3) << "Update edge, spaceId: " << this->spaceId_ << ", partId:  " << partId
            << ", src: " << edgeKey.get_src() << ", edge_type: " << edgeKey.get_edge_type()
            << ", dst: " << edgeKey.get_dst() << ", ranking: " << edgeKey.get_ranking();
    CHECK_NOTNULL(kvstore_);
    this->kvstore_->asyncAtomicOp(this->spaceId_, partId,
        [&, this] () -> std::string {
            if (checkFilter(partId, edgeKey)) {
                return updateAndWriteBack();
            }
            return std::string("");
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
                this->pushResultCode(to(code), partId);
                break;
            }
            this->onFinished();
        });
}

}  // namespace storage
}  // namespace nebula
