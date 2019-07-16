/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "base/Base.h"
#include "storage/UpdateVertexProcessor.h"
#include "base/NebulaKeyUtils.h"
#include "dataman/RowWriter.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

void UpdateVertexProcessor::onProcessFinished(int32_t retNum) {
    if (retNum > 0) {
        nebula::cpp2::Schema respScheam;
        respScheam.columns.resize(retNum);
        for (auto& tc : this->tagContexts_) {
            for (auto& prop : tc.props_) {
                if (prop.returned_) {
                    auto column = this->columnDef(std::move(prop.prop_.name), prop.type_.type);
                    respScheam.columns[prop.retIndex_] = std::move(column);
                }
            }
        }
        resp_.set_schema(std::move(respScheam));
        resp_.set_return_data(std::move(respData_));
    }
}


kvstore::ResultCode UpdateVertexProcessor::collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            const std::vector<PropContext>& props,
                            FilterContext* fcontext,
                            Collector* collector) {
    auto prefix = NebulaKeyUtils::prefix(partId, vId, tagId);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    // Will decode the properties according to the schema version
    // stored along with the properties
    tagReader_[tagId] = std::make_unique<KeyReaderPair>();
    if (iter && iter->valid()) {
        tagReader_[tagId]->key = iter->key().toString();
        tagReader_[tagId]->reader = RowReader::getTagPropReader(this->schemaMan_,
                                                                iter->val(),
                                                                spaceId_,
                                                                tagId);
    } else if (insertable_ && updateTagIDs_.find(tagId) != updateTagIDs_.end()) {
        tagReader_[tagId]->insert = true;
        auto now = std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
        tagReader_[tagId]->key = NebulaKeyUtils::vertexKey(partId, vId, tagId, now);
        auto schema = this->schemaMan_->getTagSchema(spaceId_, tagId);
        RowWriter writer(schema);
        tagReader_[tagId]->reader = RowReader::getRowReader(writer.encode(), schema);
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    this->collectProps(tagReader_[tagId]->reader.get(),
                       tagReader_[tagId]->key,
                       props,
                       fcontext,
                       collector);
    return ret;
}


kvstore::ResultCode UpdateVertexProcessor::processVertex(PartitionID partId,
                                                         VertexID vId) {
    record_.clear();
    tagReader_.clear();
    pendingUpdater_.clear();
    rowRespData_.set_upsert(false);
    rowRespData_.set_vertex_id(vId);
    FilterContext fcontext;
    RowWriter writer(nullptr);
    PropsCollector collector(&writer);
    for (auto& tc : this->tagContexts_) {
        VLOG(3) << "partId " << partId << ", vId " << vId
                << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
        auto ret = collectVertexProps(partId,
                                      vId,
                                      tc.tagId_,
                                      tc.props_,
                                      &fcontext,
                                      &collector);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
    }

    // the filter of When/Where clause
    auto& getters = expCtx_->getters();
    getters.getSrcTagProp = [&, this] (const std::string& tagName,
                                       const std::string& prop) -> OptVariantType {
        auto it = fcontext.tagFilters_.find(std::make_pair(tagName, prop));
        if (it == fcontext.tagFilters_.end()) {
            return Status::Error("Invalid Tag Filter");
        }
        VLOG(1) << "Hit srcProp filter for tag " << tagName << ", prop "
                << prop << ", value " << it->second;
        return it->second;
    };
    if (this->exp_ != nullptr) {
        auto filterResult = this->exp_->eval();
        if (filterResult.ok() && !Expression::asBool(filterResult.value())) {
            VLOG(1) << "Filter skips the update";
            return kvstore::ResultCode::ERR_FALSE_FILTER;
        }
    }

    // prepare the return(yield clause) record
    record_.resize(returnColumnsNum_);
    if (returnColumnsNum_ > 0 && !this->tagContexts_.empty()) {
        std::string encoded = writer.encode();
        auto writeSchema = std::make_shared<ResultSchemaProvider>(writer.moveSchema());
        CHECK_EQ(returnColumnsNum_, writeSchema->getNumFields());
        auto reader = RowReader::getRowReader(encoded, writeSchema);
        int64_t index = 0;
        for (auto& tc : this->tagContexts_) {
            for (auto& prop : tc.props_) {
                if (prop.returned_) {
                    auto res = RowReader::getPropByIndex(reader.get(), index);
                    if (!ok(res)) {
                        VLOG(3) << "Bad value for prop " << prop.prop_.get_name();
                        return kvstore::ResultCode::ERR_FALSE_UPDATER;
                    }
                    record_[prop.retIndex_] = value(std::move(res));
                    index++;
                }
            }
        }
    }

    // the Set clause for updating
    for (auto& item : updateItems_) {
        auto tagName = item.get_name();
        auto tagId = this->schemaMan_->toTagID(spaceId_, tagName).value();
        auto field = item.get_field();
        StatusOr<std::unique_ptr<Expression>> exp = Expression::decode(item.get_value());
        auto vexp = std::move(exp).value();
        vexp->setContext(expCtx_.get());
        auto value = vexp->eval();
        if (!value.ok()) {
            return kvstore::ResultCode::ERR_FALSE_UPDATER;
        }
        auto expValue = value.value();
        // update the props and the record
        auto itr = tagReader_.find(tagId);
        if (itr == tagReader_.end()) {
            return kvstore::ResultCode::ERR_UNKNOWN;
        } else {
            auto itu = pendingUpdater_.find(tagId);
            if (itu == pendingUpdater_.end()) {
                pendingUpdater_[tagId] = std::make_unique<KeyUpdaterPair>();
                pendingUpdater_[tagId]->key = itr->second->key;
                SchemaVer ver = itr->second->reader->schemaVer();
                const auto constSchema = this->schemaMan_->getTagSchema(spaceId_, tagId, ver);
                auto schema = std::const_pointer_cast<meta::SchemaProviderIf>(constSchema);
                // TODO(zhangguoqing) When the tag is to be inserted, it can't instantiate
                // the class RowUpdater() with itr->second->reader parameter, otherwise the
                // updater doing encoded will report the error: Failed to encode updated data.
                if (itr->second->insert) {
                    pendingUpdater_[tagId]->updater = std::unique_ptr<RowUpdater>(
                            new RowUpdater(schema));
                    rowRespData_.set_upsert(true);
                } else {
                    pendingUpdater_[tagId]->updater = std::unique_ptr<RowUpdater>(
                            new RowUpdater(std::move(itr->second->reader), schema));
                }
            }
            int32_t retIndex = -1;
            if (returnColumnsNum_ > 0) {
                for (auto& tc : this->tagContexts_) {
                    if (tc.tagId_ == tagId) {
                        auto* prop = tc.findProp(field);
                        if (prop != nullptr && prop->returned_) {
                            retIndex = prop->retIndex_;
                        }
                        break;
                    }
                }
            }
            switch (expValue.which()) {
                case VAR_INT64: {
                    auto v = boost::get<int64_t>(expValue);
                    pendingUpdater_[tagId]->updater->setInt(field, v);
                    if (retIndex != -1) record_[retIndex] = v;
                    break;
                }
                case VAR_DOUBLE: {
                    auto v = boost::get<double>(expValue);
                    pendingUpdater_[tagId]->updater->setDouble(field, v);
                    if (retIndex != -1) record_[retIndex] = v;
                    break;
                }
                case VAR_BOOL: {
                    auto v = boost::get<bool>(expValue);
                    pendingUpdater_[tagId]->updater->setBool(field, v);
                    if (retIndex != -1) record_[retIndex] = v;
                    break;
                }
                case VAR_STR: {
                    auto v = boost::get<std::string>(expValue);
                    pendingUpdater_[tagId]->updater->setString(field, v);
                    if (retIndex != -1) record_[retIndex] = v;
                    break;
                 }
                default:
                    LOG(FATAL) << "Unknown VariantType: " << expValue.which();
            }
        }
    }

    return kvstore::ResultCode::SUCCEEDED;
}

std::vector<kvstore::KV> UpdateVertexProcessor::processData() {
   std::vector<kvstore::KV> data;
   for (const auto& u : pendingUpdater_) {
       data.emplace_back(std::move(u.second->key),
                         std::move(u.second->updater->encode()));
   }

   if (!data.empty() && returnColumnsNum_ > 0) {
       RowWriter writer(nullptr);
       for (auto v : record_) {
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
           }
       }
       rowRespData_.set_data(writer.encode());
       respData_.emplace_back(rowRespData_);
   }
   return data;
}

cpp2::ErrorCode UpdateVertexProcessor::checkAndBuildContexts(
        const cpp2::UpdateVertexRequest& req) {
    auto retCode = QueryBaseProcessor<cpp2::UpdateVertexRequest,
         cpp2::UpdateResponse>::checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // build context of the update items
    if (this->expCtx_ == nullptr) {
        this->expCtx_ = std::make_unique<ExpressionContext>();
    }
    for (auto& item : req.get_update_items()) {
        std::string name = item.get_name();
        std::string field = item.get_field();
        auto* sourcePropExp = new SourcePropertyExpression(&name, &field);
        sourcePropExp->setContext(this->expCtx_.get());
        auto status = sourcePropExp->prepare();
        if (!status.ok() || !this->checkExp(sourcePropExp)) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto tagId = this->schemaMan_->toTagID(spaceId_, name).value();
        updateTagIDs_.emplace(tagId);

        StatusOr<std::unique_ptr<Expression>> exp = Expression::decode(item.get_value());
        if (!exp.ok()) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
        auto vexp = std::move(exp).value();
        vexp->setContext(this->expCtx_.get());
        status = vexp->prepare();
        if (!status.ok() || !this->checkExp(vexp.get())) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }
    }

    if (expCtx_->hasEdgeProp() || !edgeContext_.props_.empty()) {
        VLOG(3) << "ERROR: update vertex query should not contain any edge expression!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}


void UpdateVertexProcessor::process(const cpp2::UpdateVertexRequest& req) {
    VLOG(3) << "Receive UpdateVertexRequest...";
    spaceId_ = req.get_space_id();
    returnColumnsNum_ = req.get_return_columns().size();
    this->tagContexts_.reserve(returnColumnsNum_);
    VLOG(3) << "Receive request, spaceId " << spaceId_ << ", return cols " << returnColumnsNum_;
    insertable_ = req.get_insertable();
    auto retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        LOG(ERROR) << "Failure build contexts!";
        for (auto& p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }
    updateItems_ = std::move(req.get_update_items());

    CHECK_NOTNULL(kvstore_);
    std::for_each(req.get_parts().begin(), req.get_parts().end(), [&](auto& partV) {
        auto partId = partV.first;
        kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
        for (auto& vId : partV.second) {
            VLOG(3) << "Process spaceId: " << this->spaceId_
                    << ", partId: " << partId << ", vId: " << vId;
            kvstore::ResultCode updateRet = kvstore::ResultCode::SUCCEEDED;
            folly::Baton<true, std::atomic> baton;
            this->kvstore_->asyncAtomicOp(spaceId_, partId,
                    [&, this] () -> std::string {
                        ret = this->processVertex(partId, vId);
                        if (ret == kvstore::ResultCode::SUCCEEDED) {
                            auto data = this->processData();
                            if (data.empty()) {
                                return std::string("");
                            }
                            auto log = kvstore::encodeMultiValues(kvstore::OP_MULTI_PUT, data);
                            return log;
                        }
                        return std::string("");
                    },
                    [&] (kvstore::ResultCode code) {
                        updateRet = code;
                        baton.post();
                    });
            baton.wait();
            if (ret == kvstore::ResultCode::SUCCEEDED) {
                ret = updateRet;
            }
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                LOG(ERROR) << "Failure processVertex, spaceId: " << this->spaceId_
                           << ", partId: " << partId << ", vId: " << vId;
                break;
            }
        }
        this->pushResultCode(this->to(ret), partId);
    });

    onProcessFinished(returnColumnsNum_);
    this->onFinished();
}

}  // namespace storage
}  // namespace nebula
