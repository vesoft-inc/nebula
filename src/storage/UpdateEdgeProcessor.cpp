/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/UpdateEdgeProcessor.h"
#include "base/NebulaKeyUtils.h"
#include "dataman/RowWriter.h"
#include "kvstore/LogEncoder.h"

namespace nebula {
namespace storage {

void UpdateEdgeProcessor::onProcessFinished(int32_t retNum) {
    if (retNum > 0) {
        nebula::cpp2::Schema respScheam;
        respScheam.columns.resize(retNum);
        for (auto& prop : this->edgeContext_.props_) {
            if (prop.returned_) {
                auto column = this->columnDef(std::move(prop.prop_.name), prop.type_.type);
                respScheam.columns[prop.retIndex_] = std::move(column);
            }
        }
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


kvstore::ResultCode UpdateEdgeProcessor::collectEdgesProps(
                            PartitionID partId,
                            const cpp2::EdgeKey& edgeKey,
                            const std::vector<PropContext>& props,
                            FilterContext* fcontext,
                            Collector* collector) {
    auto prefix = NebulaKeyUtils::prefix(partId, edgeKey.src, edgeKey.edge_type,
                                         edgeKey.ranking, edgeKey.dst);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        VLOG(3) << "Error! ret = " << static_cast<int32_t>(ret) << ", spaceId " << spaceId_;
        return ret;
    }
    // Only use the latest version.
    if (iter && iter->valid()) {
        key_ = iter->key().toString();
        edgeReader_ = RowReader::getEdgePropReader(schemaMan_,
                                                   iter->val(),
                                                   spaceId_,
                                                   edgeKey.edge_type);
        filterReader_ = RowReader::getEdgePropReader(schemaMan_,
                                                     iter->val(),
                                                     spaceId_,
                                                     edgeKey.edge_type);
    } else if (insertable_) {
        insertEdge_ = true;
        auto now = std::numeric_limits<int64_t>::max() - time::WallClock::fastNowInMicroSec();
        key_ = NebulaKeyUtils::edgeKey(partId, edgeKey.src, edgeKey.edge_type,
                                       edgeKey.ranking, edgeKey.dst, now);
        auto schema = this->schemaMan_->getEdgeSchema(spaceId_, edgeKey.edge_type);
        RowWriter writer(schema);
        edgeReader_ = RowReader::getRowReader(writer.encode(), schema);
        filterReader_ = RowReader::getRowReader(writer.encode(), schema);
    } else {
        VLOG(3) << "Missed partId " << partId << ", edge " << edgeKey.src << " --> " << edgeKey.dst;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    this->collectProps(edgeReader_.get(), key_, props, fcontext, collector);
    return ret;
}


kvstore::ResultCode UpdateEdgeProcessor::processEdge(PartitionID partId,
                                                     const cpp2::EdgeKey& edgeKey) {
    record_.clear();
    insertEdge_ = false;
    rowRespData_.set_upsert(false);
    rowRespData_.set_edge_key(edgeKey);
    FilterContext fcontext;
    RowWriter writer(nullptr);
    PropsCollector collector(&writer);
    auto ret = collectEdgesProps(partId,
                                 edgeKey,
                                 this->edgeContext_.props_,
                                 &fcontext,
                                 &collector);
    if (ret != kvstore::ResultCode::SUCCEEDED) {
        return ret;
    }
    for (auto& tc : this->tagContexts_) {
        VLOG(3) << "partId " << partId << ", vId " << edgeKey.src
                << ", tagId " << tc.tagId_ << ", prop size " << tc.props_.size();
        ret = this->collectVertexProps(partId,
                                       edgeKey.src,
                                       tc.tagId_,
                                       tc.props_,
                                       &fcontext,
                                       &collector);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }
    }

    // the Where clause for filting
    auto& getters = expCtx_->getters();
    getters.getSrcTagProp = [&, this] (const std::string& tagName,
                                       const std::string& prop) -> OptVariantType {
        auto it = fcontext.tagFilters_.find(std::make_pair(tagName, prop));
        if (it == fcontext.tagFilters_.end()) {
            return Status::Error("Invalid Tag Filter");
        }
        VLOG(1) << "Hit srcProp for tag " << tagName << ", prop "
                << prop << ", value " << it->second;
        return it->second;
    };
    getters.getAliasProp = [&, this] (const std::string&,
                                      const std::string &prop) -> OptVariantType {
        auto res = RowReader::getPropByName(this->filterReader_.get(), prop);
        if (!ok(res)) {
            return Status::Error("Invalid Prop");
        }
        auto val = value(std::move(res));
        VLOG(1) << "Hit edgeProp for edge prop " << prop << ", value " << val;
        return val;
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
    if (returnColumnsNum_ > 0) {
        std::string encoded = writer.encode();
        auto writeSchema = std::make_shared<ResultSchemaProvider>(writer.moveSchema());
        CHECK_EQ(returnColumnsNum_, writeSchema->getNumFields());
        auto reader = RowReader::getRowReader(encoded, writeSchema);
        int64_t index = 0;
        for (auto& prop : this->edgeContext_.props_) {
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
        for (auto& tc : tagContexts_) {
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
    SchemaVer ver = edgeReader_->schemaVer();
    const auto constSchema = this->schemaMan_->getEdgeSchema(
            spaceId_, this->edgeContext_.edgeType_, ver);
    auto schema = std::const_pointer_cast<meta::SchemaProviderIf>(constSchema);
    // TODO(zhangguoqing) When the edge is to be inserted, it can't instantiate
    // the class RowUpdater() with edgeReader_ parameter, otherwise the updater_
    // doing encoded will report the error: Failed to encode updated data.
    if (insertEdge_) {
        updater_ = std::unique_ptr<RowUpdater>(new RowUpdater(schema));
        rowRespData_.set_upsert(true);
    } else {
        updater_ = std::unique_ptr<RowUpdater>(new RowUpdater(std::move(edgeReader_), schema));
    }
    for (auto& item : updateItems_) {
        auto edgeName = item.get_name();
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
        int32_t retIndex = -1;
        if (returnColumnsNum_ > 0) {
            for (auto& prop : this->edgeContext_.props_) {
                if (prop.prop_.name == field && prop.returned_) {
                    retIndex = prop.retIndex_;
                    break;
                }
            }
        }
        switch (expValue.which()) {
            case VAR_INT64: {
                auto v = boost::get<int64_t>(expValue);
                updater_->setInt(field, v);
                if (retIndex != -1) record_[retIndex] = v;
                break;
            }
            case VAR_DOUBLE: {
                auto v = boost::get<double>(expValue);
                updater_->setDouble(field, v);
                if (retIndex != -1) record_[retIndex] = v;
                break;
            }
            case VAR_BOOL: {
                auto v = boost::get<bool>(expValue);
                updater_->setBool(field, v);
                if (retIndex != -1) record_[retIndex] = v;
                break;
            }
            case VAR_STR: {
                auto v = boost::get<std::string>(expValue);
                updater_->setString(field, v);
                if (retIndex != -1) record_[retIndex] = v;
                break;
             }
            default:
                LOG(FATAL) << "Unknown VariantType: " << expValue.which();
        }
    }
    return kvstore::ResultCode::SUCCEEDED;
}


std::vector<kvstore::KV> UpdateEdgeProcessor::processData() {
   std::vector<kvstore::KV> data;
   data.emplace_back(key_, std::move(updater_->encode()));

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


cpp2::ErrorCode UpdateEdgeProcessor::checkAndBuildContexts(
        const cpp2::UpdateEdgeRequest& req) {
    auto retCode = QueryBaseProcessor<cpp2::UpdateEdgeRequest,
         cpp2::UpdateResponse>::checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        return retCode;
    }

    // build context of the update items
    if (this->expCtx_ == nullptr) {
        this->expCtx_ = std::make_unique<ExpressionContext>();
    }
    for (auto& item : req.get_update_items()) {
        std::string edgeName = item.get_name();
        std::string edgeProp = item.get_field();
        auto* edgePropExp = new AliasPropertyExpression(new std::string(""), &edgeName, &edgeProp);
        edgePropExp->setContext(this->expCtx_.get());
        auto status = edgePropExp->prepare();
        if (!status.ok() || !this->checkExp(edgePropExp)) {
            return cpp2::ErrorCode::E_INVALID_UPDATER;
        }

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

    if (expCtx_->hasDstTagProp()) {
        VLOG(3) << "ERROR: update edge[at storage level] should not contain "
                << "any DestPropertyExpression expression!";
        return cpp2::ErrorCode::E_INVALID_UPDATER;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}


void UpdateEdgeProcessor::process(const cpp2::UpdateEdgeRequest& req) {
    VLOG(3) << "Receive UpdateEdgeRequest...";
    spaceId_ = req.get_space_id();
    returnColumnsNum_ = req.get_return_columns().size();
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
    RowSetWriter rsWriter;
    std::for_each(req.get_parts().begin(), req.get_parts().end(), [&](auto& partE) {
        auto partId = partE.first;
        kvstore::ResultCode ret = kvstore::ResultCode::SUCCEEDED;
        for (auto& edgeKey : partE.second) {
            VLOG(3) << "Process spaceId: " << this->spaceId_
                    << ", partId:  " << partId
                    << ", src: " << edgeKey.get_src()
                    << ", edge_type: " << edgeKey.get_edge_type()
                    << ", dst: " << edgeKey.get_dst()
                    << ", ranking: " << edgeKey.get_ranking();
            kvstore::ResultCode updateRet = kvstore::ResultCode::SUCCEEDED;
            folly::Baton<true, std::atomic> baton;
            this->kvstore_->asyncAtomicOp(spaceId_, partId,
                    [&, this] () -> std::string {
                        ret = this->processEdge(partId, edgeKey);
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
                LOG(ERROR) << "Failure processEdge, spaceId: " << this->spaceId_
                           << ", partId: " << partId
                           << ", src: " << edgeKey.get_src()
                           << ", edge_type: " << edgeKey.get_edge_type()
                           << ", dst: " << edgeKey.get_dst()
                           << ", ranking: " << edgeKey.get_ranking();
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
