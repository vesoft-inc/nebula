/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#include "storage/query/QueryBaseProcessor.h"
#include "utils/NebulaKeyUtils.h"
#include <algorithm>
#include "dataman/RowReader.h"
#include "dataman/RowWriter.h"
#include "meta/NebulaSchemaProvider.h"
#include "filter/FunctionManager.h"
#include "time/WallClock.h"
#include "algorithm/ReservoirSampling.h"

DECLARE_int32(max_handlers_per_req);
DECLARE_int32(min_vertices_per_bucket);
DECLARE_int32(max_edge_returned_per_vertex);
DECLARE_bool(enable_vertex_cache);
DECLARE_bool(enable_reservoir_sampling);

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
bool QueryBaseProcessor<REQ, RESP>::validOperation(nebula::cpp2::SupportedType vType,
                                                   cpp2::StatType statType) {
    switch (statType) {
        case cpp2::StatType::SUM:
        case cpp2::StatType::AVG: {
            return vType == nebula::cpp2::SupportedType::INT
                    || vType == nebula::cpp2::SupportedType::VID
                    || vType == nebula::cpp2::SupportedType::TIMESTAMP
                    || vType == nebula::cpp2::SupportedType::FLOAT
                    || vType == nebula::cpp2::SupportedType::DOUBLE;
        }
        case cpp2::StatType::COUNT: {
             break;
        }
    }
    return true;
}

template <typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::addDefaultProps(std::vector<PropContext>& p, EdgeType eType) {
    p.emplace_back("_src", eType, 0, PropContext::PropInKeyType::SRC);
    p.emplace_back("_rank", eType, 1, PropContext::PropInKeyType::RANK);
    p.emplace_back("_dst", eType, 2, PropContext::PropInKeyType::DST);
    p.emplace_back("_type", eType, 3, PropContext::PropInKeyType::TYPE);
}

template <typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::initEdgeContext(const std::vector<EdgeType>& eTypes,
                                                    bool need_default_props) {
    std::transform(eTypes.cbegin(), eTypes.cend(),
                   std::inserter(edgeContexts_, edgeContexts_.end()),
                   [this, need_default_props](const auto& ec) {
                       std::vector<PropContext> prop;
                       if (need_default_props) {
                           addDefaultProps(prop, ec);
                       }
                       return std::make_pair(ec, std::move(prop));
                   });
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::checkAndBuildContexts(const REQ& req) {
    // Handle the case for query edges which should return some columns by default.
    int32_t index = std::accumulate(edgeContexts_.cbegin(), edgeContexts_.cend(), 0,
                                    [](int ac, auto& ec) { return ac + ec.second.size(); });
    std::unordered_map<TagID, int32_t> tagIndex;
    for (auto& col : req.get_return_columns()) {
        PropContext prop;
        switch (col.owner) {
            case cpp2::PropOwner::SOURCE:
            case cpp2::PropOwner::DEST: {
                auto tagId = col.id.get_tag_id();
                auto schema = this->schemaMan_->getTagSchema(spaceId_, tagId);
                if (!schema) {
                    VLOG(3) << "Can't find spaceId " << spaceId_ << ", tagId " << tagId;
                    return cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
                }
                const auto& ftype = schema->getFieldType(col.name);
                if (UNLIKELY(ftype == CommonConstants::kInvalidValueType())) {
                    return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                }
                prop.type_ = ftype;
                prop.retIndex_ = index++;
                if (col.__isset.stat && !validOperation(ftype.type, col.stat)) {
                    return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                }
                VLOG(3) << "tagId " << tagId << ", prop " << col.name;
                prop.prop_ = std::move(col);
                prop.returned_ = true;
                auto it = tagIndex.find(tagId);
                if (it == tagIndex.end()) {
                    TagContext tc;
                    tc.tagId_ = tagId;
                    tc.props_.emplace_back(std::move(prop));
                    tagContexts_.emplace_back(std::move(tc));
                    tagIndex.emplace(tagId, tagContexts_.size() - 1);
                } else {
                    tagContexts_[it->second].props_.emplace_back(std::move(prop));
                }
                break;
            }
            case cpp2::PropOwner::EDGE: {
                EdgeType edgeType = col.id.get_edge_type();
                auto edgeName = this->schemaMan_->toEdgeName(spaceId_, std::abs(edgeType));
                if (!edgeName.ok()) {
                    VLOG(3) << "Can't find spaceId " << spaceId_ << ", edgeType " << edgeType;
                    return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
                }
                this->edgeMap_.emplace(edgeName.value(), std::abs(edgeType));

                auto it = kPropsInKey_.find(col.name);
                if (it != kPropsInKey_.end()) {
                    prop.pikType_ = it->second;
                    if (prop.pikType_ == PropContext::PropInKeyType::SRC ||
                        prop.pikType_ == PropContext::PropInKeyType::DST) {
                        prop.type_.type = nebula::cpp2::SupportedType::VID;
                    } else {
                        prop.type_.type = nebula::cpp2::SupportedType::INT;
                    }
                } else {
                    auto schema = this->schemaMan_->getEdgeSchema(spaceId_,
                                                                  std::abs(edgeType));
                    if (!schema) {
                        return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                    }
                    const auto& ftype = schema->getFieldType(col.name);
                    if (UNLIKELY(ftype == CommonConstants::kInvalidValueType())) {
                        return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                    }
                    prop.type_ = ftype;
                }
                if (col.__isset.stat && !validOperation(prop.type_.type, col.stat)) {
                    return cpp2::ErrorCode::E_IMPROPER_DATA_TYPE;
                }
                prop.retIndex_ = index++;
                prop.prop_ = std::move(col);
                prop.returned_ = true;
                auto it2 = edgeContexts_.find(edgeType);
                if (it2 == edgeContexts_.end()) {
                    std::vector<PropContext> v{std::move(prop)};
                    edgeContexts_.emplace(edgeType, std::move(v));
                    break;
                }
                it2->second.emplace_back(std::move(prop));
                break;
            }
        }
    }
    const auto& filterStr = req.get_filter();
    if (!filterStr.empty()) {
        StatusOr<std::unique_ptr<Expression>> expRet = Expression::decode(filterStr);
        if (!expRet.ok()) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        exp_ = std::move(expRet).value();
        if (!checkExp(exp_.get())) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        expCtx_ = std::make_unique<ExpressionContext>();
        exp_->setContext(expCtx_.get());
    }

    buildTTLInfoAndRespSchema();
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
folly::Optional<std::pair<std::string, int64_t>>
QueryBaseProcessor<REQ, RESP>::getTagTTLInfo(TagID tagId) {
    folly::Optional<std::pair<std::string, int64_t>> ret;
    auto tagFound = tagTTLInfo_.find(tagId);
    if (tagFound != tagTTLInfo_.end()) {
        ret.emplace(tagFound->second.first, tagFound->second.second);
    }
    return ret;
}

template<typename REQ, typename RESP>
folly::Optional<std::pair<std::string, int64_t>>
QueryBaseProcessor<REQ, RESP>::getEdgeTTLInfo(EdgeType edgeType) {
    folly::Optional<std::pair<std::string, int64_t>> ret;
    auto edgeFound = edgeTTLInfo_.find(edgeType);
    if (edgeFound != edgeTTLInfo_.end()) {
       ret.emplace(edgeFound->second.first, edgeFound->second.second);
    }
    return ret;
}

template<typename REQ, typename RESP>
bool QueryBaseProcessor<REQ, RESP>::checkExp(const Expression* exp) {
    switch (exp->kind()) {
        case Expression::kPrimary:
            return true;
        case Expression::kFunctionCall: {
            auto* funcExp = static_cast<FunctionCallExpression*>(
                              const_cast<Expression*>(exp));
            auto* name = funcExp->name();
            auto args = funcExp->args();
            auto func = FunctionManager::get(*name, args.size());
            if (!func.ok()) {
                return false;
            }
            for (auto& arg : args) {
                if (!checkExp(arg)) {
                    return false;
                }
            }
            funcExp->setFunc(std::move(func).value());
            return true;
        }
        case Expression::kUnary: {
            auto* unaExp = static_cast<const UnaryExpression*>(exp);
            return checkExp(unaExp->operand());
        }
        case Expression::kTypeCasting: {
            auto* typExp = static_cast<const TypeCastingExpression*>(exp);
            return checkExp(typExp->operand());
        }
        case Expression::kArithmetic: {
            auto* ariExp = static_cast<const ArithmeticExpression*>(exp);
            return checkExp(ariExp->left()) && checkExp(ariExp->right());
        }
        case Expression::kRelational: {
            auto* relExp = static_cast<const RelationalExpression*>(exp);
            return checkExp(relExp->left()) && checkExp(relExp->right());
        }
        case Expression::kLogical: {
            auto* logExp = static_cast<const LogicalExpression*>(exp);
            return checkExp(logExp->left()) && checkExp(logExp->right());
        }
        case Expression::kSourceProp: {
            auto* sourceExp = static_cast<const SourcePropertyExpression*>(exp);
            const auto* tagName = sourceExp->alias();
            const auto* propName = sourceExp->prop();
            auto tagRet = this->schemaMan_->toTagID(spaceId_, *tagName);
            if (!tagRet.ok()) {
                VLOG(1) << "Can't find tag " << *tagName << ", in space " << spaceId_;
                return false;
            }
            auto tagId = tagRet.value();
            // TODO(heng): Now we use the latest version.
            auto schema = this->schemaMan_->getTagSchema(spaceId_, tagId);
            CHECK(!!schema);
            auto field = schema->field(*propName);
            if (field == nullptr) {
                VLOG(1) << "Can't find related prop " << *propName << " on tag " << tagName;
                return false;
            }
            // TODO(heng): Now we have to scan the whole array to find related tagId,
            // maybe we could find a better way to solve it.
            for (auto& tc : tagContexts_) {
                if (tc.tagId_ == tagId) {
                    auto* prop = tc.findProp(*propName);
                    if (prop == nullptr) {
                        tc.pushFilterProp(*tagName, *propName, field->getType());
                    } else if (!prop->filtered()) {
                        prop->setTagOrEdgeName(*tagName);
                    }
                    return true;
                }
            }
            VLOG(1) << "There is no related tag existed in tagContexts!";
            TagContext tc;
            tc.tagId_ = tagId;
            tc.pushFilterProp(*tagName, *propName, field->getType());
            tagContexts_.emplace_back(std::move(tc));
            return true;
        }
        case Expression::kEdgeRank:
        case Expression::kEdgeDstId:
        case Expression::kEdgeSrcId:
        case Expression::kEdgeType: {
            return true;
        }
        case Expression::kAliasProp: {
            if (edgeContexts_.empty()) {
                VLOG(1) << "No edge requested!";
                return false;
            }

            auto* edgeExp = static_cast<const AliasPropertyExpression*>(exp);

            // TODO(simon.liu) we need handle rename.
            auto edgeStatus = this->schemaMan_->toEdgeType(spaceId_, *edgeExp->alias());
            if (!edgeStatus.ok()) {
                VLOG(1) << "Can't find edge " << *(edgeExp->alias());
                return false;
            }

            auto edgeType = edgeStatus.value();
            auto schema = this->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
            if (!schema) {
                VLOG(1) << "Cant find edgeType " << edgeType;
                return false;
            }

            const auto* propName = edgeExp->prop();
            auto field = schema->field(*propName);
            if (field == nullptr) {
                VLOG(1) << "Can't find related prop " << *propName << " on edge "
                        << *(edgeExp->alias());
                return false;
            }
            return true;
        }
        case Expression::kVariableProp:
        case Expression::kDestProp:
        case Expression::kInputProp: {
            return false;
        }
        default: {
            VLOG(1) << "Unsupport expression type! kind = "
                    << std::to_string(static_cast<uint8_t>(exp->kind()));
            return false;
        }
    }
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::collectProps(RowReader* reader,
                                                 folly::StringPiece key,
                                                 const std::vector<PropContext>& props,
                                                 FilterContext* fcontext,
                                                 Collector* collector) {
    for (auto& prop : props) {
        VLOG(1) << "Collect prop " << prop.prop_.name
                << ", type " << static_cast<int32_t>(prop.type_.type);
        if (!key.empty()) {
            switch (prop.pikType_) {
                case PropContext::PropInKeyType::NONE:
                    break;
                case PropContext::PropInKeyType::SRC:
                    collector->collectVid(NebulaKeyUtils::getSrcId(key), prop);
                    continue;
                case PropContext::PropInKeyType::DST:
                    VLOG(3) << "collect _dst, value = " << NebulaKeyUtils::getDstId(key);
                    if (compactDstIdProps_) {
                        collector->collectVid(NebulaKeyUtils::getDstId(key), prop);
                    } else {
                        collector->collectDstId(NebulaKeyUtils::getDstId(key));
                    }
                    continue;
                case PropContext::PropInKeyType::TYPE:
                    collector->collectInt64(static_cast<int64_t>(NebulaKeyUtils::getEdgeType(key)),
                                            prop);
                    continue;
                case PropContext::PropInKeyType::RANK:
                    collector->collectInt64(NebulaKeyUtils::getRank(key), prop);
                    continue;
            }
        }
        if (reader != nullptr) {
            const auto& name = prop.prop_.get_name();
            VariantType v;
            auto res = RowReader::getPropByName(reader, name);
            if (!ok(res)) {
                VLOG(1) << "Bad value for prop: " << name;
                // TODO: Should return NULL
                auto defaultVal = RowReader::getDefaultProp(prop.type_.type);
                if (!defaultVal.ok()) {
                    // Should never reach here.
                    LOG(ERROR) << "Get default value failed for " << name;
                    continue;
                } else {
                    v = std::move(defaultVal).value();
                }
            } else {
                v = value(std::move(res));
            }

            if (prop.fromTagFilter()) {
                fcontext->tagFilters_.emplace(std::make_pair(prop.tagOrEdgeName(), name), v);
            }
            if (prop.returned_) {
                switch (v.which()) {
                    case VAR_INT64:
                        collector->collectInt64(boost::get<int64_t>(v), prop);
                        break;
                    case VAR_DOUBLE:
                        collector->collectDouble(boost::get<double>(v), prop);
                        break;
                    case VAR_BOOL:
                        collector->collectBool(boost::get<bool>(v), prop);
                        break;
                    case VAR_STR:
                        collector->collectString(boost::get<std::string>(v), prop);
                        break;
                    default:
                        LOG(FATAL) << "Unknown VariantType: " << v.which();
                }  // switch
            }  // if returned
        }  // if reader != nullptr
    }  // for
}

template<typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::collectVertexProps(
                            PartitionID partId,
                            VertexID vId,
                            TagID tagId,
                            const std::vector<PropContext>& props,
                            FilterContext* fcontext,
                            Collector* collector) {
    auto schema = this->schemaMan_->getTagSchema(spaceId_, tagId);
    if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
        auto result = vertexCache_->get(std::make_pair(vId, tagId));
        if (result.ok()) {
            auto v = std::move(result).value();
            auto reader = RowReader::getTagPropReader(this->schemaMan_, v, spaceId_, tagId);
            if (reader == nullptr) {
                return kvstore::ResultCode::ERR_CORRUPT_DATA;
            }

            // Check if ttl data expired
            auto retTtlOpt = getTagTTLInfo(tagId);
            if (retTtlOpt.hasValue()) {
                auto ttlValue = retTtlOpt.value();
                if (checkDataExpiredForTTL(schema.get(),
                                           reader.get(),
                                           ttlValue.first,
                                           ttlValue.second)) {
                    VLOG(3) << "Hit cache for vId " << vId << ", tagId "
                            << tagId <<", but data expired";
                    return kvstore::ResultCode::SUCCEEDED;
                }
            }

            this->collectProps(reader.get(), "", props, fcontext, collector);
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
    // Will decode the properties according to the schema version
    // stored along with the properties
    if (iter && iter->valid()) {
        auto reader = RowReader::getTagPropReader(this->schemaMan_, iter->val(), spaceId_, tagId);
        if (reader == nullptr) {
            return kvstore::ResultCode::ERR_CORRUPT_DATA;
        }
        // Check if ttl data expired
        auto retTtlOpt = getTagTTLInfo(tagId);
        if (retTtlOpt.hasValue()) {
            auto ttlValue = retTtlOpt.value();
            if (checkDataExpiredForTTL(schema.get(),
                                       reader.get(),
                                       ttlValue.first,
                                       ttlValue.second)) {
                return ret;
            }
        }
        this->collectProps(reader.get(), iter->key(), props, fcontext, collector);
        if (FLAGS_enable_vertex_cache && vertexCache_ != nullptr) {
            vertexCache_->insert(std::make_pair(vId, tagId),
                                 iter->val().str());
            VLOG(3) << "Insert cache for vId " << vId << ", tagId " << tagId;
        }
    } else {
        VLOG(3) << "Missed partId " << partId << ", vId " << vId << ", tagId " << tagId;
        return kvstore::ResultCode::ERR_KEY_NOT_FOUND;
    }
    return ret;
}

template<typename REQ, typename RESP>
kvstore::ResultCode QueryBaseProcessor<REQ, RESP>::collectEdgeProps(
                                               PartitionID partId,
                                               VertexID vId,
                                               EdgeType edgeType,
                                               FilterContext* fcontext,
                                               EdgeProcessor proc) {
    auto prefix = NebulaKeyUtils::edgePrefix(partId, vId, edgeType);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto ret = this->kvstore_->prefix(spaceId_, partId, prefix, &iter);
    if (ret != kvstore::ResultCode::SUCCEEDED || !iter) {
        return ret;
    }

    EdgeRanking lastRank  = -1;
    VertexID    lastDstId = 0;
    bool        firstLoop = true;
    int         cnt = 0;
    bool onlyStructure = onlyStructures_[edgeType];
    Getters getters;

    auto schema = this->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
    auto retTTL = getEdgeTTLInfo(edgeType);
    for (; iter->valid(); iter->next()) {
        if (!FLAGS_enable_reservoir_sampling
                && !(cnt < FLAGS_max_edge_returned_per_vertex)) {
            break;
        }
        auto key = iter->key();
        auto val = iter->val();
        auto rank = NebulaKeyUtils::getRank(key);
        auto dstId = NebulaKeyUtils::getDstId(key);
        if (!firstLoop && rank == lastRank && lastDstId == dstId) {
            VLOG(3) << "Only get the latest version for each edge.";
            continue;
        }
        if (firstLoop) {
            firstLoop = false;
        }
        lastRank = rank;
        lastDstId = dstId;
        RowReader reader = RowReader::getEmptyRowReader();
        if ((!onlyStructure || retTTL.has_value()) && !val.empty()) {
            reader = RowReader::getEdgePropReader(this->schemaMan_,
                                                  val,
                                                  spaceId_,
                                                  std::abs(edgeType));
            if (reader == nullptr) {
                LOG(WARNING) << "Skip the bad format row!";
                continue;
            }
            // Check if ttl data expired
            if (retTTL.has_value() && checkDataExpiredForTTL(schema.get(),
                                                             reader.get(),
                                                             retTTL.value().first,
                                                             retTTL.value().second)) {
                    VLOG(3) << "Data expired.";
                    continue;
            }

            if (exp_ != nullptr) {
                getters.getAliasProp = [this, edgeType, &reader, &key](const std::string& edgeName,
                                           const std::string& prop) -> OptVariantType {
                    auto edgeFound = this->edgeMap_.find(edgeName);
                    if (edgeFound == edgeMap_.end()) {
                        return Status::Error(
                                "Edge `%s' not found when call getters.", edgeName.c_str());
                    }
                    if (std::abs(edgeType) != edgeFound->second) {
                        return Status::Error("Ignore this edge : %s", edgeFound->first.c_str());
                    }

                    if (prop == _SRC) {
                        return NebulaKeyUtils::getSrcId(key);
                    } else if (prop == _DST) {
                        return NebulaKeyUtils::getDstId(key);
                    } else if (prop == _RANK) {
                        return NebulaKeyUtils::getRank(key);
                    } else if (prop == _TYPE) {
                        return static_cast<int64_t>(NebulaKeyUtils::getEdgeType(key));
                    }

                    auto res = RowReader::getPropByName(reader.get(), prop);
                    if (!ok(res)) {
                        return Status::Error("Invalid Prop");
                    }
                    return value(std::move(res));
                };
                getters.getEdgeRank = [&rank] () -> VariantType {
                    return rank;
                };
                getters.getEdgeDstId = [this,
                                        &edgeType,
                                        &dstId] (const std::string& edgeName) -> OptVariantType {
                    auto edgeFound = this->edgeMap_.find(edgeName);
                    if (edgeFound == edgeMap_.end()) {
                        return Status::Error(
                                "Edge `%s' not found when call getters.", edgeName.c_str());
                    }
                    if (std::abs(edgeType) != edgeFound->second) {
                        return Status::Error("Ignore this edge : %s", edgeFound->first.c_str());
                    }
                    return dstId;
                };
                getters.getSrcTagProp = [&fcontext] (const std::string& tag,
                                                     const std::string& prop) -> OptVariantType {
                    auto it = fcontext->tagFilters_.find(std::make_pair(tag, prop));
                    if (it == fcontext->tagFilters_.end()) {
                        return Status::Error("Invalid Tag Filter");
                    }
                    VLOG(1) << "Hit srcProp filter for tag " << tag << ", prop "
                            << prop << ", value " << it->second;
                    return it->second;
                };
                auto value = exp_->eval(getters);
                if (!value.ok()) {
                    VLOG(3) << value.status();
                    continue;
                }
                if (value.ok() && !Expression::asBool(value.value())) {
                    VLOG(1) << "Filter the edge "
                            << vId << "-> " << dstId << "@" << rank << ":" << edgeType;
                    continue;
                }
            }
        }

        proc(std::move(reader), key);
        ++cnt;
    }

    return ret;
}

template<typename REQ, typename RESP>
folly::Future<std::vector<OneVertexResp>>
QueryBaseProcessor<REQ, RESP>::asyncProcessBucket(
    BucketIdx bucketIdx, Bucket bucket) {
    folly::Promise<std::vector<OneVertexResp>> pro;
    auto f = pro.getFuture();
    executor_->add([this, p = std::move(pro), bucketIdx, b = std::move(bucket)] () mutable {
        std::vector<OneVertexResp> codes;
        codes.reserve(b.vertices_.size());
        for (auto& pv : b.vertices_) {
            codes.emplace_back(
                pv.first,
                pv.second,
                processVertex(bucketIdx, pv.first, pv.second));
        }
        p.setValue(std::move(codes));
    });
    return f;
}

template<typename REQ, typename RESP>
int32_t QueryBaseProcessor<REQ, RESP>::getBucketsNum(int32_t verticesNum,
                                                     int32_t minVerticesPerBucket,
                                                     int32_t handlerNum) {
    return std::min(std::max(1, verticesNum/minVerticesPerBucket), handlerNum);
}

template<typename REQ, typename RESP>
std::vector<Bucket> QueryBaseProcessor<REQ, RESP>::genBuckets(
                                                    const cpp2::GetNeighborsRequest& req) {
    std::vector<Bucket> buckets;
    int32_t verticesNum = 0;
    for (auto& pv : req.get_parts()) {
        verticesNum += pv.second.size();
    }
    auto bucketsNum = getBucketsNum(verticesNum,
                                    FLAGS_min_vertices_per_bucket,
                                    FLAGS_max_handlers_per_req);
    buckets.resize(bucketsNum);
    auto vNumPerBucket = verticesNum / bucketsNum;
    auto leftVertices = verticesNum % bucketsNum;
    int32_t bucketIndex = -1;
    size_t thresHold = vNumPerBucket;
    for (auto& pv : req.get_parts()) {
        for (auto& vId : pv.second) {
            if (bucketIndex < 0 || buckets[bucketIndex].vertices_.size() >= thresHold) {
                ++bucketIndex;
                thresHold = bucketIndex < leftVertices ? vNumPerBucket + 1 : vNumPerBucket;
                buckets[bucketIndex].vertices_.reserve(thresHold);
            }
            CHECK_LT(bucketIndex, bucketsNum);
            buckets[bucketIndex].vertices_.emplace_back(pv.first, vId);
        }
    }
    return buckets;
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::buildTTLInfoAndRespSchema() {
    if (!this->tagContexts_.empty()) {
        for (auto& tc : this->tagContexts_) {
            nebula::cpp2::Schema respTag;
            for (auto& prop : tc.props_) {
                if (prop.returned_) {
                    VLOG(3) << "Build schema with prop: " << prop.prop_.name
                            << " , type: " << static_cast<int32_t>(prop.type_.type);
                    respTag.columns.emplace_back(
                        this->columnDef(prop.prop_.name, prop.type_.type));
                }
            }

            if (!respTag.columns.empty()) {
                auto it = vertexSchemaResp_.find(tc.tagId_);
                if (it == vertexSchemaResp_.end()) {
                    auto schemaProvider = std::make_shared<ResultSchemaProvider>(respTag);
                    vertexSchema_.emplace(tc.tagId_, std::move(schemaProvider));
                    vertexSchemaResp_.emplace(tc.tagId_, std::move(respTag));
                }
            }

            // build ttl info
            auto tagFound = tagTTLInfo_.find(tc.tagId_);
            if (tagFound != tagTTLInfo_.end()) {
                continue;
            }

            auto tagschema = this->schemaMan_->getTagSchema(spaceId_, tc.tagId_);
            if (!tagschema) {
                VLOG(3) << "Can't find spaceId " << spaceId_ << ", tagId " << tc.tagId_;
                continue;
            }
            const meta::NebulaSchemaProvider* nschema =
                dynamic_cast<const meta::NebulaSchemaProvider*>(tagschema.get());
            if (nschema == NULL) {
                VLOG(3) << "Can't find NebulaSchemaProvider in spaceId " << spaceId_;
                continue;
            }

            const nebula::cpp2::SchemaProp schemaProp = nschema->getProp();

            int64_t ttlDuration = 0;
            if (schemaProp.get_ttl_duration()) {
                ttlDuration = *schemaProp.get_ttl_duration();
            }
            std::string ttlCol;
            if (schemaProp.get_ttl_col()) {
                ttlCol = *schemaProp.get_ttl_col();
            }

            // Only support the specified ttl_col mode
            // Not specifying or non-positive ttl_duration behaves like ttl_duration = infinity
            if (ttlCol.empty() || ttlDuration <= 0) {
                VLOG(3) << "TTL property is invalid";
                continue;
            }

            tagTTLInfo_.emplace(tc.tagId_, std::make_pair(ttlCol, ttlDuration));
        }
    }

    if (!edgeContexts_.empty()) {
        for (const auto& ec : edgeContexts_) {
            nebula::cpp2::Schema respEdge;
            auto& props = ec.second;
            for (auto& p : props) {
                if (!compactDstIdProps_ && p.prop_.name == "_dst") {
                    continue;
                }
                VLOG(3) << "Build schema with prop: " << p.prop_.name
                        << " , type: " << static_cast<int32_t>(p.type_.type);
                respEdge.columns.emplace_back(
                        this->columnDef(p.prop_.name, p.type_.type));
            }

            VLOG(1) << "EdgeType " << ec.first << ", onlyStructure " << respEdge.columns.empty();
            onlyStructures_.emplace(ec.first, respEdge.columns.empty());
            if (!respEdge.columns.empty()) {
                auto it = edgeSchemaResp_.find(ec.first);
                if (it == edgeSchemaResp_.end()) {
                    auto schemaProvider = std::make_shared<ResultSchemaProvider>(respEdge);
                    edgeSchema_.emplace(ec.first, std::move(schemaProvider));
                    VLOG(1) << "Fulfill edge response for edge " << ec.first;
                    edgeSchemaResp_.emplace(ec.first, std::move(respEdge));
                }
            }

            // build ttl info
            auto edgeFound = edgeTTLInfo_.find(ec.first);
            if (edgeFound != edgeTTLInfo_.end()) {
                continue;
            }

            auto edgeschema = this->schemaMan_->getEdgeSchema(spaceId_, std::abs(ec.first));
            if (!edgeschema) {
                VLOG(3) << "Can't find spaceId " << spaceId_ << ", edgeType " << ec.first;
                continue;
            }
            const meta::NebulaSchemaProvider* nschema =
                dynamic_cast<const meta::NebulaSchemaProvider*>(edgeschema.get());
            if (nschema == NULL) {
                VLOG(3) << "Can't find NebulaSchemaProvider in spaceId " << spaceId_;
                continue;
            }

            const nebula::cpp2::SchemaProp schemaProp = nschema->getProp();

            int64_t ttlDuration = 0;
            if (schemaProp.get_ttl_duration()) {
                ttlDuration = *schemaProp.get_ttl_duration();
            }
            std::string ttlCol;
            if (schemaProp.get_ttl_col()) {
                ttlCol = *schemaProp.get_ttl_col();
            }

            // Only support the specified ttl_col mode
            // Not specifying or non-positive ttl_duration behaves like ttl_duration = infinity
            if (ttlCol.empty() || ttlDuration <= 0) {
                VLOG(3) << "TTL property is invalid";
                continue;
            }

            edgeTTLInfo_.emplace(ec.first, std::make_pair(ttlCol, ttlDuration));
        }
    }
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::process(const cpp2::GetNeighborsRequest& req) {
    CHECK_NOTNULL(executor_);
    spaceId_ = req.get_space_id();
    int32_t returnColumnsNum = req.get_return_columns().size();
    VLOG(1) << "Total edge types " << req.edge_types.size()
            << ", total returned columns " << returnColumnsNum
            << ", the first column "
            << (returnColumnsNum > 0 ? req.get_return_columns()[0].name : "");
    VLOG(3) << "Receive request, spaceId " << spaceId_ << ", return cols " << returnColumnsNum;
    tagContexts_.reserve(returnColumnsNum);

    if (req.__isset.edge_types) {
        initEdgeContext(req.edge_types);
    }

    auto retCode = checkAndBuildContexts(req);
    if (retCode != cpp2::ErrorCode::SUCCEEDED) {
        for (auto& p : req.get_parts()) {
            this->pushResultCode(retCode, p.first);
        }
        this->onFinished();
        return;
    }

    // const auto& filter = req.get_filter();
    auto buckets = genBuckets(req);
    beforeProcess(buckets);
    std::vector<folly::Future<std::vector<OneVertexResp>>> results;
    for (unsigned i = 0; i < buckets.size(); i++) {
        results.emplace_back(asyncProcessBucket(i, std::move(buckets[i])));
    }
    folly::collectAll(results).via(executor_).thenTry([
                     this,
                     returnColumnsNum] (auto&& t) mutable {
        CHECK(!t.hasException());
        std::unordered_set<PartitionID> failedParts;
        for (auto& bucketTry : t.value()) {
            CHECK(!bucketTry.hasException());
            for (auto& r : bucketTry.value()) {
                auto& partId = std::get<0>(r);
                auto& ret = std::get<2>(r);
                if (ret != kvstore::ResultCode::SUCCEEDED
                      && failedParts.find(partId) == failedParts.end()) {
                    failedParts.emplace(partId);
                    if (ret == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                        this->handleLeaderChanged(spaceId_, partId);
                    } else {
                        this->pushResultCode(this->to(ret), partId);
                    }
                }
            }
        }
        this->onProcessFinished(returnColumnsNum);
        this->onFinished();
    });
}

}  // namespace storage
}  // namespace nebula
