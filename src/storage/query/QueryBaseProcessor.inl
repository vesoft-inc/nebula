/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

DECLARE_int32(max_handlers_per_req);
DECLARE_int32(min_vertices_per_bucket);
DECLARE_bool(enable_vertex_cache);

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
nebula::cpp2::ErrorCode
QueryBaseProcessor<REQ, RESP>::handleVertexProps(std::vector<cpp2::VertexProp>& vertexProps) {
    for (auto& vertexProp : vertexProps) {
        auto tagId = vertexProp.get_tag();
        auto iter = tagContext_.schemas_.find(tagId);
        if (iter == tagContext_.schemas_.end()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        CHECK(!iter->second.empty());
        const auto& tagSchema = iter->second.back();
        auto tagName = this->env_->schemaMan_->toTagName(spaceId_, tagId);
        if (!tagName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }

        std::vector<PropContext> ctxs;
        if (!(*vertexProp.props_ref()).empty()) {
            for (const auto& name : *vertexProp.props_ref()) {
                if (name != kVid && name != kTag) {
                    auto field = tagSchema->field(name);
                    if (field == nullptr) {
                        VLOG(1) << "Can't find prop " << name << " tagId " << tagId;
                        return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
                    }
                    addReturnPropContext(ctxs, name.c_str(), field);
                } else {
                    addReturnPropContext(ctxs, name.c_str(), nullptr);
                }
            }
        } else {
            // if the list of property names is empty, then all properties on the given tag
            // will be returned
            auto count = tagSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = tagSchema->getFieldName(i);
                (*vertexProp.props_ref()).emplace_back(name);
                auto field = tagSchema->field(i);
                addReturnPropContext(ctxs, name, field);
            }
        }
        tagContext_.propContexts_.emplace_back(tagId, std::move(ctxs));
        tagContext_.indexMap_.emplace(tagId, tagContext_.propContexts_.size() - 1);
        tagContext_.tagNames_.emplace(tagId, std::move(tagName).value());
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
nebula::cpp2::ErrorCode
QueryBaseProcessor<REQ, RESP>::handleEdgeProps(std::vector<cpp2::EdgeProp>& edgeProps) {
    for (auto& edgeProp : edgeProps) {
        auto edgeType = edgeProp.get_type();
        auto iter = edgeContext_.schemas_.find(std::abs(edgeType));
        if (iter == edgeContext_.schemas_.end()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        CHECK(!iter->second.empty());
        const auto& edgeSchema = iter->second.back();
        auto edgeName = this->env_->schemaMan_->toEdgeName(spaceId_, std::abs(edgeType));
        if (!edgeName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }

        std::vector<PropContext> ctxs;
        if (!(*edgeProp.props_ref()).empty()) {
            for (const auto& name : (*edgeProp.props_ref())) {
                if (name != kSrc && name != kType && name != kRank && name != kDst) {
                    auto field = edgeSchema->field(name);
                    if (field == nullptr) {
                        VLOG(1) << "Can't find prop " << name << " edgeType " << edgeType;
                        return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                    }
                    addReturnPropContext(ctxs, name.c_str(), field);
                } else {
                    addReturnPropContext(ctxs, name.c_str(), nullptr);
                }
            }
        } else {
            // if the list of property names is empty, then all properties on the given edgeType
            // will be returned
            auto count = edgeSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = edgeSchema->getFieldName(i);
                (*edgeProp.props_ref()).emplace_back(name);
                auto field = edgeSchema->field(i);
                addReturnPropContext(ctxs, name, field);
            }
        }
        edgeContext_.propContexts_.emplace_back(edgeType, std::move(ctxs));
        edgeContext_.indexMap_.emplace(edgeType, edgeContext_.propContexts_.size() - 1);
        edgeContext_.edgeNames_.emplace(edgeType, std::move(edgeName).value());
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::addReturnPropContext(
        std::vector<PropContext>& ctxs,
        const char* propName,
        const meta::SchemaProviderIf::Field* field) {
    PropContext ctx(propName);
    ctx.returned_ = true;
    ctx.field_ = field;
    ctxs.emplace_back(std::move(ctx));
}

template<typename REQ, typename RESP>
nebula::cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::buildYields(const REQ& req) {
    const auto& traverseSpec = req.get_traverse_spec();
    resultDataSet_.colNames.emplace_back("_expr");
    if (!traverseSpec.expressions_ref().has_value()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    // todo(doodle): support expression yields later
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
nebula::cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::buildFilter(const REQ& req) {
    const auto& traverseSpec = req.get_traverse_spec();
    if (!traverseSpec.filter_ref().has_value()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
    const auto& filterStr = *traverseSpec.filter_ref();

    auto pool = &this->planContext_->objPool_;
    // auto v = env_;
    if (!filterStr.empty()) {
        // the filter expression **must** return a bool
        filter_ = Expression::decode(pool, filterStr);
        if (filter_ == nullptr) {
            return nebula::cpp2::ErrorCode::E_INVALID_FILTER;
        }
        return checkExp(filter_, false, true);
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::buildTagTTLInfo() {
    for (const auto& tc : tagContext_.propContexts_) {
        auto tagId = tc.first;
        auto iter = tagContext_.schemas_.find(tagId);
        CHECK(iter != tagContext_.schemas_.end());
        const auto& tagSchema = iter->second.back();

        auto ttlInfo = tagSchema->getTTLInfo();
        if (ttlInfo.ok()) {
            VLOG(2) << "Add ttl col " << ttlInfo.value().first << " of tag " << tagId;
            tagContext_.ttlInfo_.emplace(tagId, std::move(ttlInfo).value());
        }
    }
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::buildEdgeTTLInfo() {
    for (const auto& ec : edgeContext_.propContexts_) {
        auto edgeType = ec.first;
        auto iter = edgeContext_.schemas_.find(std::abs(edgeType));
        CHECK(iter != edgeContext_.schemas_.end());
        const auto& edgeSchema = iter->second.back();

        auto ttlInfo = edgeSchema->getTTLInfo();
        if (ttlInfo.ok()) {
            VLOG(2) << "Add ttl col " << ttlInfo.value().first << " of edge " << edgeType;
            edgeContext_.ttlInfo_.emplace(edgeType, std::move(ttlInfo).value());
        }
    }
}

template<typename REQ, typename RESP>
std::vector<cpp2::VertexProp> QueryBaseProcessor<REQ, RESP>::buildAllTagProps() {
    std::vector<cpp2::VertexProp> result;
    for (const auto& entry : tagContext_.schemas_) {
        cpp2::VertexProp tagProp;
        tagProp.set_tag(entry.first);
        const auto& schema = entry.second.back();
        auto count = schema->getNumFields();
        for (size_t i = 0; i < count; i++) {
            auto name = schema->getFieldName(i);
            (*tagProp.props_ref()).emplace_back(name);
        }
        result.emplace_back(std::move(tagProp));
    }
    std::sort(result.begin(), result.end(),
              [&] (const auto& a, const auto& b) { return a.get_tag() < b.get_tag(); });
    return result;
}

template<typename REQ, typename RESP>
std::vector<cpp2::EdgeProp> QueryBaseProcessor<REQ, RESP>::buildAllEdgeProps(
        const cpp2::EdgeDirection& direction) {
    std::vector<cpp2::EdgeProp> result;
    for (const auto& entry : edgeContext_.schemas_) {
        cpp2::EdgeProp edgeProp;
        edgeProp.set_type(entry.first);
        if (direction == cpp2::EdgeDirection::IN_EDGE) {
            edgeProp.set_type(-edgeProp.get_type());
        }
        const auto& schema = entry.second.back();
        auto count = schema->getNumFields();
        for (size_t i = 0; i < count; i++) {
            auto name = schema->getFieldName(i);
            (*edgeProp.props_ref()).emplace_back(name);
        }
        if (direction == cpp2::EdgeDirection::BOTH) {
            cpp2::EdgeProp reverse = edgeProp;
            reverse.set_type(-edgeProp.get_type());
            result.emplace_back(std::move(reverse));
        }
        result.emplace_back(std::move(edgeProp));
    }
    std::sort(result.begin(), result.end(),
              [&] (const auto& a, const auto& b) { return a.get_type() < b.get_type(); });
    return result;
}

template <typename REQ, typename RESP>
nebula::cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::getSpaceVertexSchema() {
    auto tags = this->env_->schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    tagContext_.schemas_ = std::move(tags).value();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
nebula::cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::getSpaceEdgeSchema() {
    auto edges = this->env_->schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        return nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    edgeContext_.schemas_ = std::move(edges).value();
    return nebula::cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
nebula::cpp2::ErrorCode
QueryBaseProcessor<REQ, RESP>::checkExp(const Expression* exp,
                                        bool returned,
                                        bool filtered,
                                        bool updated) {
    switch (exp->kind()) {
        case Expression::Kind::kConstant: {
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kVar: {
            auto* varExp = static_cast<const VariableExpression*>(exp);
            return varExp->isInner() ? nebula::cpp2::ErrorCode::SUCCEEDED
                                     : nebula::cpp2::ErrorCode::E_INVALID_FILTER;
        }
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod: {
            auto* ariExp = static_cast<const ArithmeticExpression*>(exp);
            auto ret = checkExp(ariExp->left(), returned, filtered, updated);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            return checkExp(ariExp->right(), returned, filtered, updated);
        }
        case Expression::Kind::kIsNull:
        case Expression::Kind::kIsNotNull:
        case Expression::Kind::kIsEmpty:
        case Expression::Kind::kIsNotEmpty:
        case Expression::Kind::kUnaryPlus:
        case Expression::Kind::kUnaryNegate:
        case Expression::Kind::kUnaryNot:
        case Expression::Kind::kUnaryIncr:
        case Expression::Kind::kUnaryDecr: {
            auto* unaExp = static_cast<const UnaryExpression*>(exp);
            return checkExp(unaExp->operand(), returned, filtered, updated);
        }
        case Expression::Kind::kRelEQ:
        case Expression::Kind::kRelNE:
        case Expression::Kind::kRelLT:
        case Expression::Kind::kRelLE:
        case Expression::Kind::kRelGT:
        case Expression::Kind::kRelGE:
        case Expression::Kind::kRelREG:
        case Expression::Kind::kContains:
        case Expression::Kind::kNotContains:
        case Expression::Kind::kStartsWith:
        case Expression::Kind::kNotStartsWith:
        case Expression::Kind::kEndsWith:
        case Expression::Kind::kNotEndsWith:
        case Expression::Kind::kRelNotIn:
        case Expression::Kind::kRelIn: {
            auto* relExp = static_cast<const RelationalExpression*>(exp);
            auto ret = checkExp(relExp->left(), returned, filtered, updated);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            return checkExp(relExp->right(), returned, filtered, updated);
        }
        case Expression::Kind::kList: {
            auto* listExp = static_cast<const ListExpression*>(exp);
            for (auto& item : listExp->items()) {
                auto ret = checkExp(item, returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kSet: {
            auto* setExp = static_cast<const SetExpression*>(exp);
            for (auto& item : setExp->items()) {
                auto ret = checkExp(item, returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kMap: {
            auto* mapExp = static_cast<const MapExpression*>(exp);
            for (auto& item : mapExp->items()) {
                auto ret = checkExp(item.second, returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kCase: {
            auto* caseExp = static_cast<const CaseExpression*>(exp);
            if (caseExp->hasCondition()) {
                auto ret = checkExp(caseExp->condition(), returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            if (caseExp->hasDefault()) {
                auto ret = checkExp(caseExp->defaultResult(), returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            for (auto& whenThen : caseExp->cases()) {
                auto ret = checkExp(whenThen.when, returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
                ret = checkExp(whenThen.then, returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kListComprehension: {
            auto* lcExp = static_cast<const ListComprehensionExpression*>(exp);
            auto ret = checkExp(lcExp->collection(), returned, filtered, updated);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            if (lcExp->hasFilter()) {
                ret = checkExp(lcExp->filter(), returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            if (lcExp->hasMapping()) {
                ret = checkExp(lcExp->mapping(), returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kPredicate: {
            auto* predExp = static_cast<const PredicateExpression*>(exp);
            auto ret = checkExp(predExp->collection(), returned, filtered, updated);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            ret = checkExp(predExp->filter(), returned, filtered, updated);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kReduce: {
            auto* reduceExp = static_cast<const ReduceExpression*>(exp);
            auto ret = checkExp(reduceExp->collection(), returned, filtered, updated);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            ret = checkExp(reduceExp->mapping(), returned, filtered, updated);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            auto* logExp = static_cast<const LogicalExpression*>(exp);
            for (auto &expr : logExp->operands()) {
                auto ret = checkExp(expr, returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kTypeCasting: {
            auto* typExp = static_cast<const TypeCastingExpression*>(exp);
            return checkExp(typExp->operand(), returned, filtered, updated);
        }
        case Expression::Kind::kFunctionCall: {
            auto* funExp = static_cast<const FunctionCallExpression*>(exp);
            auto& args = funExp->args()->args();
            for (auto iter = args.begin(); iter < args.end(); ++iter) {
                auto ret = checkExp(*iter, returned, filtered, updated);
                if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kSrcProperty: {
            auto* sourceExp = static_cast<const SourcePropertyExpression*>(exp);
            const auto& tagName = sourceExp->sym();
            const auto& propName = sourceExp->prop();
            auto tagRet = this->env_->schemaMan_->toTagID(spaceId_, tagName);
            if (!tagRet.ok()) {
                VLOG(1) << "Can't find tag " << tagName << ", in space " << spaceId_;
                return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }

            auto tagId = tagRet.value();
            auto iter = tagContext_.schemas_.find(tagId);
            if (iter == tagContext_.schemas_.end()) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
                return nebula::cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }
            CHECK(!iter->second.empty());
            const auto& tagSchema = iter->second.back();

            if (propName == kVid || propName == kTag) {
                return nebula::cpp2::ErrorCode::SUCCEEDED;
            }

            auto field = tagSchema->field(propName);
            if (field == nullptr) {
                VLOG(1) << "Can't find related prop " << propName << " on tag " << tagName;
                return nebula::cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            }

            addPropContextIfNotExists(tagContext_.propContexts_,
                                      tagContext_.indexMap_,
                                      tagContext_.tagNames_,
                                      tagId,
                                      tagName,
                                      propName,
                                      field,
                                      returned,
                                      filtered);
            if (updated) {
                valueProps_.emplace(propName);
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeProperty: {
            auto* edgeExp = static_cast<const PropertyExpression*>(exp);
            const auto& edgeName = edgeExp->sym();
            const auto& propName = edgeExp->prop();
            auto edgeRet = this->env_->schemaMan_->toEdgeType(spaceId_, edgeName);
            if (!edgeRet.ok()) {
                VLOG(1) << "Can't find edge " << edgeName << ", in space " << spaceId_;
                return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }

            auto edgeType = edgeRet.value();
            auto iter = edgeContext_.schemas_.find(std::abs(edgeType));
            if (iter == edgeContext_.schemas_.end()) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << std::abs(edgeType);
                return nebula::cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }
            CHECK(!iter->second.empty());
            const auto& edgeSchema = iter->second.back();

            if (propName == kSrc || propName == kType ||
                propName == kRank || propName == kDst) {
                return nebula::cpp2::ErrorCode::SUCCEEDED;
            }

            const meta::SchemaProviderIf::Field* field = nullptr;
            if (exp->kind() == Expression::Kind::kEdgeProperty) {
                field = edgeSchema->field(propName);
                if (field == nullptr) {
                    VLOG(1) << "Can't find related prop " << propName << " on edge " << edgeName;
                    return nebula::cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                }
            }
            /*
            Because we can't distinguish the edgeType should be positive or negative by edge
            name, as for expression of edge properties, we need extra information to decide.
            So when checkExp is called, user need to make sure that related contexts have been
            set correctly, which are propContexts_, indexMap_, edgeNames_ in EdgeContext.
            */
            bool inEdgeContext = false;
            if (edgeContext_.indexMap_.count(edgeType)) {
                inEdgeContext = true;
                addPropContextIfNotExists(edgeContext_.propContexts_,
                                          edgeContext_.indexMap_,
                                          edgeContext_.edgeNames_,
                                          edgeType,
                                          edgeName,
                                          propName,
                                          field,
                                          returned,
                                          filtered);
            }
            if (edgeContext_.indexMap_.count(-edgeType)) {
                inEdgeContext = true;
                addPropContextIfNotExists(edgeContext_.propContexts_,
                                          edgeContext_.indexMap_,
                                          edgeContext_.edgeNames_,
                                          -edgeType,
                                          edgeName,
                                          propName,
                                          field,
                                          returned,
                                          filtered);
            }
            if (!inEdgeContext) {
                VLOG(1) << "Edge context not find related edge " << edgeName;
                return nebula::cpp2::ErrorCode::E_MUTATE_EDGE_CONFLICT;
            }
            if (updated) {
                valueProps_.emplace(propName);
            }
            return nebula::cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kInputProperty:
        case Expression::Kind::kSubscript:
        case Expression::Kind::kAttribute:
        case Expression::Kind::kLabelAttribute:
        case Expression::Kind::kTagProperty:
        case Expression::Kind::kVertex:
        case Expression::Kind::kEdge:
        case Expression::Kind::kLabel:
        case Expression::Kind::kVarProperty:
        case Expression::Kind::kDstProperty:
        case Expression::Kind::kUUID:
        case Expression::Kind::kPathBuild:
        case Expression::Kind::kColumn:
        case Expression::Kind::kTSPrefix:
        case Expression::Kind::kTSWildcard:
        case Expression::Kind::kTSRegexp:
        case Expression::Kind::kTSFuzzy:
        case Expression::Kind::kAggregate:
        case Expression::Kind::kSubscriptRange:
        case Expression::Kind::kVersionedVar: {
            LOG(ERROR) << "Unimplemented expression type! kind = " << exp->kind();
            return nebula::cpp2::ErrorCode::E_INVALID_FILTER;
        }
    }
    return nebula::cpp2::ErrorCode::E_INVALID_FILTER;
}

template <typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::addPropContextIfNotExists(
        std::vector<std::pair<SchemaID, std::vector<PropContext>>>& propContexts,
        std::unordered_map<SchemaID, size_t>& indexMap,
        std::unordered_map<SchemaID, std::string>& names,
        int32_t entryId,
        const std::string& entryName,
        const std::string& propName,
        const meta::SchemaProviderIf::Field* field,
        bool returned,
        bool filtered,
        const std::pair<size_t, cpp2::StatType>* statInfo) {
    auto idxIter = indexMap.find(entryId);
    if (idxIter == indexMap.end()) {
        // if no property of tag/edge has been add to propContexts
        PropContext ctx(propName.c_str(), field, returned, filtered, statInfo);
        std::vector<PropContext> ctxs;
        ctxs.emplace_back(std::move(ctx));
        propContexts.emplace_back(entryId, std::move(ctxs));
        indexMap.emplace(entryId, propContexts.size() - 1);
        names.emplace(entryId, entryName);
    } else {
        // some property of tag/edge has been add to propContexts
        auto& props = propContexts[idxIter->second].second;
        auto iter = std::find_if(props.begin(), props.end(), [&propName] (const auto& prop) {
                                 return prop.name_ == propName; });
        if (iter == props.end()) {
            // this prop has not been add to propContexts
            PropContext ctx(propName.c_str(), field, returned, filtered, statInfo);
            props.emplace_back(std::move(ctx));
        } else {
            // this prop been add to propContexts, just update the flag
            iter->returned_ |= returned;
            iter->filtered_ |= filtered;
            if (statInfo != nullptr) {
                iter->addStat(statInfo);
            }
        }
    }
}

}  // namespace storage
}  // namespace nebula
