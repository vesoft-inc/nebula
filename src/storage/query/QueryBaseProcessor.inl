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
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::handleVertexProps(
        std::vector<cpp2::VertexProp>& vertexProps) {
    for (auto& vertexProp : vertexProps) {
        auto tagId = vertexProp.tag;
        auto iter = tagContext_.schemas_.find(tagId);
        if (iter == tagContext_.schemas_.end()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }
        CHECK(!iter->second.empty());
        const auto& tagSchema = iter->second.back();
        auto tagName = this->env_->schemaMan_->toTagName(spaceId_, tagId);
        if (!tagName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
            return cpp2::ErrorCode::E_TAG_NOT_FOUND;
        }

        std::vector<PropContext> ctxs;
        if (!vertexProp.props.empty()) {
            for (const auto& name : vertexProp.props) {
                auto field = tagSchema->field(name);
                if (field == nullptr) {
                    VLOG(1) << "Can't find prop " << name << " tagId " << tagId;
                    return cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
                }
                addReturnPropContext(ctxs, name.c_str(), field);
            }
        } else {
            // if the list of property names is empty, then all properties on the given tag
            // will be returned
            auto count = tagSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = tagSchema->getFieldName(i);
                vertexProp.props.emplace_back(name);
                auto field = tagSchema->field(i);
                addReturnPropContext(ctxs, name, field);
            }
        }
        tagContext_.propContexts_.emplace_back(tagId, std::move(ctxs));
        tagContext_.indexMap_.emplace(tagId, tagContext_.propContexts_.size() - 1);
        tagContext_.tagNames_.emplace(tagId, std::move(tagName).value());
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::handleEdgeProps(
        std::vector<cpp2::EdgeProp>& edgeProps) {
    for (auto& edgeProp : edgeProps) {
        auto edgeType = edgeProp.type;
        auto iter = edgeContext_.schemas_.find(std::abs(edgeType));
        if (iter == edgeContext_.schemas_.end()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }
        CHECK(!iter->second.empty());
        const auto& edgeSchema = iter->second.back();
        auto edgeName = this->env_->schemaMan_->toEdgeName(spaceId_, std::abs(edgeType));
        if (!edgeName.ok()) {
            VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
            return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
        }

        std::vector<PropContext> ctxs;
        if (!edgeProp.props.empty()) {
            for (const auto& name : edgeProp.props) {
                if (name != kSrc && name != kType && name != kRank && name != kDst) {
                    auto field = edgeSchema->field(name);
                    if (field == nullptr) {
                        VLOG(1) << "Can't find prop " << name << " edgeType " << edgeType;
                        return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                    }
                    addReturnPropContext(ctxs, name.c_str(), field);
                } else {
                    addReturnPropContext(ctxs, name.c_str(), nullptr);
                }
            }
        } else {
            // if the list of property names is empty, then all properties on the given edgeType
            // will be returned
            addReturnPropContext(ctxs, kSrc, nullptr);
            addReturnPropContext(ctxs, kType, nullptr);
            addReturnPropContext(ctxs, kRank, nullptr);
            addReturnPropContext(ctxs, kDst, nullptr);
            edgeProp.props.emplace_back(kSrc);
            edgeProp.props.emplace_back(kType);
            edgeProp.props.emplace_back(kRank);
            edgeProp.props.emplace_back(kDst);
            auto count = edgeSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = edgeSchema->getFieldName(i);
                edgeProp.props.emplace_back(name);
                auto field = edgeSchema->field(i);
                addReturnPropContext(ctxs, name, field);
            }
        }
        edgeContext_.propContexts_.emplace_back(edgeType, std::move(ctxs));
        edgeContext_.indexMap_.emplace(edgeType, edgeContext_.propContexts_.size() - 1);
        edgeContext_.edgeNames_.emplace(edgeType, std::move(edgeName).value());
    }
    return cpp2::ErrorCode::SUCCEEDED;
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
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::buildYields(const REQ& req) {
    const auto& traverseSpec = req.get_traverse_spec();
    resultDataSet_.colNames.emplace_back("_expr");
    if (!traverseSpec.__isset.expressions) {
        return cpp2::ErrorCode::SUCCEEDED;
    }
    // todo(doodle): support expression yields later
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::buildFilter(const REQ& req) {
    const auto& traverseSpec = req.get_traverse_spec();
    if (!traverseSpec.__isset.filter) {
        return cpp2::ErrorCode::SUCCEEDED;
    }
    const auto& filterStr = *traverseSpec.get_filter();
    if (!filterStr.empty()) {
        // the filter expression **must** return a bool
        filter_ = std::move(Expression::decode(filterStr));
        if (filter_ == nullptr) {
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
        return checkExp(filter_.get(), false, true);
    }
    return cpp2::ErrorCode::SUCCEEDED;
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
        tagProp.tag = entry.first;
        const auto& schema = entry.second.back();
        auto count = schema->getNumFields();
        for (size_t i = 0; i < count; i++) {
            auto name = schema->getFieldName(i);
            tagProp.props.emplace_back(name);
        }
        result.emplace_back(std::move(tagProp));
    }
    std::sort(result.begin(), result.end(),
              [&] (const auto& a, const auto& b) { return a.tag < b.tag; });
    return result;
}

template<typename REQ, typename RESP>
std::vector<cpp2::EdgeProp> QueryBaseProcessor<REQ, RESP>::buildAllEdgeProps(
        const cpp2::EdgeDirection& direction) {
    std::vector<cpp2::EdgeProp> result;
    for (const auto& entry : edgeContext_.schemas_) {
        cpp2::EdgeProp edgeProp;
        edgeProp.type = entry.first;
        if (direction == cpp2::EdgeDirection::IN_EDGE) {
            edgeProp.type = -edgeProp.type;
        }

        // add default property in key
        edgeProp.props.emplace_back(kSrc);
        edgeProp.props.emplace_back(kType);
        edgeProp.props.emplace_back(kRank);
        edgeProp.props.emplace_back(kDst);

        const auto& schema = entry.second.back();
        auto count = schema->getNumFields();
        for (size_t i = 0; i < count; i++) {
            auto name = schema->getFieldName(i);
            edgeProp.props.emplace_back(name);
        }
        if (direction == cpp2::EdgeDirection::BOTH) {
            cpp2::EdgeProp reverse = edgeProp;
            reverse.type = -edgeProp.type;
            result.emplace_back(std::move(reverse));
        }
        result.emplace_back(std::move(edgeProp));
    }
    std::sort(result.begin(), result.end(),
              [&] (const auto& a, const auto& b) { return a.type < b.type; });
    return result;
}

template <typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::getSpaceVertexSchema() {
    auto tags = this->env_->schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    tagContext_.schemas_ = std::move(tags).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::getSpaceEdgeSchema() {
    auto edges = this->env_->schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    edgeContext_.schemas_ = std::move(edges).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::checkExp(const Expression* exp,
                                                        bool returned,
                                                        bool filtered,
                                                        bool updated) {
    switch (exp->kind()) {
        case Expression::Kind::kConstant:
            return cpp2::ErrorCode::SUCCEEDED;
        case Expression::Kind::kAdd:
        case Expression::Kind::kMinus:
        case Expression::Kind::kMultiply:
        case Expression::Kind::kDivision:
        case Expression::Kind::kMod: {
            auto* ariExp = static_cast<const ArithmeticExpression*>(exp);
            auto ret = checkExp(ariExp->left(), returned, filtered, updated);
            if (ret != cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            return checkExp(ariExp->right(), returned, filtered, updated);
        }
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
            if (ret != cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            return checkExp(relExp->right(), returned, filtered, updated);
        }
        case Expression::Kind::kList: {
            auto* listExp = static_cast<const ListExpression*>(exp);
            for (auto& item : listExp->items()) {
                auto ret = checkExp(item.get(), returned, filtered, updated);
                if (ret != cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kSet: {
            auto* setExp = static_cast<const SetExpression*>(exp);
            for (auto& item : setExp->items()) {
                auto ret = checkExp(item.get(), returned, filtered, updated);
                if (ret != cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kMap: {
            auto* mapExp = static_cast<const MapExpression*>(exp);
            for (auto& item : mapExp->items()) {
                auto ret = checkExp(item.second.get(), returned, filtered, updated);
                if (ret != cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kLogicalAnd:
        case Expression::Kind::kLogicalOr:
        case Expression::Kind::kLogicalXor: {
            auto* logExp = static_cast<const LogicalExpression*>(exp);
            auto ret = checkExp(logExp->left(), returned, filtered, updated);
            if (ret != cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
            return checkExp(logExp->right(), returned, filtered, updated);
        }
        case Expression::Kind::kTypeCasting: {
            auto* typExp = static_cast<const TypeCastingExpression*>(exp);
            return checkExp(typExp->operand(), returned, filtered, updated);
        }
        case Expression::Kind::kFunctionCall: {
            auto* funExp = static_cast<const FunctionCallExpression*>(exp);
            auto& args = funExp->args()->args();
            for (auto iter = args.begin(); iter < args.end(); ++iter) {
                auto ret = checkExp(iter->get(), returned, filtered, updated);
                if (ret != cpp2::ErrorCode::SUCCEEDED) {
                    return ret;
                }
            }
            return cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kSrcProperty: {
            auto* sourceExp = static_cast<const SourcePropertyExpression*>(exp);
            const auto* tagName = sourceExp->sym();
            const auto* propName = sourceExp->prop();
            auto tagRet = this->env_->schemaMan_->toTagID(spaceId_, *tagName);
            if (!tagRet.ok()) {
                VLOG(1) << "Can't find tag " << *tagName << ", in space " << spaceId_;
                return cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }

            auto tagId = tagRet.value();
            auto iter = tagContext_.schemas_.find(tagId);
            if (iter == tagContext_.schemas_.end()) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " tagId " << tagId;
                return cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }
            CHECK(!iter->second.empty());
            const auto& tagSchema = iter->second.back();

            auto field = tagSchema->field(*propName);
            if (field == nullptr) {
                VLOG(1) << "Can't find related prop " << *propName << " on tag " << *tagName;
                return cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
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
                valueProps_.emplace(*propName);
            }
            return cpp2::ErrorCode::SUCCEEDED;
        }
        case Expression::Kind::kEdgeRank:
        case Expression::Kind::kEdgeDst:
        case Expression::Kind::kEdgeSrc:
        case Expression::Kind::kEdgeType:
        case Expression::Kind::kEdgeProperty: {
            auto* edgeExp = static_cast<const PropertyExpression*>(exp);
            const auto* edgeName = edgeExp->sym();
            const auto* propName = edgeExp->prop();
            auto edgeRet = this->env_->schemaMan_->toEdgeType(spaceId_, *edgeName);
            if (!edgeRet.ok()) {
                VLOG(1) << "Can't find edge " << *edgeName << ", in space " << spaceId_;
                return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }

            auto edgeType = edgeRet.value();
            auto iter = edgeContext_.schemas_.find(std::abs(edgeType));
            if (iter == edgeContext_.schemas_.end()) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << std::abs(edgeType);
                return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }
            CHECK(!iter->second.empty());
            const auto& edgeSchema = iter->second.back();

            const meta::SchemaProviderIf::Field* field = nullptr;
            if (exp->kind() == Expression::Kind::kEdgeProperty) {
                field = edgeSchema->field(*propName);
                if (field == nullptr) {
                    VLOG(1) << "Can't find related prop " << *propName << " on edge " << *edgeName;
                    return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                }
            }
            /*
            Because we can't distinguish the edgeType should be positive or negative by edge
            name, as for expression of edge properties, we need extra information to decide.
            So when checkExp is called, user need to make sure that related contexts have been
            set correctly, which are propContexts_, indexMap_, edgeNames_ in EdgeContext.
            */
            if (edgeContext_.indexMap_.count(edgeType)) {
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
            if (updated) {
                valueProps_.emplace(*propName);
            }
            return cpp2::ErrorCode::SUCCEEDED;
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
        case Expression::Kind::kVar:
        case Expression::Kind::kVersionedVar:
        default: {
            LOG(INFO) << "Unimplemented expression type! kind = " << exp->kind();
            return cpp2::ErrorCode::E_INVALID_FILTER;
        }
    }
    return cpp2::ErrorCode::E_INVALID_FILTER;
}

template <typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::addPropContextIfNotExists(
        std::vector<std::pair<SchemaID, std::vector<PropContext>>>& propContexts,
        std::unordered_map<SchemaID, size_t>& indexMap,
        std::unordered_map<SchemaID, std::string>& names,
        int32_t entryId,
        const std::string* entryName,
        const std::string* propName,
        const meta::SchemaProviderIf::Field* field,
        bool returned,
        bool filtered,
        const std::pair<size_t, cpp2::StatType>* statInfo) {
    auto idxIter = indexMap.find(entryId);
    if (idxIter == indexMap.end()) {
        // if no property of tag/edge has been add to propContexts
        PropContext ctx(propName->c_str(), field, returned, filtered, statInfo);
        std::vector<PropContext> ctxs;
        ctxs.emplace_back(std::move(ctx));
        propContexts.emplace_back(entryId, std::move(ctxs));
        indexMap.emplace(entryId, propContexts.size() - 1);
        names.emplace(entryId, *entryName);
    } else {
        // some property of tag/edge has been add to propContexts
        auto& props = propContexts[idxIter->second].second;
        auto iter = std::find_if(props.begin(), props.end(), [propName] (const auto& prop) {
                                 return prop.name_ == *propName; });
        if (iter == props.end()) {
            // this prop has not been add to propContexts
            PropContext ctx(propName->c_str(), field, returned, filtered, statInfo);
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
