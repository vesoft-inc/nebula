/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

DECLARE_int32(max_handlers_per_req);
DECLARE_int32(min_vertices_per_bucket);
DECLARE_int32(max_edge_returned_per_vertex);
DECLARE_bool(enable_vertex_cache);
DECLARE_bool(enable_reservoir_sampling);

namespace nebula {
namespace storage {

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::handleVertexProps(
        const std::vector<ReturnProp>& vertexProps) {
    for (size_t i = 0; i < vertexProps.size(); i++) {
        auto tagId = vertexProps[i].entryId_;
        auto iter = this->tagContext_.schemas_.find(tagId);
        if (iter == this->tagContext_.schemas_.end()) {
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
        resultDataSet_.colNames.emplace_back(
            folly::stringPrintf("%d:%s", tagId, tagName.value().c_str()));

        std::vector<PropContext> ctxs;
        auto& props = vertexProps[i].names_;
        for (const auto& name : props) {
            auto field = tagSchema->field(name);
            if (field == nullptr) {
                VLOG(1) << "Can't find prop " << name << " tagId " << tagId;
                return cpp2::ErrorCode::E_TAG_PROP_NOT_FOUND;
            }

            // todo(doodle): perhaps need to dedup here
            PropContext ctx(name.c_str());
            ctx.returned_ = true;
            ctx.field_ = field;
            ctxs.emplace_back(std::move(ctx));
        }
        this->tagContext_.propContexts_.emplace_back(tagId, std::move(ctxs));
        this->tagContext_.indexMap_.emplace(tagId, this->tagContext_.propContexts_.size() - 1);
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::handleEdgeProps(
        const std::vector<ReturnProp>& edgeProps) {
    for (size_t i = 0; i < edgeProps.size(); i++) {
        auto edgeType = edgeProps[i].entryId_;
        auto iter = this->edgeContext_.schemas_.find(std::abs(edgeType));
        if (iter == this->edgeContext_.schemas_.end()) {
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
        resultDataSet_.colNames.emplace_back(
            folly::stringPrintf("%d:%s", edgeType, edgeName.value().c_str()));

        std::vector<PropContext> ctxs;
        auto& props = edgeProps[i].names_;
        for (const auto& name : props) {
            // because there are some reserved edge prop in key (src/dst/type/rank),
            // we can't find those prop in schema
            PropContext ctx(name.c_str());
            auto propIter = std::find_if(kPropsInKey_.begin(), kPropsInKey_.end(),
                                         [&] (const auto& entry) { return entry.first == name; });
            if (propIter != kPropsInKey_.end()) {
                ctx.propInKeyType_ = propIter->second;
            } else {
                auto field = edgeSchema->field(name);
                if (field == nullptr) {
                    VLOG(1) << "Can't find prop " << name << " edgeType " << edgeType;
                    return cpp2::ErrorCode::E_EDGE_PROP_NOT_FOUND;
                }
                ctx.field_ = field;
            }
            ctx.returned_ = true;
            // todo(doodle): perhaps need to dedup here
            ctxs.emplace_back(std::move(ctx));
        }
        this->edgeContext_.propContexts_.emplace_back(edgeType, std::move(ctxs));
        this->edgeContext_.indexMap_.emplace(edgeType, this->edgeContext_.propContexts_.size() - 1);
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::buildFilter(const REQ& req) {
    if (!req.__isset.filter) {
        return cpp2::ErrorCode::SUCCEEDED;
    }
    const auto& filterStr = *req.get_filter();
    if (!filterStr.empty()) {
        // todo(doodle): wait Expression ready, check if filter valid,
        // and add tag filter to tagContexts_
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

template<typename REQ, typename RESP>
void QueryBaseProcessor<REQ, RESP>::buildTagTTLInfo() {
    for (const auto& tc : this->tagContext_.propContexts_) {
        auto tagId = tc.first;
        auto iter = this->tagContext_.schemas_.find(tagId);
        CHECK(iter != this->tagContext_.schemas_.end());
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
    for (const auto& ec : this->edgeContext_.propContexts_) {
        auto edgeType = ec.first;
        auto iter = this->edgeContext_.schemas_.find(std::abs(edgeType));
        CHECK(iter != this->edgeContext_.schemas_.end());
        const auto& edgeSchema = iter->second.back();

        auto ttlInfo = edgeSchema->getTTLInfo();
        if (ttlInfo.ok()) {
            VLOG(2) << "Add ttl col " << ttlInfo.value().first << " of edge " << edgeType;
            edgeContext_.ttlInfo_.emplace(edgeType, std::move(ttlInfo).value());
        }
    }
}

template<typename REQ, typename RESP>
std::vector<ReturnProp> QueryBaseProcessor<REQ, RESP>::buildAllTagProps() {
    std::vector<ReturnProp> result;
    for (const auto& entry : this->tagContext_.schemas_) {
        ReturnProp prop;
        prop.entryId_ = entry.first;
        std::vector<std::string> names;
        const auto& schema = entry.second.back();
        auto count = schema->getNumFields();
        for (size_t i = 0; i < count; i++) {
            auto name = schema->getFieldName(i);
            names.emplace_back(std::move(name));
        }
        prop.names_ = std::move(names);
        result.emplace_back(std::move(prop));
    }
    std::sort(result.begin(), result.end(),
              [&] (const auto& a, const auto& b) { return a.entryId_ < b.entryId_; });
    return result;
}

template<typename REQ, typename RESP>
std::vector<ReturnProp> QueryBaseProcessor<REQ, RESP>::buildAllEdgeProps(
        const cpp2::EdgeDirection& direction) {
    std::vector<ReturnProp> result;
    for (const auto& entry : this->edgeContext_.schemas_) {
        ReturnProp prop;
        prop.entryId_ = entry.first;
        if (direction == cpp2::EdgeDirection::IN_EDGE) {
            prop.entryId_ = -prop.entryId_;
        }
        std::vector<std::string> names;
        const auto& schema = entry.second.back();
        auto count = schema->getNumFields();
        for (size_t i = 0; i < count; i++) {
            auto name = schema->getFieldName(i);
            names.emplace_back(std::move(name));
        }
        prop.names_ = std::move(names);
        if (direction == cpp2::EdgeDirection::BOTH) {
            ReturnProp reverse = prop;
            reverse.entryId_ = -prop.entryId_;
            result.emplace_back(std::move(reverse));
        }
        result.emplace_back(std::move(prop));
    }
    std::sort(result.begin(), result.end(),
              [&] (const auto& a, const auto& b) { return a.entryId_ < b.entryId_; });
    return result;
}

template<typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::prepareVertexProps(
        const std::vector<cpp2::PropExp>& vertexProps,
        std::vector<ReturnProp>& returnProps) {
    // todo(doodle): wait
    /*
    for (auto& vertexProp : vertexProps) {
        // If there is no property specified, add all property of latest schema to vertexProps
        if (vertexProp.names.empty()) {
            auto tagId = vertexProp.tag;
            auto tagSchema = env_->schemaMan_->getTagSchema(spaceId_, tagId);
            if (!tagSchema) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " tag " << tagId;
                return cpp2::ErrorCode::E_TAG_NOT_FOUND;
            }

            auto count = tagSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = tagSchema->getFieldName(i);
                vertexProp.names.emplace_back(std::move(name));
            }
        }
    }
    */
    UNUSED(vertexProps);
    UNUSED(returnProps);
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::prepareEdgeProps(
        const std::vector<cpp2::PropExp>& edgeProps,
        std::vector<ReturnProp>& returnProps) {
    // todo(doodle): wait
    /*
    for (auto& edgeProp : edgeProps) {
        // If there is no property specified, add all property of latest schema to edgeProps
        if (edgeProp.names.empty()) {
            auto edgeType = edgeProp.type;
            auto edgeSchema = env_->schemaMan_->getEdgeSchema(spaceId_, std::abs(edgeType));
            if (!edgeSchema) {
                VLOG(1) << "Can't find spaceId " << spaceId_ << " edgeType " << edgeType;
                return cpp2::ErrorCode::E_EDGE_NOT_FOUND;
            }

            auto count = edgeSchema->getNumFields();
            for (size_t i = 0; i < count; i++) {
                auto name = edgeSchema->getFieldName(i);
                edgeProp.names.emplace_back(std::move(name));
            }
        }
    }
    */
    UNUSED(edgeProps);
    UNUSED(returnProps);
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::getSpaceVertexSchema() {
    auto tags = this->env_->schemaMan_->getAllVerTagSchema(spaceId_);
    if (!tags.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }
    this->tagContext_.schemas_ = std::move(tags).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

template <typename REQ, typename RESP>
cpp2::ErrorCode QueryBaseProcessor<REQ, RESP>::getSpaceEdgeSchema() {
    auto edges = this->env_->schemaMan_->getAllVerEdgeSchema(spaceId_);
    if (!edges.ok()) {
        return cpp2::ErrorCode::E_SPACE_NOT_FOUND;
    }

    this->edgeContext_.schemas_ = std::move(edges).value();
    return cpp2::ErrorCode::SUCCEEDED;
}


}  // namespace storage
}  // namespace nebula
