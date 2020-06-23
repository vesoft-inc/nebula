/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "tools/storage-client/src/lib/NebulaStorageClientQueryImpl.h"
#include "tools/storage-client/src/lib/ExpressionsParser.h"
#include <interface/gen-cpp2/common_types.h>

namespace nebula {

ResultCode NebulaStorageClientQueryImpl::verifyLookupContext(const LookupContext& context) {
    indexId_ = getIndexId(context.isEdge_, context.index_);
    if (indexId_ == -1) {
        return ResultCode::E_INDEX_NOT_FOUND;
    }

    auto edgeOrTagId = getSchemaIdFromIndex(context.isEdge_, indexId_);
    if (edgeOrTagId == -1) {
        return context.isEdge_ ? ResultCode::E_EDGE_NOT_FOUND : ResultCode::E_TAG_NOT_FOUND;
    }

    auto schema = getLatestVersionSchema(context.isEdge_, edgeOrTagId);
    if (!schema) {
        return context.isEdge_ ? ResultCode::E_EDGE_NOT_FOUND : ResultCode::E_TAG_NOT_FOUND;
    }

    auto ret = verifyLookupReturnCols(context.returnCols_, schema.get(), context.isEdge_);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }

    ret = verifyLookupFilter(context.filter_);
    return ret;
}

ResultCode NebulaStorageClientQueryImpl::verifyLookupReturnCols(
    const std::vector<std::string>& cols,
    const meta::SchemaProviderIf* schema,
    bool isEdge) {
    auto owner = isEdge ? PropOwner::EDGE : PropOwner::TAG;
    if (isEdge) {
        resultSet_.returnCols.emplace_back(ColumnDef(owner, "SrcVID"));
        resultSet_.returnCols.emplace_back(ColumnDef(owner, "DstVID"));
        resultSet_.returnCols.emplace_back(ColumnDef(owner, "Ranking"));
        resultSet_.columnsType.emplace_back(DataType::VID);
        resultSet_.columnsType.emplace_back(DataType::VID);
        resultSet_.columnsType.emplace_back(DataType::INT);
    } else {
        resultSet_.returnCols.emplace_back(ColumnDef(PropOwner::TAG, "VertexID"));
        resultSet_.columnsType.emplace_back(DataType::VID);
    }
    for (const auto& returnCol : cols) {
        resultSet_.returnCols.emplace_back(ColumnDef(owner, returnCol));
        auto t = toClientType(schema->getFieldType(returnCol).type);
        if (t == DataType::UNKNOWN) {
            LOG(ERROR) << "column type error by column : " << returnCol;
            return ResultCode::E_IMPROPER_DATA_TYPE;
        }
        resultSet_.columnsType.emplace_back(t);
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientQueryImpl::verifyLookupFilter(const std::string& filter) {
    if (filter.empty()) {
        return ResultCode::E_INVALID_FILTER;
    }
    auto ret = ExpressionsParser::encodeExpression(filter, filter_);
    if (!ret) {
        return ResultCode::E_INVALID_FILTER;
    }
    if (!filter_.empty()) {
        auto expr = Expression::decode(filter_);
        if (!expr.ok()) {
            return ResultCode::E_INVALID_FILTER;
        } else {
            return lookupFilterCheck(std::move(expr).value().get());
        }
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientQueryImpl::lookupFilterCheck(const Expression *expr) {
    switch (expr->kind()) {
        case nebula::Expression::kLogical : {
            auto* lExpr = dynamic_cast<const LogicalExpression*>(expr);
            if (lExpr->op() == LogicalExpression::Operator::XOR) {
                LOG(ERROR) << "Syntax error : " << lExpr->toString().c_str();
                return ResultCode::E_INVALID_FILTER;
            }
            auto* left = lExpr->left();
            auto ret = lookupFilterCheck(left);
            if (ret != ResultCode::SUCCEEDED) {
                return ret;
            }
            auto* right = lExpr->right();
            ret = lookupFilterCheck(right);
            if (ret != ResultCode::SUCCEEDED) {
                return ret;
            }
            break;
        }
        case nebula::Expression::kRelational : {
            auto* rExpr = dynamic_cast<const RelationalExpression*>(expr);
            auto* left = rExpr->left();
            auto* right = rExpr->right();
            if (left->kind() == nebula::Expression::kAliasProp) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(left);
                if (*aExpr->alias() != targetSchemaName_) {
                    LOG(ERROR) << "Edge or Tag name error : " << targetSchemaName_;
                    return ResultCode::E_INVALID_FILTER;
                }
            } else if (right->kind() == nebula::Expression::kAliasProp) {
                auto* aExpr = dynamic_cast<const AliasPropertyExpression*>(right);
                if (*aExpr->alias() != targetSchemaName_) {
                    LOG(ERROR) << "Edge or Tag name error : " << targetSchemaName_;
                    return ResultCode::E_INVALID_FILTER;
                }
            } else {
                LOG(ERROR) << "Unsupported expression ：" << rExpr->toString().c_str();
                return ResultCode::E_INVALID_FILTER;
            }
            break;
        }
        case nebula::Expression::kFunctionCall : {
            auto* fExpr = dynamic_cast<const FunctionCallExpression*>(expr);
            auto* name = fExpr->name();
            if (*name == "udf_is_in") {
                LOG(ERROR) << "Unsupported function ：" << name->c_str();
                return ResultCode::E_INVALID_FILTER;
            }
            break;
        }
        default : {
            LOG(ERROR) << "Syntax error ：" << expr->toString().c_str();
            return ResultCode::E_INVALID_FILTER;
        }
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientQueryImpl::lookup(const LookupContext& context) {
    clearQueryEnv();
    auto ret = verifyLookupContext(context);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }
    auto future = storageClient_->lookUpIndex(spaceId_,
                                indexId_,
                                filter_,
                                context.returnCols_,
                                context.isEdge_);

    auto result = std::move(future).get();
    if (!result.succeeded()) {
        LOG(ERROR) << "Failed to lookup index of " << indexId_;
        for (const auto& partEntry : result.failedParts()) {
            LOG(ERROR) << "failing part : " << partEntry.first;
            return toResultCode(partEntry.second);
        }
    }
    auto all = result.responses();
    std::shared_ptr<ResultSchemaProvider> schema = nullptr;
    for (auto resp : all) {
        // Have checked the failedParts as above. Is this checking redundant?
        if (!resp.result.failed_codes.empty()) {
            LOG(ERROR) << "storage failed. failing code : "
                       << static_cast<int32_t>(resp.result.failed_codes[0].get_code());
            return toResultCode(resp.result.failed_codes[0].get_code());
        }
        auto hasRows = context.isEdge_  ? resp.__isset.edges : resp.__isset.vertices;
        if (!hasRows) {
            continue;
        }
        if (schema == nullptr && resp.__isset.schema && resp.get_schema() != nullptr) {
            schema = std::make_shared<ResultSchemaProvider>(*(resp.get_schema()));
        }
        if (context.isEdge_) {
            auto rows = resp.get_edges();
            for (const auto& edge : *rows) {
                writeEdgeIndexResult(edge, schema, resultSet_);
            }
        } else {
            auto rows = *resp.get_vertices();
            for (const auto& v : rows) {
                writeTagIndexResult(v, schema, resultSet_);
            }
        }
    }
    return ret;
}

ResultCode NebulaStorageClientQueryImpl::verifyNeighborsContext(const NeighborsContext& context) {
    auto ret = verifyEdgeTypes(context.edges_);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }

    ret = verifyReturns(context.returns_);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }

    ret = verifyNeighborsFilter(context.filter_);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }
    return ret;
}

ResultCode NebulaStorageClientQueryImpl::verifyEdgeTypes(const std::vector<std::string>& edges) {
    for (const auto& edge : edges) {
        auto id = getSchemaIdByName(true, edge);
        if (id == -1) {
            LOG(ERROR) << "Edge not found : " << edge;
            return ResultCode::E_EDGE_NOT_FOUND;
        }
        edgeTypes_.emplace_back(id);
        edges_[edge] = id;
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientQueryImpl::verifyReturns(const std::vector<PropDef>& props) {
    for (const auto& prop : props) {
        if ((prop.owner_ != PropOwner::TAG && prop.owner_ != PropOwner::EDGE) ||
            prop.propName_.empty() || prop.schemaName_.empty()) {
            LOG(ERROR) << "Incomplete parameters";
            return (prop.owner_ == PropOwner::EDGE)
                   ? ResultCode::E_EDGE_PROP_NOT_FOUND
                   : ResultCode::E_TAG_PROP_NOT_FOUND;
        }
        storage::cpp2::PropDef pd;
        pd.owner = toPropOwner(prop.owner_);
        pd.name = prop.propName_;
        if (prop.owner_ == PropOwner::EDGE) {
            auto it = edges_.find(prop.schemaName_);
            if (it == edges_.end()) {
                LOG(ERROR) << "edge not found : " << prop.schemaName_;
                return ResultCode::E_EDGE_NOT_FOUND;
            }
            pd.id.set_edge_type(it->second);
        } else {
            auto tagId = getSchemaIdByName(false, prop.schemaName_);
            if (tagId == -1) {
                LOG(ERROR) << "tag not found : " << prop.schemaName_;
                return ResultCode::E_TAG_NOT_FOUND;
            }
            pd.id.set_tag_id(tagId);
            tags_[prop.schemaName_] = tagId;
        }

        props_.emplace_back(std::move(pd));
        auto colDef = ColumnDef(prop.owner_, prop.schemaName_, prop.propName_);
        resultSet_.returnCols.emplace_back(std::move(colDef));
        DataType type = DataType::UNKNOWN;
        if (prop.propName_ == "_src" || prop.propName_ == "_dst") {
            type = DataType::VID;
        } else if (prop.propName_ == "_rank" || prop.propName_ == "_type") {
            type = DataType::INT;
        } else {
            type = getColumnTypeBySchema(prop.owner_ == PropOwner::EDGE,
                                         prop.schemaName_,
                                         prop.propName_);
        }
        if (type == DataType::UNKNOWN) {
            LOG(ERROR) << "prop type error : " << prop.schemaName_ << "." << prop.propName_;
            return ResultCode::E_IMPROPER_DATA_TYPE;
        }
        resultSet_.columnsType.emplace_back(type);
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientQueryImpl::verifyNeighborsFilter(const std::string& filter) {
    if (filter.empty()) {
        return ResultCode::SUCCEEDED;
    }
    auto ret = ExpressionsParser::encodeExpression(filter, filter_);
    if (!ret) {
        return ResultCode::E_INVALID_FILTER;
    }
    return ResultCode::SUCCEEDED;
}

ResultCode NebulaStorageClientQueryImpl::getNeighbors(const NeighborsContext& context) {
    clearQueryEnv();
    auto ret = verifyNeighborsContext(context);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }
    auto future  = storageClient_->getNeighbors(spaceId_,
                                                context.vertices_,
                                                edgeTypes_,
                                                filter_,
                                                std::move(props_));
    auto result = std::move(future).get();
    if (!result.succeeded()) {
        LOG(ERROR) << "Failed to getNeighbors";
        for (const auto& partEntry : result.failedParts()) {
            LOG(ERROR) << "failing part : " << partEntry.first;
            return toResultCode(partEntry.second);
        }
    }
    auto all = result.responses();
    for (const auto& resp : all) {
        writeQueryResult(resp, resultSet_);
    }
    return ret;
}

ResultCode NebulaStorageClientQueryImpl::verifyVertexPropsContext(
    const VertexPropsContext& context) {
    auto colDef = ColumnDef(nebula::PropOwner::TAG, "", "VertexID");
    resultSet_.returnCols.emplace_back(std::move(colDef));
    resultSet_.columnsType.emplace_back(DataType::VID);
    return verifyReturns(context.returns_);
}

ResultCode NebulaStorageClientQueryImpl::getVertexProps(const VertexPropsContext& context) {
    clearQueryEnv();
    auto ret = verifyVertexPropsContext(context);
    if (ret != ResultCode::SUCCEEDED) {
        return ret;
    }

    auto future  = storageClient_->getVertexProps(spaceId_, context.ids_, std::move(props_));
    auto result = std::move(future).get();
    if (!result.succeeded()) {
        LOG(ERROR) << "Failed to getNeighbors";
        for (const auto& partEntry : result.failedParts()) {
            LOG(ERROR) << "failing part : " << partEntry.first;
            return toResultCode(partEntry.second);
        }
    }
    auto all = result.responses();
    for (const auto& resp : all) {
        writeVertexPropResult(resp, resultSet_);
    }
    return ret;
}

ResultSet NebulaStorageClientQueryImpl::resultSet() {
    return std::move(resultSet_);
}

void NebulaStorageClientQueryImpl::clearQueryEnv() {
    if (!resultSet_.returnCols.empty()) {
        resultSet_.returnCols.clear();
    }

    resultSet_.rows.clear();
    resultSet_.columnsType.clear();
    props_.clear();
    edgeTypes_.clear();
    edges_.clear();
    tags_.clear();
    indexId_ = -1;
    targetSchemaName_ = "";
    filter_ = "";
}

}   // namespace nebula
