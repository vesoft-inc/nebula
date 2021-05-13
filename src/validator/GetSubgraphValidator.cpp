/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "validator/GetSubgraphValidator.h"
#include <memory>

#include "common/expression/UnaryExpression.h"
#include "common/expression/VariableExpression.h"
#include "common/expression/VertexExpression.h"
#include "context/QueryExpressionContext.h"
#include "parser/TraverseSentences.h"
#include "planner/plan/Logic.h"
#include "planner/plan/Query.h"
#include "planner/plan/Algo.h"

namespace nebula {
namespace graph {

Status GetSubgraphValidator::validateImpl() {
    auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);

    NG_RETURN_IF_ERROR(validateStep(gsSentence->step(), steps_));
    NG_RETURN_IF_ERROR(validateStarts(gsSentence->from(), from_));
    NG_RETURN_IF_ERROR(validateInBound(gsSentence->in()));
    NG_RETURN_IF_ERROR(validateOutBound(gsSentence->out()));
    NG_RETURN_IF_ERROR(validateBothInOutBound(gsSentence->both()));

    if (!exprProps_.srcTagProps().empty() || !exprProps_.dstTagProps().empty()) {
        return Status::SemanticError("Only support input and variable in Subgraph sentence.");
    }
    if (!exprProps_.inputProps().empty() && !exprProps_.varProps().empty()) {
        return Status::SemanticError("Not support both input and variable in Subgraph sentence.");
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateInBound(InBoundClause* in) {
    if (in != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = in->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : edges) {
            if (e->alias() != nullptr) {
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            auto v = -et.value();
            edgeTypes_.emplace(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateOutBound(OutBoundClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            edgeTypes_.emplace(et.value());
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateBothInOutBound(BothInOutClause* out) {
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes_.reserve(edgeTypes_.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            auto v = et.value();
            edgeTypes_.emplace(v);
            v = -v;
            edgeTypes_.emplace(v);
        }
    }

    return Status::OK();
}

StatusOr<GetNeighbors::EdgeProps> GetSubgraphValidator::buildEdgeProps() {
    if (edgeTypes_.empty()) {
        auto allEdgePropResult = buildAllEdgeProp();
        NG_RETURN_IF_ERROR(allEdgePropResult);
        return std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
            std::move(allEdgePropResult).value());
    }
    auto edgePropResult = fillEdgeProp(edgeTypes_);
    NG_RETURN_IF_ERROR(edgePropResult);
    return std::make_unique<std::vector<storage::cpp2::EdgeProp>>(
        std::move(edgePropResult).value());
}

StatusOr<std::vector<storage::cpp2::EdgeProp>> GetSubgraphValidator::fillEdgeProp(
    const std::unordered_set<EdgeType>& edges) {
    // list all edge properties
    std::vector<storage::cpp2::EdgeProp> eProps;
    for (const auto edge : edges) {
        auto edgeSchema = qctx()->schemaMng()->getEdgeSchema(space_.id, std::abs(edge));
        if (edgeSchema == nullptr) {
            return Status::SemanticError("Not exist edge `%d' in space `%d'.", edge, space_.id);
        }
        storage::cpp2::EdgeProp eProp;
        eProp.set_type(edge);
        std::vector<std::string> props{kSrc, kType, kRank, kDst};
        for (std::size_t i = 0; i < edgeSchema->getNumFields(); ++i) {
            props.emplace_back(edgeSchema->getFieldName(i));
        }
        eProp.set_props(std::move(props));
        eProps.emplace_back(std::move(eProp));
    }
    return eProps;
}

StatusOr<std::vector<storage::cpp2::EdgeProp>> GetSubgraphValidator::buildAllEdgeProp() {
    // list all edge properties
    std::map<TagID, std::shared_ptr<const meta::SchemaProviderIf>> edgesSchema;
    const auto allEdgesResult = qctx()->schemaMng()->getAllVerEdgeSchema(space_.id);
    NG_RETURN_IF_ERROR(allEdgesResult);
    const auto allEdges = std::move(allEdgesResult).value();
    for (const auto& edge : allEdges) {
        edgesSchema.emplace(edge.first, edge.second.back());
    }
    std::vector<storage::cpp2::EdgeProp> eProps;
    for (const auto& edgeSchema : edgesSchema) {
        storage::cpp2::EdgeProp eProp;
        storage::cpp2::EdgeProp rEProp;
        eProp.set_type(edgeSchema.first);
        rEProp.set_type(-edgeSchema.first);
        std::vector<std::string> props{kSrc, kType, kRank, kDst};
        for (std::size_t i = 0; i < edgeSchema.second->getNumFields(); ++i) {
            props.emplace_back(edgeSchema.second->getFieldName(i));
        }
        eProp.set_props(props);
        rEProp.set_props(std::move(props));
        eProps.emplace_back(std::move(eProp));
        eProps.emplace_back(std::move(rEProp));
    }
    return eProps;
}

Status GetSubgraphValidator::zeroStep(PlanNode* depend, const std::string& inputVar) {
    auto& space = vctx_->whichSpace();
    std::vector<storage::cpp2::Expr> exprs;
    auto vertexPropsResult = buildVertexProp();
    NG_RETURN_IF_ERROR(vertexPropsResult);
    auto* getVertex = GetVertices::make(qctx_,
                                        depend,
                                        space.id,
                                        from_.src,
                                        std::move(vertexPropsResult).value(),
                                        std::move(exprs),
                                        true);
    getVertex->setInputVar(inputVar);

    auto var = vctx_->anonVarGen()->getVar();
    auto* column = new VertexExpression();
    auto* func = new AggregateExpression(new std::string("COLLECT"), column, false);
    qctx_->objPool()->add(func);
    auto* collectVertex =
        Aggregate::make(qctx_,
                        getVertex,
                        {},
                        {func});
    collectVertex->setInputVar(getVertex->outputVar());
    collectVertex->setColNames({"_vertices"});

    root_ = collectVertex;
    tail_ = projectStartVid_ != nullptr ? projectStartVid_ : getVertex;
    return Status::OK();
}

Status GetSubgraphValidator::toPlan() {
    auto& space = vctx_->whichSpace();
    auto* bodyStart = StartNode::make(qctx_);

    std::string startVidsVar;
    PlanNode* loopDep = nullptr;
    if (!from_.vids.empty() && from_.originalSrc == nullptr) {
        buildConstantInput(from_, startVidsVar);
    } else {
        loopDep = buildRuntimeInput(from_, projectStartVid_);
        startVidsVar = loopDep->outputVar();
    }

    if (steps_.steps == 0) {
        return zeroStep(loopDep == nullptr ? bodyStart : loopDep, startVidsVar);
    }

    auto vertexPropsResult = buildVertexProp();
    NG_RETURN_IF_ERROR(vertexPropsResult);
    auto* gn = GetNeighbors::make(qctx_, bodyStart, space.id);
    gn->setSrc(from_.src);
    gn->setVertexProps(std::make_unique<std::vector<storage::cpp2::VertexProp>>(
        std::move(vertexPropsResult).value()));
    auto edgePropsResult = buildEdgeProps();
    NG_RETURN_IF_ERROR(edgePropsResult);
    gn->setEdgeProps(
        std::make_unique<std::vector<storage::cpp2::EdgeProp>>(*edgePropsResult.value()));
    gn->setInputVar(startVidsVar);

    auto oneMoreStepOutput = vctx_->anonVarGen()->getVar();
    auto* subgraph = Subgraph::make(qctx_, gn, oneMoreStepOutput, loopSteps_, steps_.steps + 1);
    subgraph->setOutputVar(startVidsVar);
    subgraph->setColNames({nebula::kVid});

    auto* loopCondition = buildExpandCondition(gn->outputVar(), steps_.steps + 1);
    qctx_->objPool()->add(loopCondition);
    auto* loop = Loop::make(qctx_, loopDep, subgraph, loopCondition);

    std::vector<std::string> collects = {gn->outputVar(), oneMoreStepOutput};
    auto* dc =
        DataCollect::make(qctx_, loop, DataCollect::CollectKind::kSubgraph, std::move(collects));
    dc->setColNames({"_vertices", "_edges"});
    root_ = dc;
    tail_ = projectStartVid_ != nullptr ? projectStartVid_ : loop;
    return Status::OK();
}

StatusOr<std::vector<storage::cpp2::VertexProp>> GetSubgraphValidator::buildVertexProp() {
    // list all tag properties
    std::map<TagID, std::shared_ptr<const meta::SchemaProviderIf>> tagsSchema;
    const auto allTagsResult = qctx()->schemaMng()->getAllLatestVerTagSchema(space_.id);
    NG_RETURN_IF_ERROR(allTagsResult);
    const auto allTags = std::move(allTagsResult).value();
    for (const auto& tag : allTags) {
        tagsSchema.emplace(tag.first, tag.second);
    }
    std::vector<storage::cpp2::VertexProp> vProps;
    for (const auto& tagSchema : tagsSchema) {
        storage::cpp2::VertexProp vProp;
        vProp.set_tag(tagSchema.first);
        std::vector<std::string> props;
        for (std::size_t i = 0; i < tagSchema.second->getNumFields(); ++i) {
            props.emplace_back(tagSchema.second->getFieldName(i));
        }
        vProp.set_props(std::move(props));
        vProps.emplace_back(std::move(vProp));
    }
    return vProps;
}

}   // namespace graph
}   // namespace nebula
