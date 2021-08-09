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

namespace nebula {
namespace graph {

Status GetSubgraphValidator::validateImpl() {
    auto* gsSentence = static_cast<GetSubgraphSentence*>(sentence_);
    subgraphCtx_ = getContext<SubgraphContext>();
    subgraphCtx_->withProp = gsSentence->withProp();

    NG_RETURN_IF_ERROR(validateStep(gsSentence->step(), subgraphCtx_->steps));
    NG_RETURN_IF_ERROR(validateStarts(gsSentence->from(), subgraphCtx_->from));
    NG_RETURN_IF_ERROR(validateInBound(gsSentence->in()));
    NG_RETURN_IF_ERROR(validateOutBound(gsSentence->out()));
    NG_RETURN_IF_ERROR(validateBothInOutBound(gsSentence->both()));
    // set output col & type
    if (subgraphCtx_->steps.steps() == 0) {
        outputs_.emplace_back(kVertices, Value::Type::VERTEX);
    } else {
        outputs_.emplace_back(kVertices, Value::Type::VERTEX);
        outputs_.emplace_back(kEdges, Value::Type::EDGE);
    }
    return Status::OK();
}

Status GetSubgraphValidator::validateInBound(InBoundClause* in) {
    auto& edgeTypes = subgraphCtx_->edgeTypes;
    if (in != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = in->edges();
        edgeTypes.reserve(edgeTypes.size() + edges.size());
        for (auto* e : edges) {
            if (e->alias() != nullptr) {
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            auto v = -et.value();
            edgeTypes.emplace(v);
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateOutBound(OutBoundClause* out) {
    auto& edgeTypes = subgraphCtx_->edgeTypes;
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes.reserve(edgeTypes.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            edgeTypes.emplace(et.value());
        }
    }

    return Status::OK();
}

Status GetSubgraphValidator::validateBothInOutBound(BothInOutClause* out) {
    auto& edgeTypes = subgraphCtx_->edgeTypes;
    if (out != nullptr) {
        auto space = vctx_->whichSpace();
        auto edges = out->edges();
        edgeTypes.reserve(edgeTypes.size() + edges.size());
        for (auto* e : out->edges()) {
            if (e->alias() != nullptr) {
                return Status::SemanticError("Get Subgraph not support rename edge name.");
            }

            auto et = qctx_->schemaMng()->toEdgeType(space.id, *e->edge());
            NG_RETURN_IF_ERROR(et);

            auto v = et.value();
            edgeTypes.emplace(v);
            v = -v;
            edgeTypes.emplace(v);
        }
    }

    return Status::OK();
}

}   // namespace graph
}   // namespace nebula
