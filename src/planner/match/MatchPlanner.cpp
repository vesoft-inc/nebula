/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "planner/match/MatchPlanner.h"

#include "context/ast/CypherAstContext.h"
#include "planner/match/MatchClausePlanner.h"
#include "planner/match/ReturnClausePlanner.h"
#include "planner/match/SegmentsConnector.h"
#include "planner/match/UnwindClausePlanner.h"
#include "planner/match/WithClausePlanner.h"

namespace nebula {
namespace graph {
bool MatchPlanner::match(AstContext* astCtx) {
    return astCtx->sentence->kind() == Sentence::Kind::kMatch;
}

StatusOr<SubPlan> MatchPlanner::transform(AstContext* astCtx) {
    if (astCtx->sentence->kind() != Sentence::Kind::kMatch) {
        return Status::Error("Only MATCH is accepted for match planner.");
    }
    auto* matchCtx = static_cast<MatchAstContext*>(astCtx);

    std::vector<SubPlan> subplans;
    for (auto& clauseCtx : matchCtx->clauses) {
        switch (clauseCtx->kind) {
            case CypherClauseKind::kMatch: {
                auto subplan = std::make_unique<MatchClausePlanner>()->transform(clauseCtx.get());
                NG_RETURN_IF_ERROR(subplan);
                subplans.emplace_back(std::move(subplan).value());
                break;
            }
            case CypherClauseKind::kUnwind: {
                auto subplan = std::make_unique<UnwindClausePlanner>()->transform(clauseCtx.get());
                NG_RETURN_IF_ERROR(subplan);
                auto& unwind = subplan.value().root;
                std::vector<std::string> inputCols;
                if (!subplans.empty()) {
                    auto input = subplans.back().root;
                    auto cols = input->colNames();
                    for (auto col : cols) {
                        inputCols.emplace_back(col);
                    }
                }
                inputCols.emplace_back(unwind->colNames().front());
                unwind->setColNames(inputCols);
                subplans.emplace_back(std::move(subplan).value());
                break;
            }
            case CypherClauseKind::kWith: {
                auto subplan = std::make_unique<WithClausePlanner>()->transform(clauseCtx.get());
                NG_RETURN_IF_ERROR(subplan);
                subplans.emplace_back(std::move(subplan).value());
                break;
            }
            case CypherClauseKind::kReturn: {
                auto subplan = std::make_unique<ReturnClausePlanner>()->transform(clauseCtx.get());
                NG_RETURN_IF_ERROR(subplan);
                subplans.emplace_back(std::move(subplan).value());
                break;
            }
            default: { return Status::Error("Unsupported clause."); }
        }
    }

    auto finalPlan = connectSegments(astCtx, subplans, matchCtx->clauses);
    NG_RETURN_IF_ERROR(finalPlan);
    return std::move(finalPlan).value();
}

StatusOr<SubPlan> MatchPlanner::connectSegments(
    AstContext* astCtx,
    std::vector<SubPlan>& subplans,
    std::vector<std::unique_ptr<CypherClauseContextBase>>& clauses) {
    DCHECK(!subplans.empty());
    subplans.front().tail->setInputVar(astCtx->qctx->vctx()->anonVarGen()->getVar());
    if (subplans.size() == 1) {
        return subplans.front();
    }

    SubPlan finalPlan = subplans.front();
    for (size_t i = 0; i < subplans.size() - 1; ++i) {
        auto interimPlan = SegmentsConnector::connectSegments(
            clauses[i + 1].get(), clauses[i].get(), subplans[i + 1], finalPlan);
        NG_RETURN_IF_ERROR(interimPlan);
        finalPlan = std::move(interimPlan).value();
    }

    return finalPlan;
}
}  // namespace graph
}  // namespace nebula
