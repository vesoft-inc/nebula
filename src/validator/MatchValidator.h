/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef VALIDATOR_MATCHVALIDATOR_H_
#define VALIDATOR_MATCHVALIDATOR_H_

#include "common/base/Base.h"
#include "validator/TraversalValidator.h"
#include "util/AnonVarGenerator.h"
#include "planner/Query.h"

namespace nebula {

class MatchStepRange;

namespace graph {

struct MatchAstContext;

class MatchValidator final : public TraversalValidator {
public:
    using Direction = MatchEdge::Direction;
    struct NodeInfo {
        TagID                                   tid{0};
        bool                                    anonymous{false};
        const std::string                      *label{nullptr};
        const std::string                      *alias{nullptr};
        const MapExpression                    *props{nullptr};
        Expression                             *filter{nullptr};
    };

    struct EdgeInfo {
        bool                                    anonymous{false};
        MatchStepRange                         *range{nullptr};
        std::vector<EdgeType>                   edgeTypes;
        MatchEdge::Direction                    direction{MatchEdge::Direction::OUT_EDGE};
        std::vector<std::string>                types;
        const std::string                      *alias{nullptr};
        const MapExpression                    *props{nullptr};
        Expression                             *filter{nullptr};
    };

    enum AliasType {
        kNode, kEdge, kPath
    };

    struct ScanInfo {
        Expression                             *filter{nullptr};
        int32_t                                 schemaId{0};
    };

    enum class QueryEntry {
        kId,  // query start by id
        kIndex  // query start by index scan
    };

    MatchValidator(Sentence *sentence, QueryContext *context);

private:
    Status validateImpl() override;

    AstContext* getAstContext() override;

    Status validatePath(const MatchPath *path);

    Status validateFilter(const Expression *filter);

    Status validateReturn(MatchReturn *ret);

    Status validateAliases(const std::vector<const Expression*> &exprs) const;

    Status validateStepRange(const MatchStepRange *range) const;

    StatusOr<Expression*> makeSubFilter(const std::string &alias,
                                        const MapExpression *map) const;

    template <typename T>
    T* saveObject(T *obj) const {
        return qctx_->objPool()->add(obj);
    }

    Status buildNodeInfo(const MatchPath *path);

    Status buildEdgeInfo(const MatchPath *path);

    Status buildPathExpr(const MatchPath *path);

private:
    std::unique_ptr<MatchAstContext>            matchCtx_;
};

struct MatchAstContext final : AstContext {
    std::vector<MatchValidator::NodeInfo>                       nodeInfos;
    std::vector<MatchValidator::EdgeInfo>                       edgeInfos;
    std::unordered_map<std::string, MatchValidator::AliasType>  aliases;
    std::unique_ptr<PathBuildExpression>                        pathBuild;
    std::unique_ptr<Expression>                                 filter;
    const YieldColumns                                         *yieldColumns;
    MatchValidator::ScanInfo                                    scanInfo;
    const Expression                                           *ids;
    std::vector<std::pair<size_t, OrderFactor::OrderType>>      indexedOrderFactors;
    int64_t                                                     skip;
    int64_t                                                     limit;
};
}   // namespace graph
}   // namespace nebula

#endif  // VALIDATOR_MATCHVALIDATOR_H_
