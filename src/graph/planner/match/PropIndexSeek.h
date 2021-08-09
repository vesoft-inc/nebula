/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_PROPINDEXSEEK_H_
#define PLANNER_MATCH_PROPINDEXSEEK_H_

#include "planner/match/StartVidFinder.h"

namespace nebula {
namespace graph {
/*
 * The PropIndexSeek was designed to find if could get starting vids by tag props or edge props index.
 */
class PropIndexSeek final : public StartVidFinder {
public:
    static std::unique_ptr<PropIndexSeek> make() {
        return std::unique_ptr<PropIndexSeek>(new PropIndexSeek());
    }

    bool matchNode(NodeContext* nodeCtx) override;

    bool matchEdge(EdgeContext* edgeCtx) override;

    StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

    StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

private:
    PropIndexSeek() = default;
};
}  // namespace graph
}  // namespace nebula
#endif  // PLANNER_MATCH_PropIndexSeek_H_
