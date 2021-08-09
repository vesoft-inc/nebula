/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef PLANNER_MATCH_LABELINDEXSEEK_H_
#define PLANNER_MATCH_LABELINDEXSEEK_H_

#include "planner/match/StartVidFinder.h"

namespace nebula {
namespace graph {

/*
 * The LabelIndexSeek was designed to find if could get the starting vids by tag index.
 */
class LabelIndexSeek final : public StartVidFinder {
public:
    static std::unique_ptr<LabelIndexSeek> make() {
        return std::unique_ptr<LabelIndexSeek>(new LabelIndexSeek());
    }

private:
    LabelIndexSeek() = default;

    bool matchNode(NodeContext* nodeCtx) override;

    bool matchEdge(EdgeContext* edgeCtx) override;

    StatusOr<SubPlan> transformNode(NodeContext* nodeCtx) override;

    StatusOr<SubPlan> transformEdge(EdgeContext* edgeCtx) override;

    static StatusOr<std::vector<IndexID>> pickTagIndex(const NodeContext* nodeCtx);

    static StatusOr<std::vector<IndexID>> pickEdgeIndex(const EdgeContext* edgeCtx);

    static std::shared_ptr<meta::cpp2::IndexItem> selectIndex(
        const std::shared_ptr<meta::cpp2::IndexItem> candidate,
        const std::shared_ptr<meta::cpp2::IndexItem> income) {
        // less fields is better
        // TODO(shylock) rank for field itself(e.g. INT is better than STRING)
        if (candidate->get_fields().size() > income->get_fields().size()) {
            return income;
        }
        return candidate;
    }
};

}   // namespace graph
}   // namespace nebula
#endif   // PLANNER_MATCH_LABELINDEXSEEK_H_
