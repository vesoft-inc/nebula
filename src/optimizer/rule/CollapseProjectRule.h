/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_RULE_COLLAPSEPROJECTRULE_H_
#define OPTIMIZER_RULE_COLLAPSEPROJECTRULE_H_

#include <memory>

#include "optimizer/OptRule.h"

namespace nebula {
namespace opt {

/**
 * Combines two [[Project]] nodes into one
 * Required conditions:
 *  1. Match the pattern
 *  2. Expressions between nodes cannot be referenced more than once
 * Benefits:
 *  1. reduce the copy of memory between nodes
 *  2. reduces expression overhead in some cases(similar to column pruning)
 */

class CollapseProjectRule final : public OptRule {
public:
    const Pattern &pattern() const override;

    StatusOr<TransformResult> transform(OptContext *ctx,
                                        const MatchedResult &matched) const override;

    bool match(OptContext *ctx, const MatchedResult &matched) const override;

    std::string toString() const override;

private:
    CollapseProjectRule();

    static std::unique_ptr<OptRule> kInstance;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_RULE_COLLAPSEPROJECTRULE_H_
