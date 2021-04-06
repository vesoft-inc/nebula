/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_RULE_PUSHFILTERDOWNAGGREGATERULE_H_
#define OPTIMIZER_RULE_PUSHFILTERDOWNAGGREGATERULE_H_

#include <memory>
#include "optimizer/OptRule.h"

namespace nebula {
namespace opt {

class PushFilterDownAggregateRule final : public OptRule {
public:
    const Pattern &pattern() const override;

    StatusOr<OptRule::TransformResult> transform(OptContext *qctx,
                                                 const MatchedResult &matched) const override;

    std::string toString() const override;

private:
    PushFilterDownAggregateRule();

    static std::unique_ptr<OptRule> kInstance;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_RULE_PUSHFILTERDOWNAGGREGATERULE_H_
