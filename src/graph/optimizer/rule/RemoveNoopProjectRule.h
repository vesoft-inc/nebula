/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_RULE_REMOVENOOPPROJECTRULE_H_
#define OPTIMIZER_RULE_REMOVENOOPPROJECTRULE_H_

#include <memory>

#include "optimizer/OptRule.h"

namespace nebula {
namespace opt {

class RemoveNoopProjectRule final : public OptRule {
public:
    const Pattern &pattern() const override;

    StatusOr<TransformResult> transform(OptContext *ctx,
                                        const MatchedResult &matched) const override;

    bool match(OptContext *ctx, const MatchedResult &matched) const override;

    std::string toString() const override;

private:
    RemoveNoopProjectRule();

    static std::unique_ptr<OptRule> kInstance;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_RULE_REMOVENOOPPROJECTRULE_H_
