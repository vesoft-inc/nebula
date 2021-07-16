/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_RULE_OPTIMIZETAGINDEXSCANBYFILTERRULE_H_
#define OPTIMIZER_RULE_OPTIMIZETAGINDEXSCANBYFILTERRULE_H_

#include <memory>

#include "optimizer/OptRule.h"

namespace nebula {
namespace opt {

// At present, we do NOT split filter conditions into two parts, one part is pushed down storage
// layer and another will be leaved into filter. Because this is enough for Lookup queries. We
// will enhance this rule for general usage later, such as MATCH queries.
class OptimizeTagIndexScanByFilterRule final : public OptRule {
public:
    const Pattern &pattern() const override;
    bool match(OptContext *ctx, const MatchedResult &matched) const override;
    StatusOr<TransformResult> transform(OptContext *ctx,
                                        const MatchedResult &matched) const override;

    std::string toString() const override;

private:
    OptimizeTagIndexScanByFilterRule();

    static std::unique_ptr<OptRule> kInstance;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_RULE_OPTIMIZETAGINDEXSCANBYFILTERRULE_H_
