/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_RULE_UNIONALLINDEXSCANBASERULE_H_
#define OPTIMIZER_RULE_UNIONALLINDEXSCANBASERULE_H_

#include "optimizer/OptRule.h"

namespace nebula {
namespace opt {

class UnionAllIndexScanBaseRule : public OptRule {
public:
    bool match(OptContext *ctx, const MatchedResult &matched) const override;
    StatusOr<TransformResult> transform(OptContext *ctx,
                                        const MatchedResult &matched) const override;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_RULE_UNIONALLINDEXSCANBASERULE_H_
