/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef OPTIMIZER_RULE_UNIONALLTAGINDEXSCANRULE_H_
#define OPTIMIZER_RULE_UNIONALLTAGINDEXSCANRULE_H_

#include "optimizer/rule/UnionAllIndexScanBaseRule.h"

namespace nebula {
namespace opt {

class UnionAllTagIndexScanRule final : public UnionAllIndexScanBaseRule {
public:
    const Pattern &pattern() const override;
    std::string toString() const override;

private:
    UnionAllTagIndexScanRule();

    static std::unique_ptr<OptRule> kInstance;
};

}   // namespace opt
}   // namespace nebula

#endif   // OPTIMIZER_RULE_UNIONALLTAGINDEXSCANRULE_H_
