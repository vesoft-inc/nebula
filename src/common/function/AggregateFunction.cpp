/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/function/AggregateFunction.h"

namespace nebula {
std::unordered_map<AggFun::Function,
                   std::function<std::unique_ptr<AggFun>(bool)>> AggFun::aggFunMap_  = {
    { AggFun::Function::kNone,
        [](bool distinct) -> auto {
            UNUSED(distinct);
            return std::make_unique<Group>();} },
    { AggFun::Function::kCount,
        [](bool distinct) -> auto { return std::make_unique<Count>(distinct);} },
    { AggFun::Function::kSum,
        [](bool distinct) -> auto { return std::make_unique<Sum>(distinct);} },
    { AggFun::Function::kAvg,
        [](bool distinct) -> auto { return std::make_unique<Avg>(distinct);} },
    { AggFun::Function::kMax,
        [](bool distinct) -> auto { return std::make_unique<Max>(distinct);} },
    { AggFun::Function::kMin,
        [](bool distinct) -> auto { return std::make_unique<Min>(distinct);} },
    { AggFun::Function::kStdev,
        [](bool distinct) -> auto { return std::make_unique<Stdev>(distinct);} },
    { AggFun::Function::kBitAnd,
        [](bool distinct) -> auto { return std::make_unique<BitAnd>(distinct);} },
    { AggFun::Function::kBitOr,
        [](bool distinct) -> auto { return std::make_unique<BitOr>(distinct);} },
    { AggFun::Function::kBitXor,
        [](bool distinct) -> auto { return std::make_unique<BitXor>(distinct);} },
    { AggFun::Function::kCollect,
        [](bool distinct) -> auto { return std::make_unique<Collect>(distinct);} },
};

}  // namespace nebula
