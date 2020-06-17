/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "common/expression/VariableExpression.h"
#include "common/datatypes/List.h"

namespace nebula {
const Value& VariableExpression::eval(ExpressionContext& ctx) {
    return ctx.getVar(*var_);
}

const Value& VersionedVariableExpression::eval(ExpressionContext& ctx) {
    if (version_ != nullptr) {
        auto version = version_->eval(ctx);
        if (UNLIKELY(!version.isInt())) {
            return Value::kNullBadType;
        }
        auto ver = version.getInt();
        return ctx.getVersionedVar(*var_, ver);
    } else {
        return ctx.getVar(*var_);
    }
}
}  // namespace nebula
