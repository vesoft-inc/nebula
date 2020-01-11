/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */


#ifndef STORAGE_INDEXOPTIMIZER_H
#define STORAGE_INDEXOPTIMIZER_H

#include "base/Base.h"
#include "filter/Expressions.h"

namespace nebula {
namespace storage {
class IndexOptimizer {
public:
    virtual ~IndexOptimizer() = default;

protected:
    IndexOptimizer() {}

    cpp2::ErrorCode optimizeFilter(const std::string& filter);

private:
    cpp2::ErrorCode prepareExpr(const std::string& filter);

    cpp2::ErrorCode optimizeExpr(Expression* exp);

    cpp2::ErrorCode checkExp(const Expression* exp);

protected:
    std::unique_ptr<ExpressionContext> expCtx_{nullptr};
    std::unique_ptr<Expression>        exp_{nullptr};
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_INDEXOPTIMIZER_H
