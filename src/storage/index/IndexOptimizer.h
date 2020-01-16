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

    /**
     * details Conditional expression optimization entry, The process logic as below:
     *          1, Parse filter string to expression;
     *          2, Optimize expression. For example, split expression node,
     *             move expression node, merge expression node, etc.
     *          3, Check the validity of optimized expression.
     * param  filter : From encode string of where clause.
     * return ErrorCode : Filter optimization and index scan operations
     *                     terminated if processing errors occur.
     **/
    cpp2::ErrorCode optimizeFilter(const std::string& filter);

private:
    /**
     * Details Convert the filter string to expression.
     * Param filter : From encode string of where clause.
     * Return ErrorCode
     **/
    cpp2::ErrorCode prepareExpr(const std::string& filter);

    /**
     * Details Optimizing operation of processing expression nodes.
     *         In the future, priority selection of expression nodes
     *         is based on cost information.
     **/
    cpp2::ErrorCode optimizeExpr(Expression* exp);

    cpp2::ErrorCode checkExp(const Expression* exp);

protected:
    std::unique_ptr<ExpressionContext> expCtx_{nullptr};
    std::unique_ptr<Expression>        exp_{nullptr};
};
}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_INDEXOPTIMIZER_H
