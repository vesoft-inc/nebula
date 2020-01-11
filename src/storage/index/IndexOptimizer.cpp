/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/index/IndexOptimizer.h"

namespace nebula {
namespace storage {

cpp2::ErrorCode IndexOptimizer::optimizeFilter(const std::string& filter) {
    auto ret = prepareExpr(filter);
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }

    ret = optimizeExpr(exp_.get());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }

    ret = checkExp(exp_.get());
    if (ret != cpp2::ErrorCode::SUCCEEDED) {
        return ret;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode IndexOptimizer::prepareExpr(const std::string& filter) {
    if (filter.empty()) {
        return cpp2::ErrorCode::SUCCEEDED;
    }
    auto expRet = Expression::decode(filter);
    if (!expRet.ok()) {
        VLOG(1) << "Can't decode the filter " << filter;
        return cpp2::ErrorCode::E_INVALID_FILTER;
    }
    exp_ = std::move(expRet).value();
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode IndexOptimizer::optimizeExpr(Expression* exp) {
    /**
     * TODO sky : Filter expression not optimized yet.
     *            In the future, the sub-item of the
     *            expression will be optimized.
     **/

    if (expCtx_ == nullptr) {
        expCtx_ = std::make_unique<ExpressionContext>();
    }
    exp->setContext(this->expCtx_.get());
    auto status = exp->prepare();
    if (!status.ok()) {
        return cpp2::ErrorCode::E_INVALID_FILTER;
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode IndexOptimizer::checkExp(const Expression* exp) {
    /**
     * TODO sky : Check the validity of the optimized expression
     */
    UNUSED(exp);
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
