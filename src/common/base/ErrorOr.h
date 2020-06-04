/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_ERROROR_H_
#define COMMON_BASE_ERROROR_H_

#include "common/base/Base.h"
#include "common/base/EitherOr.h"

namespace nebula {

/**
 * ErrorOr<> is a convenient type to allow a method to return either an error
 * code or a user defined result
 *
 * vs. StatusOr<>: ErrorOr<> is an alias of EitherOr with a few accessory methods.
 * The left type of the ErrorOr has to be an integral type or enum, while the left
 * type of StatusOr<> is a Status, which is a more complicate object. This is pretty
 * much the only difference between ErrorOr<> and StatusOr<>
 *
 * So in the scenario that the caller only cares about the error code (or result code),
 * ErrorOr<> would be more suitable. If the caller also needs an error message along
 * with the error code from the callee, then StatusOr<> should be used
 */
template<
    typename ErrorCode, typename ResultType,
    typename = std::enable_if_t<
        std::is_integral<ErrorCode>::value || std::is_enum<ErrorCode>::value
    >
>
using ErrorOr = EitherOr<ErrorCode, ResultType>;


/***********************************************
 *
 * Accessary methods
 *
 **********************************************/
// We treat void ErrorOr objects as succeeded
template<typename E, typename R>
bool ok(const ErrorOr<E, R>& err) {
    return !err.isLeftType();
}

template<typename E, typename R>
E error(const ErrorOr<E, R>& err) {
    return err.left();
}

template<typename E, typename R>
bool hasValue(const ErrorOr<E, R>& err) {
    return err.isRightType();
}

template<typename E, typename R>
R& value(ErrorOr<E, R>& err) {
    return err.right();
}

template<typename E, typename R>
const R& value(const ErrorOr<E, R>& err) {
    return err.right();
}

template<typename E, typename R>
R value(ErrorOr<E, R>&& err) {
    return std::move(err).right();
}

}  // namespace nebula
#endif  // COMMON_BASE_ERROROR_H_
