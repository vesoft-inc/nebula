/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef COMMON_BASE_ERROROR_H_
#define COMMON_BASE_ERROROR_H_

#include "base/Base.h"
#include "base/EitherOr.h"

namespace nebula {

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
    return !err.isTypeOne();
}

template<typename E, typename R>
E error(const ErrorOr<E, R>& err) {
    return err.v1();
}

template<typename E, typename R>
bool hasValue(const ErrorOr<E, R>& err) {
    return err.isTypeTwo();
}

template<typename E, typename R>
R& value(ErrorOr<E, R>& err) {
    return err.v2();
}

template<typename E, typename R>
const R& value(const ErrorOr<E, R>& err) {
    return err.v2();
}

template<typename E, typename R>
R value(ErrorOr<E, R>&& err) {
    return std::move(err).v2();
}

}  // namespace nebula
#endif  // COMMON_BASE_ERROROR_H_
