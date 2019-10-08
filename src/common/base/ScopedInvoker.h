/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_SCOPED_INVOKER_H_
#define COMMON_BASE_SCOPED_INVOKER_H_

#include <functional>
#include <tuple>

#include <folly/functional/ApplyTuple.h>
#include "cpp/helpers.h"

namespace nebula {

template <typename... Args>
class ScopedInvoker : public nebula::cpp::NonCopyable, nebula::cpp::NonMovable {
public:
    ScopedInvoker(std::function<void(Args...)> f, Args... args)
        : f_(f), args_(args...) {}

    ~ScopedInvoker() {
        folly::apply(f_, args_);
    }

private:
    std::function<void(Args...)> f_;
    std::tuple<Args...> args_;
};

}  // nebula

#endif  // header guard
