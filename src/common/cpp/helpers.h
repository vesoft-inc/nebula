/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_CPP_HELPERS_H_
#define COMMON_CPP_HELPERS_H_

namespace nebula {
namespace cpp {

class NonCopyable {
protected:
    NonCopyable() {}
    NonCopyable(const NonCopyable&) = delete;
    NonCopyable& operator=(const NonCopyable&) = delete;
};

static_assert(sizeof(NonCopyable) == 1UL, "Unexpected sizeof(NonCopyable)!");

class NonMovable {
protected:
    NonMovable() {}
    NonMovable(NonMovable&&) = delete;
    NonMovable& operator=(NonMovable&&) = delete;
};

static_assert(sizeof(NonMovable) == 1UL, "Unexpected sizeof(NonMovable)!");

}   // namespace cpp
}   // namespace nebula
#endif  // COMMON_CPP_HELPERS_H_
