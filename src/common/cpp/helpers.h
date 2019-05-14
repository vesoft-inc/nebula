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
public:
    NonCopyable() {}
    NonCopyable(const NonCopyable&) = delete;
    NonCopyable& operator=(const NonCopyable&) = delete;
};

class NonMovable {
public:
    NonMovable() {}
    NonMovable(NonMovable&&) = delete;
    NonMovable& operator=(NonMovable&&) = delete;
};

}   // namespace cpp
}   // namespace nebula
#endif  // COMMON_CPP_HELPERS_H_
