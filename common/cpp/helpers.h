/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */
#ifndef COMMON_CPP_HELPERS_H_
#define COMMON_CPP_HELPERS_H_

namespace vesoft {
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
}   // namespace vesoft
#endif  // COMMON_CPP_HELPERS_H_
