/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef COMMON_CPP_HELPERS_H_
#define COMMON_CPP_HELPERS_H_

namespace nebula {
namespace cpp {

class NonMovable {
 public:
  NonMovable() = default;
  ~NonMovable() = default;

  NonMovable(const NonMovable&) = default;
  NonMovable& operator=(const NonMovable&) = default;

  NonMovable(NonMovable&&) = delete;
  NonMovable& operator=(NonMovable&&) = delete;
};

}  // namespace cpp
}  // namespace nebula
#endif  // COMMON_CPP_HELPERS_H_
