/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef COMMON_BASE_BASE_H_
#define COMMON_BASE_BASE_H_

#include <fcntl.h>
#include <folly/Hash.h>
#include <folly/Range.h>
#include <folly/String.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <any>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#if defined(__clang__) && defined(__aarch64__)
#if FOLLY_HAVE_EXTRANDOM_SFMT19937
#undef FOLLY_HAVE_EXTRANDOM_SFMT19937
#endif
#endif  // __clang__ && __aarch64__
#include <folly/Conv.h>
#include <folly/RWSpinLock.h>
#include <folly/Random.h>
#include <folly/ThreadLocal.h>
#include <folly/Unicode.h>
#include <folly/Varint.h>
#include <folly/dynamic.h>
#include <folly/futures/Future.h>
#include <folly/json.h>

#include "common/base/Logging.h"

#define NG_MUST_USE_RESULT __attribute__((warn_unused_result))
#define DONT_OPTIMIZE __attribute__((optimize("O0")))

#define ALWAYS_INLINE __attribute__((always_inline))
#define ALWAYS_NO_INLINE __attribute__((noinline))

#define BEGIN_NO_OPTIMIZATION _Pragma("GCC push_options") _Pragma("GCC optimize(\"O0\")")
#define END_NO_OPTIMIZATION _Pragma("GCC pop_options")

#define NEBULA_STRINGIFY(STR) NEBULA_STRINGIFY_X(STR)
#define NEBULA_STRINGIFY_X(STR) #STR

#ifndef UNUSED
#define UNUSED(x) (void)(x)
#endif  // UNUSED

#ifndef MAYBE_UNUSED
#if (__cplusplus >= 201703L)  // c++17
#include <folly/CppAttributes.h>
#define MAYBE_UNUSED FOLLY_MAYBE_UNUSED
#else
#define MAYBE_UNUSED __attribute__((unused))
#endif
#endif

#ifndef COMPILER_BARRIER
#define COMPILER_BARRIER() asm volatile("" ::: "memory")
#endif  // COMPILER_BARRIER

// Formated logging
#define FLOG_FATAL(...) LOG(FATAL) << folly::stringPrintf(__VA_ARGS__)
#define FLOG_ERROR(...) LOG(ERROR) << folly::stringPrintf(__VA_ARGS__)
#define FLOG_WARN(...) LOG(WARNING) << folly::stringPrintf(__VA_ARGS__)
#define FLOG_INFO(...) LOG(INFO) << folly::stringPrintf(__VA_ARGS__)
#define FVLOG1(...) VLOG(1) << folly::stringPrintf(__VA_ARGS__)
#define FVLOG2(...) VLOG(2) << folly::stringPrintf(__VA_ARGS__)
#define FVLOG3(...) VLOG(3) << folly::stringPrintf(__VA_ARGS__)
#define FVLOG4(...) VLOG(4) << folly::stringPrintf(__VA_ARGS__)

namespace nebula {

using VariantType = std::variant<int64_t, double, bool, std::string>;

#ifndef VAR_INT64
#define VAR_INT64 0
#endif

#ifndef VAR_DOUBLE
#define VAR_DOUBLE 1
#endif

#ifndef VAR_BOOL
#define VAR_BOOL 2
#endif

#ifndef VAR_STR
#define VAR_STR 3
#endif

// reserved property names
constexpr char kId[] = "_id";
constexpr char kVid[] = "_vid";
constexpr char kTag[] = "_tag";
constexpr char kSrc[] = "_src";
constexpr char kType[] = "_type";
constexpr char kRank[] = "_rank";
constexpr char kDst[] = "_dst";

// Useful type traits

// Tell if `T' is copy-constructible
template <typename T>
static constexpr auto is_copy_constructible_v = std::is_copy_constructible<T>::value;

// Tell if `T' is move-constructible
template <typename T>
static constexpr auto is_move_constructible_v = std::is_move_constructible<T>::value;

// Tell if `T' is copy or move constructible
template <typename T>
static constexpr auto is_copy_or_move_constructible_v =
    is_copy_constructible_v<T> || is_move_constructible_v<T>;

// Tell if `T' is constructible from `Args'
template <typename T, typename... Args>
static constexpr auto is_constructible_v = std::is_constructible<T, Args...>::value;

// Tell if `U' could be convertible to `T'
template <typename U, typename T>
static constexpr auto is_convertible_v = std::is_constructible<U, T>::value;

std::string toHexStr(folly::StringPiece str);

}  // namespace nebula

#endif  // COMMON_BASE_BASE_H_
