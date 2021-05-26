/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef COMMON_BASE_BASE_H_
#define COMMON_BASE_BASE_H_

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <thread>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <regex>
#include <chrono>
#include <limits>

#include <functional>
#include <string>
#include <memory>
#include <sstream>
#include <iostream>
#include <fstream>

#include <vector>
#include <map>
#include <set>
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <queue>
#include <deque>
#include <tuple>

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <cassert>
#include <cmath>

#include <gflags/gflags.h>

#include <boost/variant.hpp>
#include <boost/any.hpp>

#include <folly/init/Init.h>
#include <folly/String.h>
#include <folly/Range.h>
#include <folly/Hash.h>
#include <folly/Random.h>
#include <folly/Conv.h>
#include <folly/ThreadLocal.h>
#include <folly/Varint.h>
#include <folly/dynamic.h>
#include <folly/json.h>
#include <folly/RWSpinLock.h>

#include "base/Logging.h"
#include "thread/NamedThread.h"
// #include "base/StringUnorderedMap.h"

#define MUST_USE_RESULT                 __attribute__((warn_unused_result))
#define DONT_OPTIMIZE                   __attribute__((optimize("O0")))

#define ALWAYS_INLINE                   __attribute__((always_inline))
#define ALWAYS_NO_INLINE                __attribute__((noinline))

#define BEGIN_NO_OPTIMIZATION           _Pragma("GCC push_options") \
                                        _Pragma("GCC optimize(\"O0\")")
#define END_NO_OPTIMIZATION             _Pragma("GCC pop_options")

#define NEBULA_STRINGIFY(STR)           NEBULA_STRINGIFY_X(STR)
#define NEBULA_STRINGIFY_X(STR)         #STR

#ifndef UNUSED
#define UNUSED(x) (void)(x)
#endif  // UNUSED

#ifndef COMPILER_BARRIER
#define COMPILER_BARRIER()              asm volatile ("":::"memory")
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

#include "base/ThriftTypes.h"


namespace nebula {

// Types using in a graph
// Partition ID is defined as PartitionID in Raftex

// Host address type and utility functions
using HostAddr = std::pair<IPv4, Port>;

std::ostream& operator<<(std::ostream &, const HostAddr&);

template<typename Key, typename T>
using UnorderedMap = typename std::conditional<
    std::is_same<Key, std::string>::value,
//    StringUnorderedMap<T>,
    std::unordered_map<std::string, T>,
    std::unordered_map<Key, T>
>::type;

struct PartMeta {
    GraphSpaceID           spaceId_;
    PartitionID            partId_;
    std::vector<HostAddr>  peers_;

    bool operator==(const PartMeta& that) const {
        return this->spaceId_ == that.spaceId_
                    && this->partId_ == that.partId_
                    && this->peers_ == that.peers_;
    }

    bool operator!=(const PartMeta& that) const {
        return !(*this == that);
    }
};

using PartsMap  = std::unordered_map<GraphSpaceID, std::unordered_map<PartitionID, PartMeta>>;

using VariantType = boost::variant<int64_t, double, bool, std::string>;
constexpr const char* VARIANT_TYPE_NAME[] = {"int", "double", "bool", "string"};

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
constexpr char _ID[]    = "_id";
constexpr char _SRC[]   = "_src";
constexpr char _TYPE[]  = "_type";
constexpr char _RANK[]  = "_rank";
constexpr char _DST[]   = "_dst";

#define ID_HASH(id, numShards) \
    ((static_cast<uint64_t>(id)) % numShards + 1)

// Useful type traits

// Tell if `T' is copy-constructible
template <typename T>
static constexpr auto is_copy_constructible_v = std::is_copy_constructible<T>::value;

// Tell if `T' is move-constructible
template <typename T>
static constexpr auto is_move_constructible_v = std::is_move_constructible<T>::value;

// Tell if `T' is copy or move constructible
template <typename T>
static constexpr auto is_copy_or_move_constructible_v = is_copy_constructible_v<T> ||
                                                        is_move_constructible_v<T>;

// Tell if `T' is constructible from `Args'
template <typename T, typename...Args>
static constexpr auto is_constructible_v = std::is_constructible<T, Args...>::value;

// Tell if `U' could be convertible to `T'
template <typename U, typename T>
static constexpr auto is_convertible_v = std::is_constructible<U, T>::value;

std::string versionString();

}  // namespace nebula

namespace std {

template <>
struct hash<nebula::VariantType> {
    std::size_t operator()(const nebula::VariantType& v) const {
        return boost::hash<nebula::VariantType>()(v);
    }
};

}

#endif  // COMMON_BASE_BASE_H_
