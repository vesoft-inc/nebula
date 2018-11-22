/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
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

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <cassert>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <boost/variant.hpp>

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

#include "thread/NamedThread.h"
//#include "base/StringUnorderedMap.h"

#define VE_MUST_USE_RESULT              __attribute__((warn_unused_result))
#define VE_DONT_OPTIMIZE                __attribute__((optimize("O0")))

#define VE_ALWAYS_INLINE                __attribute__((always_inline))
#define VE_ALWAYS_NO_INLINE             __attribute__((noinline))

#define VE_BEGIN_NO_OPTIMIZATION        _Pragma("GCC push_options") \
                                        _Pragma("GCC optimize(\"O0\")")
#define VE_END_NO_OPTIMIZATION          _Pragma("GCC pop_options")

#ifndef UNUSED
#define UNUSED(x) (void)(x)
#endif  // UNUSED

#ifndef COMPILER_BARRIER
#define COMPILER_BARRIER()              asm volatile ("":::"memory")
#endif  // COMPILER_BARRIER

namespace vesoft {

using GraphSpaceID = int32_t;
using PartitionID = int32_t;
using TermID = int64_t;
using LogID = int64_t;
using IPv4 = int32_t;
using Port = int32_t;

template<typename Key, typename T>
using UnorderedMap = typename std::conditional<
    std::is_same<Key, std::string>::value,
//    StringUnorderedMap<T>,
    std::unordered_map<std::string, T>,
    std::unordered_map<Key, T>
>::type;

}  // namespace vesoft
#endif  // COMMON_BASE_BASE_H_
