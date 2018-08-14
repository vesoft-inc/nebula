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
#include <deque>

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cerrno>
#include <cstring>
#include <ctime>

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

//#include "base/StringUnorderedMap.h"

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
