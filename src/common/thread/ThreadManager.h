/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef COMMON_THREAD_THREADMANAGER_H_
#define COMMON_THREAD_THREADMANAGER_H_

#include "base/Base.h"
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

DECLARE_string(num_threads_per_priority);
DECLARE_int32(num_worker_threads);

namespace nebula {
namespace thread {

std::shared_ptr<apache::thrift::concurrency::ThreadManager> getThreadManager();

}   // namespace thread
}   // namespace nebula

#endif /* COMMON_THREAD_THREADMANAGER_H_ */
