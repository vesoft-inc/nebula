/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "thread/ThreadManager.h"

DEFINE_string(num_threads_per_priority, "",
        "Number of worker threads for each request priority.");
DEFINE_int32(num_worker_threads, 0, "Number of threads to execute user queries");

namespace nebula {
namespace thread {

static std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager>
createPriorityThreadManager() {
    std::vector<size_t> threadNums;
    folly::split(":", FLAGS_num_threads_per_priority, threadNums, false);
    if (threadNums.size() != apache::thrift::concurrency::N_PRIORITIES) {
        LOG(WARNING) << "num_threads_per_priority does not declare all priorities,"
                << " it's value is " << FLAGS_num_threads_per_priority;
        threadNums.resize(apache::thrift::concurrency::N_PRIORITIES);
    }
    std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> workers(
            apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(
                    {{
                        threadNums[apache::thrift::concurrency::HIGH_IMPORTANT],
                        threadNums[apache::thrift::concurrency::HIGH],
                        threadNums[apache::thrift::concurrency::IMPORTANT],
                        threadNums[apache::thrift::concurrency::NORMAL],
                        threadNums[apache::thrift::concurrency::BEST_EFFORT]
                    }}, false /*stats*/));
    workers->setNamePrefix("priority_executor");
    workers->start();
    return workers;
}

static std::shared_ptr<apache::thrift::concurrency::ThreadManager>
createSimpleThreadManager() {
    if (FLAGS_num_worker_threads == 0) {
        FLAGS_num_worker_threads = std::thread::hardware_concurrency();
    }
    LOG(INFO) << "Number of worker threads: " << FLAGS_num_worker_threads;
    std::shared_ptr<apache::thrift::concurrency::ThreadManager> workers(
            apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(
                    FLAGS_num_worker_threads));
    auto threadFactory = std::make_shared<apache::thrift::concurrency::PosixThreadFactory>();
    workers->threadFactory(threadFactory);
    workers->setNamePrefix("simple_executor");
    workers->start();
    return workers;
}

std::shared_ptr<apache::thrift::concurrency::ThreadManager> getThreadManager() {
    if (!FLAGS_num_threads_per_priority.empty()) {
        static std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> priorityMgr(
                createPriorityThreadManager());
        return priorityMgr;
    } else {
        static std::shared_ptr<apache::thrift::concurrency::ThreadManager> simpleMgr(
                createSimpleThreadManager());
        return simpleMgr;
    }
}

}   // namespace thread
}   // namespace nebula
