// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_GC_H_
#define GRAPH_GC_H_

#include "common/base/Base.h"
#include "common/thread/GenericThreadPool.h"
#include "graph/context/Result.h"

namespace nebula {
namespace graph {

// Clean the unused memory on background threads, this is helpful
// for big queries since the memory release of interim results may
// cost too much time.
class GC {
 public:
  static GC& instance();

  ~GC() {
    workers_.stop();
  }

  void clear(std::vector<Result>&& garbage);

 private:
  GC();
  void periodicTask();
  folly::UMPMCQueue<std::vector<Result>, false> queue_;
  thread::GenericThreadPool workers_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_GC_H_
