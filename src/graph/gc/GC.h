// Copyright (c) 2022 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_GC_H_
#define GRAPH_GC_H_

#include "common/base/Base.h"
#include "graph/context/Result.h"

namespace nebula {
namespace graph {

class GC {
 public:
  static void clear(std::vector<Result>&& garbage) {
    queue_.enqueue(garbage);
  }

  // TODO: async consumes and release the memory.

 private:
  static folly::UMPSCQueue<std::vector<Result>, false> queue_;
};
}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_GC_H_
