/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef NEBULA_GRAPH_MEMORYSTATS_H
#define NEBULA_GRAPH_MEMORYSTATS_H

#include <atomic>

namespace nebula {

class MemoryStats {
 public:
  static MemoryStats& instance() {
    static MemoryStats stats;
    return stats;
  }

  void add(int64_t bytes) {
    amount_ += bytes;
  }

  int64_t amount() {
    return amount_;
  }

 private:
  std::atomic<int64_t> amount_{0};
};

}  // namespace nebula

#endif  // NEBULA_GRAPH_MEMORYSTATS_H
