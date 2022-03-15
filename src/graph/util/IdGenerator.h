// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_PLANNER_IDGENERATOR_H_
#define GRAPH_PLANNER_IDGENERATOR_H_

#include "common/base/Base.h"

namespace nebula {
namespace graph {

// TODO(Aiee) Remove class and use snowflake id generater instead
class IdGenerator {
 public:
  explicit IdGenerator(int64_t init = 0) : counter_(init) {}

  // The valid id starts from 0.
  static constexpr int64_t INVALID_ID = -1;

  int64_t id() {
    return counter_++;
  }

 private:
  std::atomic<int64_t> counter_{0};
};

class EPIdGenerator final : public IdGenerator {
 public:
  EPIdGenerator(EPIdGenerator&) = delete;
  EPIdGenerator& operator=(const EPIdGenerator) = delete;

  static EPIdGenerator& instance() {
    return instance_;
  }

 private:
  EPIdGenerator() = default;

 private:
  static EPIdGenerator instance_;
};

}  // namespace graph
}  // namespace nebula
#endif  // GRAPH_PLANNER_IDGENERATOR_H_
