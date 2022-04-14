/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include <folly/Executor.h>

namespace nebula {

class NebulaTaskExecutor : public folly::Executor {
 public:
  explicit NebulaTaskExecutor(folly::Executor* proxy);

  ~NebulaTaskExecutor();

  void add(folly::Func f) override;

 private:
  class TaskManager;
  std::unique_ptr<TaskManager> taskMgr_;
};

}  // namespace nebula
