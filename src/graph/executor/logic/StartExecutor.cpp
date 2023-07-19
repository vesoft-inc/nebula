// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/logic/StartExecutor.h"

namespace nebula {
namespace graph {

folly::Future<Status> StartExecutor::execute() {
  SCOPED_TIMER(&execTime_);

  return start();
}

}  // namespace graph
}  // namespace nebula
