// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_EXECUTIONERROR_H_
#define GRAPH_EXECUTOR_EXECUTIONERROR_H_

#include "common/base/Status.h"

namespace nebula {
namespace graph {

class ExecutionError final : public std::runtime_error {
 public:
  explicit ExecutionError(Status status) : std::runtime_error(status.toString()) {
    status_ = std::move(status);
  }

  Status status() const {
    return status_;
  }

 private:
  Status status_;
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_EXECUTIONERROR_H_
