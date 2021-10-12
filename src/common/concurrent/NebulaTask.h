/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#pragma once

namespace nebula {
namespace concurrency {

class NebulaTask {
 public:
  virtual ~NebulaTask() = default;
};

}  // namespace concurrency
}  // namespace nebula
