/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#pragma once

#include "common/base/StatusOr.h"
#include "common/meta/SchemaProviderIf.h"

namespace nebula {

class NebulaCodec {
 public:
  typedef boost::any Value;

  virtual ~NebulaCodec() = default;

  virtual std::string encode(std::vector<Value> values,
                             std::shared_ptr<const meta::SchemaProviderIf> schema = nullptr) = 0;

  virtual StatusOr<std::unordered_map<std::string, Value>> decode(
      std::string encoded, std::shared_ptr<const meta::SchemaProviderIf> schema) = 0;
};

}  // namespace nebula
