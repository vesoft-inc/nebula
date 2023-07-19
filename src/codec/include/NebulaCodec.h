/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CODEC_INCLUDE_NEBULACODEC_H
#define CODEC_INCLUDE_NEBULACODEC_H

#include "common/base/StatusOr.h"
#include "common/meta/NebulaSchemaProvider.h"

namespace nebula {

class NebulaCodec {
 public:
  using Value = std::any;

  virtual ~NebulaCodec() = default;

  virtual std::string encode(
      std::vector<Value> values,
      std::shared_ptr<const meta::NebulaSchemaProvider> schema = nullptr) = 0;

  virtual StatusOr<std::unordered_map<std::string, Value>> decode(
      std::string encoded, std::shared_ptr<const meta::NebulaSchemaProvider> schema) = 0;
};

}  // namespace nebula
#endif
