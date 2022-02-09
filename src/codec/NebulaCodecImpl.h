/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef NEBULA_GRAPH_NEBULACODECIMPL_H
#define NEBULA_GRAPH_NEBULACODECIMPL_H

#include "codec/include/NebulaCodec.h"

namespace nebula {

class NebulaCodecImpl : public NebulaCodec {
 public:
  std::string encode(std::vector<Value> values,
                     std::shared_ptr<const meta::SchemaProviderIf> schema = nullptr) override;

  StatusOr<std::unordered_map<std::string, Value>> decode(
      std::string encoded, std::shared_ptr<const meta::SchemaProviderIf> schema) override;
};

}  // namespace nebula

#endif  // NEBULA_GRAPH_NEBULACODECIMPL_H
