/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_NEBULACODEC_H
#define NEBULA_GRAPH_NEBULACODEC_H

#include <string>
#include <vector>
#include <unordered_map>
#include "base/Base.h"
#include "meta/SchemaProviderIf.h"

namespace nebula {
namespace dataman {

typedef boost::any Value;

class NebulaCodec {
 public:
  virtual std::string encode(std::vector<Value> values) = 0;

  virtual std::unordered_map<std::string, Value> decode(std::string encoded,
          std::vector<std::pair<std::string, storage::cpp2::SupportedType>> fields) = 0;
};

}  // namespace dataman
}  // namespace nebula
#endif  // NEBULA_GRAPH_NEBULACODEC_H
