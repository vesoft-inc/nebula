/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_NEBULACODEC_H
#define NEBULA_GRAPH_NEBULACODEC_H

#include "base/StatusOr.h"
#include <string>
#include <vector>
#include <unordered_map>
#include "meta/SchemaProviderIf.h"
#include <boost/any.hpp>

namespace nebula {
namespace dataman {

typedef boost::any Value;

class NebulaCodec {
public:
    virtual std::string encode(std::vector<Value> values,
                               std::shared_ptr<const meta::SchemaProviderIf> schema
                                   = std::shared_ptr<const meta::SchemaProviderIf>()) = 0;

    virtual StatusOr<std::unordered_map<std::string, Value>>
    decode(std::string encoded,
           std::shared_ptr<const meta::SchemaProviderIf> schema) = 0;
};

}  // namespace dataman
}  // namespace nebula
#endif  // NEBULA_GRAPH_NEBULACODEC_H
