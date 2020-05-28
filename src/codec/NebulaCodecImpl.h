/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef NEBULA_GRAPH_NEBULACODECIMPL_H
#define NEBULA_GRAPH_NEBULACODECIMPL_H

#include "common/base/StatusOr.h"
#include "codec/include/NebulaCodec.h"

namespace nebula {
namespace dataman {

class NebulaCodecImpl : public NebulaCodec {
public:
    std::string encode(std::vector<Value> values,
                       std::shared_ptr<const meta::SchemaProviderIf> schema
                           = std::shared_ptr<const meta::SchemaProviderIf>()) override;

    StatusOr<std::unordered_map<std::string, Value>>
    decode(std::string encoded,
           std::shared_ptr<const meta::SchemaProviderIf> schema) override;
};

}  // namespace dataman
}  // namespace nebula

#endif  // NEBULA_GRAPH_NEBULACODECIMPL_H
