/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_NEBULACODECIMPL_H
#define NEBULA_GRAPH_NEBULACODECIMPL_H

#include "dataman/include/NebulaCodec.h"

namespace nebula {
namespace dataman {

class NebulaCodecImpl : public NebulaCodec {
 public:
  std::string encode(std::vector<Value> values) override;
};

 }  // dataman
}  // nebula

 #endif  // NEBULA_GRAPH_NEBULACODECIMPL_H
