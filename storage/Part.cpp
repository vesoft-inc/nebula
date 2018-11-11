/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/Part.h"

namespace vesoft {
namespace vgraph {
namespace storage {

ResultCode SimplePart::asyncMultiPut(std::vector<KV> keyValues, KVCallback cb) {
    CHECK_NOTNULL(engine_);
    auto ret = engine_->multiPut(std::move(keyValues));
    cb(ret, HostAddr(0, 0));
    return ret;
}

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft

