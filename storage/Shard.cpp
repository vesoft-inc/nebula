/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "storage/Shard.h"

namespace vesoft {
namespace vgraph {
namespace storage {

ResultCode Shard::get(const std::string& key, std::string& value) {
    VLOG(3) << "Get " << key << ", result " << value;
    return ResultCode::SUCCESSED;
}

ResultCode Shard::put(std::string key, std::string value) {
    VLOG(3) << "Put " << key << ", result " << value;
    return ResultCode::SUCCESSED;
}

}  // namespace storage
}  // namespace vgraph
}  // namespace vesoft

