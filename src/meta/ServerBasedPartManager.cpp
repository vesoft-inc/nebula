/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "base/Base.h"
#include "meta/ServerBasedPartManager.h"

namespace nebula {
namespace meta {

// static
std::unordered_map<GraphSpaceID, std::shared_ptr<const PartManager>>
ServerBasedPartManager::init() {
    std::unordered_map<GraphSpaceID, std::shared_ptr<const PartManager>> pms;
    return pms;
}

}  // namespace meta
}  // namespace nebula


