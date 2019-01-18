/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SERVERBASEDPARTMANAGER_H_
#define META_SERVERBASEDPARTMANAGER_H_

#include "base/Base.h"
#include "meta/PartManager.h"

namespace nebula {
namespace meta {

class ServerBasedPartManager final : public PartManager {
    friend class PartManager;
private:
    static std::unordered_map<GraphSpaceID, std::shared_ptr<const PartManager>> init();

    ServerBasedPartManager() = default;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_ADHOCPARTMANAGER_H_

