/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_ADHOCPARTMANAGER_H_
#define META_ADHOCPARTMANAGER_H_

#include "base/Base.h"
#include "meta/PartManager.h"

namespace nebula {
namespace meta {

//
// This class is ONLY meant for test purpose. Please DO NOT use it
// in the production environment!!!!
//
class AdHocPartManager final : public PartManager {
    friend class PartManager;
public:
    static void addPartition(GraphSpaceID space, PartitionID part, HostAddr host);

private:
    static std::unordered_map<GraphSpaceID, std::shared_ptr<PartManager>> init();

    AdHocPartManager() = default;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_ADHOCPARTMANAGER_H_

