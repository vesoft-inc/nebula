/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_FILEBASEDPARTMANAGER_H_
#define META_FILEBASEDPARTMANAGER_H_

#include "base/Base.h"
#include "meta/PartManager.h"

namespace nebula {
namespace meta {

class FileBasedPartManager final : public PartManager {
    friend class PartManager;
private:
    static std::unordered_map<GraphSpaceID, std::shared_ptr<const PartManager>> init();

    FileBasedPartManager() = default;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_FILEBASEDPARTMANAGER_H_
