/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "tools/db-upgrade/NebulaKeyUtilsV3.h"

namespace nebula {
std::string NebulaKeyUtilsV3::partTagPrefix(PartitionID partId) {
  PartitionID item = (partId << kPartitionOffset) | static_cast<uint32_t>(kTag_);
  std::string key;
  key.reserve(sizeof(PartitionID));
  key.append(reinterpret_cast<const char*>(&item), sizeof(PartitionID));
  return key;
}
std::string NebulaKeyUtilsV3::getVertexKey(folly::StringPiece tagKey) {
  std::string key = tagKey.toString();
  key[0] = static_cast<uint32_t>(kVertex);
  key.resize(key.size() - sizeof(TagID));
  return key;
}
std::string NebulaKeyUtilsV3::dataVersionValue() {
  return "3.0";
}

}  // namespace nebula
