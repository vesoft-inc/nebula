/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/upgrade/v2/MetaServiceUtilsV2.h"

#include <thrift/lib/cpp2/protocol/CompactProtocol.h>
#include <thrift/lib/cpp2/protocol/Serializer.h>

namespace nebula::meta::v2 {

std::string MetaServiceUtilsV2::groupKey(const std::string& group) {
  std::string key;
  key.reserve(kGroupsTable.size() + group.size());
  key.append(kGroupsTable.data(), kGroupsTable.size()).append(group);
  return key;
}

std::vector<std::string> MetaServiceUtilsV2::parseZoneNames(folly::StringPiece rawData) {
  std::vector<std::string> zones;
  folly::split(',', rawData.str(), zones);
  return zones;
}

cpp2::SpaceDesc MetaServiceUtilsV2::parseSpace(folly::StringPiece rawData) {
  cpp2::SpaceDesc spaceDesc;
  apache::thrift::CompactSerializer::deserialize(rawData, spaceDesc);
  return spaceDesc;
}

}  // namespace nebula::meta::v2
