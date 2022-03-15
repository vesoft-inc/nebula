/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef TOOLS_METADATAUPDATETOOL_OLDTHRIFT_METADATAUPDATE_V2_H_
#define TOOLS_METADATAUPDATETOOL_OLDTHRIFT_METADATAUPDATE_V2_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/thrift/ThriftTypes.h"
#include "interface/gen-cpp2/meta_types.h"
#include "kvstore/Common.h"
#include "meta/upgrade/v2/gen-cpp2/meta_types.h"

namespace nebula::meta::v2 {

const std::string kGroupsTable = "__groups__";  // NOLINT

/**
 * @brief Meta kv store utils to help parsing key and values.
 *        These shoule be used to upgrade meta from 2.x to 3.x
 *
 */
class MetaServiceUtilsV2 final {
 public:
  MetaServiceUtilsV2() = delete;

  static std::string groupKey(const std::string& group);

  static std::vector<std::string> parseZoneNames(folly::StringPiece rawData);

  static cpp2::SpaceDesc parseSpace(folly::StringPiece rawData);
};

}  // namespace nebula::meta::v2

#endif  // TOOLS_METADATAUPDATETOOL_OLDTHRIFT_METADATAUPDATE_V2_H_
