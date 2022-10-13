/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef TOOLS_DB_UPGRADE_NEBULAKEYUTILSV3_H
#define TOOLS_DB_UPGRADE_NEBULAKEYUTILSV3_H
#include "common/utils/Types.h"
namespace nebula {
class NebulaKeyUtilsV3 {
 public:
  static std::string partTagPrefix(PartitionID partId);
  static std::string getVertexKey(folly::StringPiece tagKey);
  static std::string dataVersionValue();

 private:
  enum NebulaKeyTypeV3 : uint32_t { kTag_ = 0x00000001, kVertex = 0x00000007 };
};

}  // namespace nebula
#endif
