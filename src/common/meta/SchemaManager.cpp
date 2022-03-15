/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/base/Base.h"
#include "common/meta/ServerBasedSchemaManager.h"

namespace nebula {
namespace meta {

StatusOr<std::pair<bool, int32_t>> SchemaManager::getSchemaIDByName(GraphSpaceID space,
                                                                    folly::StringPiece schemaName) {
  auto ret = toEdgeType(space, schemaName);
  if (ret.ok()) {
    return std::make_pair(true, ret.value());
  } else {
    ret = toTagID(space, schemaName);
    if (ret.ok()) {
      return std::make_pair(false, ret.value());
    }
  }
  return Status::Error("Schema not exist: %s", schemaName.str().c_str());
}

StatusOr<std::unordered_map<TagID, std::string>> SchemaManager::getAllTags(GraphSpaceID space) {
  std::unordered_map<TagID, std::string> tags;
  auto tagSchemas = getAllLatestVerTagSchema(space);
  NG_RETURN_IF_ERROR(tagSchemas);
  for (auto& tagSchema : tagSchemas.value()) {
    auto tagName = toTagName(space, tagSchema.first);
    NG_RETURN_IF_ERROR(tagName);
    tags.emplace(tagSchema.first, tagName.value());
  }
  return tags;
}

}  // namespace meta
}  // namespace nebula
