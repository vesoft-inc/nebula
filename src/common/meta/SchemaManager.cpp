/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "common/meta/SchemaManager.h"  // for SchemaManager

#include <folly/Range.h>  // for StringPiece

#include <cstdint>        // for int32_t
#include <string>         // for string, basic_string
#include <unordered_map>  // for unordered_map, _Node_iter...
#include <utility>        // for pair, make_pair, move

#include "common/base/Status.h"         // for Status, NG_RETURN_IF_ERROR
#include "common/base/StatusOr.h"       // for StatusOr
#include "common/meta/SchemaManager.h"  // for SchemaManager
#include "common/thrift/ThriftTypes.h"  // for TagID, GraphSpaceID

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
