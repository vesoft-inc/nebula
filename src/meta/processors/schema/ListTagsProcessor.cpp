/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/ListTagsProcessor.h"

namespace nebula {
namespace meta {

void ListTagsProcessor::process(const cpp2::ListTagsReq &req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);

  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  // The TagItem obtained directly through tagId is inaccurate (it may be invalid data).
  // It is accurate to obtain tagId by traversing the tag index table, and then obtain TagItem
  // through tagId
  auto indexTagPrefix = MetaKeyUtils::indexTagPrefix(spaceId);
  auto indexTagRet = doPrefix(indexTagPrefix);
  if (!nebula::ok(indexTagRet)) {
    LOG(INFO) << "List Tags failed, SpaceID: " << spaceId;
    handleErrorCode(nebula::error(indexTagRet));
    onFinished();
    return;
  }

  auto indexTagIter = nebula::value(indexTagRet).get();
  std::vector<nebula::meta::cpp2::TagItem> tags;

  while (indexTagIter->valid()) {
    auto tagId = *reinterpret_cast<const TagID *>(indexTagIter->val().toString().c_str());

    auto tagSchemaPrefix = MetaKeyUtils::schemaTagPrefix(spaceId, tagId);
    auto tagSchemaRet = doPrefix(tagSchemaPrefix);
    if (!nebula::ok(tagSchemaRet)) {
      LOG(INFO) << "Get Tag failed, SpaceID: " << spaceId << ", tagId: " << tagId;
      handleErrorCode(nebula::error(tagSchemaRet));
      onFinished();
      return;
    }

    auto tagSchemaIter = nebula::value(tagSchemaRet).get();
    while (tagSchemaIter->valid()) {
      auto key = tagSchemaIter->key();
      auto val = tagSchemaIter->val();
      auto version = MetaKeyUtils::parseTagVersion(key);
      auto nameLen = *reinterpret_cast<const int32_t *>(val.data());
      auto tagName = val.subpiece(sizeof(int32_t), nameLen).str();
      auto schema = MetaKeyUtils::parseSchema(val);
      cpp2::TagItem item;
      item.tag_id_ref() = tagId;
      item.tag_name_ref() = std::move(tagName);
      item.version_ref() = version;
      item.schema_ref() = std::move(schema);
      tags.emplace_back(std::move(item));
      tagSchemaIter->next();
    }
    indexTagIter->next();
  }

  resp_.tags_ref() = std::move(tags);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
