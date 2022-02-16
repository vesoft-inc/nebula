/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/schema/GetTagProcessor.h"

#include <folly/Range.h>               // for StringPiece
#include <folly/SharedMutex.h>         // for SharedMutex
#include <thrift/lib/cpp2/FieldRef.h>  // for field_ref

#include <memory>   // for unique_ptr
#include <ostream>  // for operator<<, basic_ost...
#include <string>   // for operator<<, char_traits

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LogMessage, LOG, _LOG...
#include "common/thrift/ThriftTypes.h"        // for GraphSpaceID
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doGet
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void GetTagProcessor::process(const cpp2::GetTagReq& req) {
  GraphSpaceID spaceId = req.get_space_id();
  CHECK_SPACE_ID_AND_RETURN(spaceId);
  const auto& tagName = req.get_tag_name();
  auto ver = req.get_version();

  folly::SharedMutex::ReadHolder rHolder(LockUtils::tagAndEdgeLock());
  auto tagIdRet = getTagId(spaceId, tagName);
  if (!nebula::ok(tagIdRet)) {
    LOG(ERROR) << "Get tag " << tagName << " failed.";
    handleErrorCode(nebula::error(tagIdRet));
    onFinished();
    return;
  }
  auto tagId = nebula::value(tagIdRet);

  std::string schemaValue;
  // Get the latest version
  if (ver < 0) {
    auto tagPrefix = MetaKeyUtils::schemaTagPrefix(spaceId, tagId);
    auto ret = doPrefix(tagPrefix);
    if (!nebula::ok(ret)) {
      LOG(ERROR) << "Get Tag SpaceID: " << spaceId << ", tagName: " << tagName
                 << ", latest version failed.";
      handleErrorCode(nebula::error(ret));
      onFinished();
      return;
    }
    auto iter = nebula::value(ret).get();
    if (!iter->valid()) {
      LOG(ERROR) << "Get Tag SpaceID: " << spaceId << ", tagName: " << tagName
                 << ", latest version "
                 << " not found.";
      handleErrorCode(nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND);
      onFinished();
      return;
    }
    schemaValue = iter->val().str();
  } else {
    auto tagKey = MetaKeyUtils::schemaTagKey(spaceId, tagId, ver);
    auto ret = doGet(tagKey);
    if (!nebula::ok(ret)) {
      LOG(ERROR) << "Get Tag SpaceID: " << spaceId << ", tagName: " << tagName << ", version "
                 << ver << " failed.";
      handleErrorCode(nebula::error(ret));
      onFinished();
      return;
    }
    schemaValue = nebula::value(ret);
  }

  VLOG(3) << "Get Tag SpaceID: " << spaceId << ", tagName: " << tagName << ", version " << ver;

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.schema_ref() = MetaKeyUtils::parseSchema(schemaValue);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
