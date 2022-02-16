/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/ListSpacesProcessor.h"

#include <folly/SharedMutex.h>              // for SharedMutex
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref

#include <algorithm>  // for max
#include <memory>     // for unique_ptr
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for basic_string, operator<<
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LogMessage, COMPACT_G...
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils, EntryType
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doPrefix
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void ListSpacesProcessor::process(const cpp2::ListSpacesReq&) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  const auto& prefix = MetaKeyUtils::spacePrefix();
  auto ret = doPrefix(prefix);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    LOG(ERROR) << "List spaces failed, error " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  auto iter = nebula::value(ret).get();

  std::vector<cpp2::IdName> spaces;
  while (iter->valid()) {
    auto spaceId = MetaKeyUtils::spaceId(iter->key());
    auto spaceName = MetaKeyUtils::spaceName(iter->val());
    VLOG(3) << "List spaces " << spaceId << ", name " << spaceName;
    cpp2::IdName space;
    space.id_ref() = to(spaceId, EntryType::SPACE);
    space.name_ref() = std::move(spaceName);
    spaces.emplace_back(std::move(space));
    iter->next();
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.spaces_ref() = std::move(spaces);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
