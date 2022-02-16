/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/GetPartsAllocProcessor.h"

#include <folly/Format.h>                   // for sformat
#include <folly/Range.h>                    // for Range
#include <folly/SharedMutex.h>              // for SharedMutex
#include <string.h>                         // for memcpy
#include <thrift/lib/cpp/util/EnumUtils.h>  // for enumNameSafe
#include <thrift/lib/cpp2/FieldRef.h>       // for field_ref, optional_f...

#include <algorithm>  // for max
#include <memory>     // for allocator_traits<>::v...
#include <ostream>    // for operator<<, basic_ost...
#include <string>     // for string, basic_string
#include <tuple>      // for tie, ignore, tuple
#include <vector>     // for vector

#include "common/base/ErrorOr.h"              // for error, ok, value
#include "common/base/Logging.h"              // for LOG, LogMessage, _LOG...
#include "common/datatypes/HostAddr.h"        // for HostAddr
#include "common/utils/MetaKeyUtils.h"        // for MetaKeyUtils, kDefaul...
#include "interface/gen-cpp2/common_types.h"  // for ErrorCode, ErrorCode:...
#include "kvstore/KVIterator.h"               // for KVIterator
#include "kvstore/KVStore.h"                  // for KVStore
#include "meta/processors/BaseProcessor.h"    // for BaseProcessor::doGet
#include "meta/processors/Common.h"           // for LockUtils

namespace nebula {
namespace meta {

void GetPartsAllocProcessor::process(const cpp2::GetPartsAllocReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  auto spaceId = req.get_space_id();
  auto prefix = MetaKeyUtils::partPrefix(spaceId);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(ERROR) << "Get parts failed, error " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  std::unordered_map<PartitionID, std::vector<nebula::HostAddr>> parts;
  while (iter->valid()) {
    auto key = iter->key();
    PartitionID partId;
    memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
    std::vector<HostAddr> partHosts = MetaKeyUtils::parsePartVal(iter->val());
    parts.emplace(partId, std::move(partHosts));
    iter->next();
  }
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.parts_ref() = std::move(parts);
  auto terms = getTerm(spaceId);
  if (!terms.empty()) {
    resp_.terms_ref() = std::move(terms);
  }
  onFinished();
}

std::unordered_map<PartitionID, TermID> GetPartsAllocProcessor::getTerm(GraphSpaceID spaceId) {
  std::unordered_map<PartitionID, TermID> ret;

  auto spaceKey = MetaKeyUtils::spaceKey(spaceId);
  auto spaceVal = doGet(spaceKey);
  if (!nebula::ok(spaceVal)) {
    auto rc = nebula::error(spaceVal);
    LOG(ERROR) << "Get Space SpaceId: " << spaceId
               << " error: " << apache::thrift::util::enumNameSafe(rc);
    handleErrorCode(rc);
    return ret;
  }

  auto properties = MetaKeyUtils::parseSpace(nebula::value(spaceVal));
  auto partNum = properties.get_partition_num();

  std::vector<PartitionID> partIdVec;
  std::vector<std::string> leaderKeys;
  for (auto partId = 1; partId <= partNum; ++partId) {
    partIdVec.emplace_back(partId);
    leaderKeys.emplace_back(MetaKeyUtils::leaderKey(spaceId, partId));
  }

  std::vector<std::string> vals;
  auto [code, statusVec] =
      kvstore_->multiGet(kDefaultSpaceId, kDefaultPartId, std::move(leaderKeys), &vals);
  if (code != nebula::cpp2::ErrorCode::SUCCEEDED &&
      code != nebula::cpp2::ErrorCode::E_PARTIAL_RESULT) {
    LOG(INFO) << "error rc = " << apache::thrift::util::enumNameSafe(code);
    return ret;
  }

  TermID term;
  for (auto i = 0U; i != vals.size(); ++i) {
    if (statusVec[i].ok()) {
      std::tie(std::ignore, term, code) = MetaKeyUtils::parseLeaderValV3(vals[i]);
      if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        LOG(WARNING) << apache::thrift::util::enumNameSafe(code);
        LOG(INFO) << folly::sformat("term of part {} is invalid", partIdVec[i]);
        continue;
      }
      LOG(INFO) << folly::sformat("term of part {} is {}", partIdVec[i], term);
      ret[partIdVec[i]] = term;
    }
  }

  return ret;
}

}  // namespace meta
}  // namespace nebula
