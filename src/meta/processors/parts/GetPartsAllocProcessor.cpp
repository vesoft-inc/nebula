/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/parts/GetPartsAllocProcessor.h"

namespace nebula {
namespace meta {

void GetPartsAllocProcessor::process(const cpp2::GetPartsAllocReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
  auto spaceId = req.get_space_id();
  auto prefix = MetaServiceUtils::partPrefix(spaceId);
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
    std::vector<HostAddr> partHosts = MetaServiceUtils::parsePartVal(iter->val());
    parts.emplace(partId, std::move(partHosts));
    iter->next();
  }
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.set_parts(std::move(parts));
  auto terms = getTerm(spaceId);
  if (!terms.empty()) {
    resp_.set_terms(std::move(terms));
  }
  onFinished();
}

std::unordered_map<PartitionID, TermID> GetPartsAllocProcessor::getTerm(GraphSpaceID spaceId) {
  std::unordered_map<PartitionID, TermID> ret;

  auto spaceKey = MetaServiceUtils::spaceKey(spaceId);
  auto spaceVal = doGet(spaceKey);
  if (!nebula::ok(spaceVal)) {
    auto rc = nebula::error(spaceVal);
    LOG(ERROR) << "Get Space SpaceId: " << spaceId
               << " error: " << apache::thrift::util::enumNameSafe(rc);
    handleErrorCode(rc);
    return ret;
  }

  auto properties = MetaServiceUtils::parseSpace(nebula::value(spaceVal));
  auto partNum = properties.get_partition_num();

  std::vector<PartitionID> partIdVec;
  std::vector<std::string> leaderKeys;
  for (auto partId = 1; partId <= partNum; ++partId) {
    partIdVec.emplace_back(partId);
    leaderKeys.emplace_back(MetaServiceUtils::leaderKey(spaceId, partId));
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
      std::tie(std::ignore, term, code) = MetaServiceUtils::parseLeaderValV3(vals[i]);
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
