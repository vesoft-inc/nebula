/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/parts/DropSpaceProcessor.h"

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void DropSpaceProcessor::process(const cpp2::DropSpaceReq& req) {
  folly::SharedMutex::ReadHolder rHolder(LockUtils::snapshotLock());
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  const auto& spaceName = req.get_space_name();
  auto spaceRet = getSpaceId(spaceName);

  if (!nebula::ok(spaceRet)) {
    auto retCode = nebula::error(spaceRet);
    if (retCode == nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
      if (req.get_if_exists()) {
        retCode = nebula::cpp2::ErrorCode::SUCCEEDED;
      } else {
        LOG(INFO) << "Drop space Failed, space " << spaceName << " not existed.";
      }
    } else {
      LOG(INFO) << "Drop space Failed, space " << spaceName
                << " error: " << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto spaceId = nebula::value(spaceRet);
  auto batchHolder = std::make_unique<kvstore::BatchHolder>();

  // 1. Delete related part meta data.
  auto prefix = MetaKeyUtils::partPrefix(spaceId);
  auto iterRet = doPrefix(prefix);
  if (!nebula::ok(iterRet)) {
    auto retCode = nebula::error(iterRet);
    LOG(INFO) << "Drop space Failed, space " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto iter = nebula::value(iterRet).get();
  while (iter->valid()) {
    auto key = iter->key();
    batchHolder->remove(key.str());
    iter->next();
  }

  // 2. Delete this space data
  batchHolder->remove(MetaKeyUtils::indexSpaceKey(spaceName));
  batchHolder->remove(MetaKeyUtils::spaceKey(spaceId));

  // 3. Delete related role data.
  auto rolePrefix = MetaKeyUtils::roleSpacePrefix(spaceId);
  auto roleRet = doPrefix(rolePrefix);
  if (!nebula::ok(roleRet)) {
    auto retCode = nebula::error(roleRet);
    LOG(INFO) << "Drop space Failed, space " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto roleIter = nebula::value(roleRet).get();
  while (roleIter->valid()) {
    VLOG(3) << "Revoke role " << MetaKeyUtils::parseRoleStr(roleIter->val()) << " for user "
            << MetaKeyUtils::parseRoleUser(roleIter->key());
    auto key = roleIter->key();
    batchHolder->remove(key.str());
    roleIter->next();
  }

  // 4. Delete listener meta data
  auto lstPrefix = MetaKeyUtils::listenerPrefix(spaceId);
  auto lstRet = doPrefix(rolePrefix);
  if (!nebula::ok(lstRet)) {
    auto retCode = nebula::error(lstRet);
    LOG(INFO) << "Drop space Failed, space " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto lstIter = nebula::value(lstRet).get();
  while (lstIter->valid()) {
    auto key = lstIter->key();
    batchHolder->remove(key.str());
    lstIter->next();
  }

  // 5. Delete related stats data
  auto statskey = MetaKeyUtils::statsKey(spaceId);
  batchHolder->remove(std::move(statskey));

  // 6. Delete related fulltext index meta data
  auto ftPrefix = MetaKeyUtils::fulltextIndexPrefix();
  auto ftRet = doPrefix(ftPrefix);
  if (!nebula::ok(ftRet)) {
    auto retCode = nebula::error(ftRet);
    LOG(INFO) << "Drop space Failed, space " << spaceName
              << " error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto ftIter = nebula::value(ftRet).get();
  while (ftIter->valid()) {
    auto index = MetaKeyUtils::parsefulltextIndex(ftIter->val());
    if (index.get_space_id() == spaceId) {
      auto key = ftIter->key();
      batchHolder->remove(key.str());
    }
    ftIter->next();
  }

  // 7. Delete local_id meta data
  auto localIdkey = MetaKeyUtils::localIdKey(spaceId);
  batchHolder->remove(std::move(localIdkey));

  // 8. Delete all job data
  auto jobPrefix = MetaKeyUtils::jobPrefix(spaceId);
  auto jobRet = doPrefix(jobPrefix);
  if (!nebula::ok(jobRet)) {
    auto retCode = nebula::error(jobRet);
    LOG(INFO) << "Loading Job Failed" << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto jobIter = nebula::value(jobRet).get();
  while (jobIter->valid()) {
    batchHolder->remove(jobIter->key().str());
    jobIter->next();
  }

  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
  LOG(INFO) << "Drop space " << spaceName << ", id " << spaceId;
}

}  // namespace meta
}  // namespace nebula
