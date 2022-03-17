/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/service/ServiceProcessor.h"

#include "kvstore/LogEncoder.h"

namespace nebula {
namespace meta {

void SignInServiceProcessor::process(const cpp2::SignInServiceReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto type = req.get_type();

  auto serviceKey = MetaKeyUtils::serviceKey(type);
  auto ret = doGet(serviceKey);
  if (nebula::ok(ret)) {
    LOG(INFO) << "Service already exists.";
    handleErrorCode(nebula::cpp2::ErrorCode::E_EXISTED);
    onFinished();
    return;
  } else {
    auto retCode = nebula::error(ret);
    if (retCode != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      LOG(INFO) << "Sign in service failed, error: " << apache::thrift::util::enumNameSafe(retCode);
      handleErrorCode(retCode);
      onFinished();
      return;
    }
  }

  std::vector<kvstore::KV> data;
  data.emplace_back(std::move(serviceKey), MetaKeyUtils::serviceVal(req.get_clients()));
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(data, timeInMilliSec);
  auto result = doSyncPut(std::move(data));
  handleErrorCode(result);
  onFinished();
}

void SignOutServiceProcessor::process(const cpp2::SignOutServiceReq& req) {
  folly::SharedMutex::WriteHolder holder(LockUtils::lock());
  auto type = req.get_type();

  auto serviceKey = MetaKeyUtils::serviceKey(type);
  auto ret = doGet(serviceKey);
  if (!nebula::ok(ret)) {
    auto retCode = nebula::error(ret);
    if (retCode == nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
      LOG(INFO) << "Sign out service failed, service not exists.";
    } else {
      LOG(INFO) << "Sign out service failed, error: "
                << apache::thrift::util::enumNameSafe(retCode);
    }
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  auto batchHolder = std::make_unique<kvstore::BatchHolder>();
  batchHolder->remove({std::move(serviceKey)});
  auto timeInMilliSec = time::WallClock::fastNowInMilliSec();
  LastUpdateTimeMan::update(batchHolder.get(), timeInMilliSec);
  auto batch = encodeBatchValue(std::move(batchHolder)->getBatch());
  doBatchOperation(std::move(batch));
}

void ListServiceClientsProcessor::process(const cpp2::ListServiceClientsReq& req) {
  folly::SharedMutex::ReadHolder holder(LockUtils::lock());
  auto type = req.get_type();

  std::unordered_map<cpp2::ExternalServiceType, std::vector<cpp2::ServiceClient>> serviceClients;
  const auto& serviceKey = MetaKeyUtils::serviceKey(type);
  auto ret = doGet(serviceKey);
  if (!nebula::ok(ret) && nebula::error(ret) != nebula::cpp2::ErrorCode::E_KEY_NOT_FOUND) {
    auto retCode = nebula::error(ret);
    LOG(INFO) << "List service failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  if (nebula::ok(ret)) {
    serviceClients.emplace(type, MetaKeyUtils::parseServiceClients(nebula::value(ret)));
  }

  resp_.clients_ref() = std::move(serviceClients);
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
