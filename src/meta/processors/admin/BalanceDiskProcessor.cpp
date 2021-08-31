/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/BalanceDiskProcessor.h"

#include "meta/processors/admin/Balancer.h"

namespace nebula {
namespace meta {

void BalanceDiskProcessor::process(const cpp2::BalanceDiskReq& req) {
  auto host = req.get_host();
  auto disks = req.get_disks();
  auto dismiss = req.get_dismiss();
  auto ret = Balancer::instance(kvstore_)->balanceDisk(std::move(host), std::move(disks), dismiss);
  if (!ok(ret)) {
    auto retCode = error(ret);
    LOG(ERROR) << "Balance Disk Failed: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }
  resp_.set_id(value(ret));
  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  onFinished();
}

}  // namespace meta
}  // namespace nebula
