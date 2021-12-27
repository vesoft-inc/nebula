/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/kv/MultiGetProcessor.h"

namespace nebula {
namespace meta {

void MultiGetProcessor::process(const cpp2::MultiGetReq& req) {
  std::vector<std::string> keys;
  for (auto& key : req.get_keys()) {
    keys.emplace_back(MetaKeyUtils::assembleSegmentKey(req.get_segment(), key));
  }

  auto result = doMultiGet(std::move(keys));
  if (!nebula::ok(result)) {
    auto retCode = nebula::error(result);
    LOG(ERROR) << "MultiGet Failed, error: " << apache::thrift::util::enumNameSafe(retCode);
    handleErrorCode(retCode);
    onFinished();
    return;
  }

  handleErrorCode(nebula::cpp2::ErrorCode::SUCCEEDED);
  resp_.values_ref() = std::move(nebula::value(result));
  onFinished();
}

}  // namespace meta
}  // namespace nebula
