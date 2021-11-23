/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/MetaStreamService.h"

namespace nebula {
namespace meta {

apache::thrift::ServerStream<cpp2::MetaData> MetaStreamService::readMetaData(
    int64_t lastRevision) {
  VLOG(3) << "Received the call readMetaData()";
  auto [stream, publisher] =
    apache::thrift::ServerStream<cpp2::MetaData>::createPublisher();

  // Get the EventBase for the current thread
  auto evb = folly::EventBaseManager::get()->getEventBase();
//  auto evb = folly::getGlobalIOExecutor()->getEventBase();

  CHECK_NOTNULL(cache_);
  cache_->addPublisher(std::move(publisher), evb, lastRevision);

  VLOG(3) << "Returned from the readMetaData() call";
  return std::move(stream);
}

}  // namespace meta
}  // namespace nebula
