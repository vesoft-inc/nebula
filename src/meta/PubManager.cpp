/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/PubManager.h"

DEFINE_int32(publisher_life_span, 50,
             "The number of messages published to a publisher before it is destroyed");

namespace nebula {
namespace meta {

std::shared_ptr<PubManager::PublisherInfo> PubManager::addPublisher(
    ::apache::thrift::ServerStreamPublisher<cpp2::MetaData>&& publisher,
    folly::EventBase* evb) {
  return publishers_.emplace_back(
      std::make_shared<PublisherInfo>(std::move(publisher), evb));
}


void PubManager::publishToOne(std::shared_ptr<PubManager::PublisherInfo> pub,
                              cpp2::MetaData&& data) {
  pub->evb_->runInEventBaseThreadAlwaysEnqueue([pub, data = std::move(data)] {
    pub->publisher_.next(data);
    pub->numPushes_++;
  });
}


void PubManager::publishToAll(cpp2::MetaData&& data) {
  std::vector<decltype(publishers_)::iterator> oldPublishers;
  for (auto iter = publishers_.begin(); iter != publishers_.end(); ++iter) {
    auto pub = *iter;
    pub->evb_->runInEventBaseThreadAlwaysEnqueue([pub, data] {
      pub->publisher_.next(data);
      pub->numPushes_++;
    });
    if (pub->numPushes_ > FLAGS_publisher_life_span) {
      oldPublishers.push_back(iter);
    }
  }

  // Remove publishers that are too old
  for (auto& iter : oldPublishers) {
    auto pub = *iter;
    pub->evb_->runInEventBaseThreadAlwaysEnqueue([pub] {
      std::move(pub->publisher_).complete();
      VLOG(3) << "Terminate an old publisher";
    });
    publishers_.erase(iter);
  }
}

}  // namespace meta
}  // namespace nebula
