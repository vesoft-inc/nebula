/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef META_PUBMANAGER_H_
#define META_PUBMANAGER_H_

#include "interface/gen-cpp2/meta_stream_types.h"

namespace nebula {
namespace meta {

/********************************************************************
 *
 * PubManager class manages all meta data publishers.
 *
 * This class is included in the MetaCache class. MetaCache needs
 * guarantee the threaf-safe access of PubManager
 *
 ********************************************************************/
class PubManager final {
public:
  struct PublisherInfo {
    PublisherInfo(apache::thrift::ServerStreamPublisher<cpp2::MetaData>&& pub,
                  folly::EventBase* evb)
        : publisher_(std::move(pub))
        , evb_(evb)
        , numPushes_{0} {}

    ::apache::thrift::ServerStreamPublisher<cpp2::MetaData> publisher_;
    folly::EventBase* evb_;
    int32_t numPushes_;
  };

public:
  PubManager() = default;

  // Add a new publisher. If the client's last revision is older, then send
  // the meta data
  std::shared_ptr<PublisherInfo>  addPublisher(
      ::apache::thrift::ServerStreamPublisher<cpp2::MetaData>&& publisher,
      folly::EventBase* evb);

  // Publish meta to a single subscriber
  void publishToOne(std::shared_ptr<PublisherInfo> pub, cpp2::MetaData&& data);

  // Publish meta data to all subscribers
  void publishToAll(cpp2::MetaData&& data);

private:
  // List of all publishers
  std::list<std::shared_ptr<PublisherInfo>> publishers_;
};

}  // namespace meta
}  // namespace nebula
#endif  // META_PUBMANAGER_H_
