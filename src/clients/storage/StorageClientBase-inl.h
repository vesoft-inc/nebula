/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_STORAGECLIENTBASE_INL_H
#define CLIENTS_STORAGE_STORAGECLIENTBASE_INL_H

#include <folly/Try.h>

#include "clients/storage/stats/StorageClientStats.h"
#include "common/ssl/SSLConfig.h"
#include "common/stats/StatsManager.h"
#include "common/time/WallClock.h"

namespace nebula {
namespace storage {

template <class Request, class RemoteFunc, class Response>
struct ResponseContext {
 public:
  ResponseContext(size_t reqsSent, RemoteFunc&& remoteFunc)
      : resp(reqsSent), serverMethod(std::move(remoteFunc)) {}

  // Return true if processed all responses
  bool finishSending() {
    std::lock_guard<std::mutex> g(lock_);
    finishSending_ = true;
    if (ongoingRequests_.empty() && !fulfilled_) {
      fulfilled_ = true;
      return true;
    } else {
      return false;
    }
  }

  std::pair<const Request*, bool> insertRequest(HostAddr host, Request&& req) {
    std::lock_guard<std::mutex> g(lock_);
    auto res = ongoingRequests_.emplace(host, std::move(req));
    return std::make_pair(&res.first->second, res.second);
  }

  const Request& findRequest(HostAddr host) {
    std::lock_guard<std::mutex> g(lock_);
    auto it = ongoingRequests_.find(host);
    DCHECK(it != ongoingRequests_.end());
    return it->second;
  }

  // Return true if processed all responses
  bool removeRequest(HostAddr host) {
    std::lock_guard<std::mutex> g(lock_);
    ongoingRequests_.erase(host);
    if (finishSending_ && !fulfilled_ && ongoingRequests_.empty()) {
      fulfilled_ = true;
      return true;
    } else {
      return false;
    }
  }

 public:
  folly::Promise<StorageRpcResponse<Response>> promise;
  StorageRpcResponse<Response> resp;
  RemoteFunc serverMethod;

 private:
  std::mutex lock_;
  std::unordered_map<HostAddr, Request> ongoingRequests_;
  bool finishSending_{false};
  bool fulfilled_{false};
};

template <typename ClientType, typename ClientManagerType>
StorageClientBase<ClientType, ClientManagerType>::StorageClientBase(
    std::shared_ptr<folly::IOThreadPoolExecutor> threadPool, meta::MetaClient* metaClient)
    : metaClient_(metaClient), ioThreadPool_(threadPool) {
  clientsMan_ = std::make_unique<ClientManagerType>(FLAGS_enable_ssl);
}

template <typename ClientType, typename ClientManagerType>
StorageClientBase<ClientType, ClientManagerType>::~StorageClientBase() {
  VLOG(3) << "Destructing StorageClientBase";
  if (nullptr != metaClient_) {
    metaClient_ = nullptr;
  }
}

template <typename ClientType, typename ClientManagerType>
StatusOr<HostAddr> StorageClientBase<ClientType, ClientManagerType>::getLeader(
    GraphSpaceID spaceId, PartitionID partId) const {
  return metaClient_->getStorageLeaderFromCache(spaceId, partId);
}

template <typename ClientType, typename ClientManagerType>
void StorageClientBase<ClientType, ClientManagerType>::updateLeader(GraphSpaceID spaceId,
                                                                    PartitionID partId,
                                                                    const HostAddr& leader) {
  metaClient_->updateStorageLeader(spaceId, partId, leader);
}

template <typename ClientType, typename ClientManagerType>
void StorageClientBase<ClientType, ClientManagerType>::invalidLeader(GraphSpaceID spaceId,
                                                                     PartitionID partId) {
  metaClient_->invalidStorageLeader(spaceId, partId);
}

template <typename ClientType, typename ClientManagerType>
void StorageClientBase<ClientType, ClientManagerType>::invalidLeader(
    GraphSpaceID spaceId, std::vector<PartitionID>& partsId) {
  for (const auto& partId : partsId) {
    invalidLeader(spaceId, partId);
  }
}

template <typename ClientType, typename ClientManagerType>
template <class Request, class RemoteFunc, class Response>
folly::SemiFuture<StorageRpcResponse<Response>>
StorageClientBase<ClientType, ClientManagerType>::collectResponse(
    folly::EventBase* evb,
    std::unordered_map<HostAddr, Request> requests,
    RemoteFunc&& remoteFunc) {
  using TransportException = apache::thrift::transport::TTransportException;
  auto context = std::make_shared<ResponseContext<Request, RemoteFunc, Response>>(
      requests.size(), std::move(remoteFunc));

  DCHECK(!!ioThreadPool_);

  for (auto& req : requests) {
    auto& host = req.first;
    auto spaceId = req.second.get_space_id();
    auto res = context->insertRequest(host, std::move(req.second));
    DCHECK(res.second);
    evb = ioThreadPool_->getEventBase();
    // Invoke the remote method
    folly::via(evb, [this, evb, context, host, spaceId, res]() mutable {
      auto client = clientsMan_->client(host, evb, false, FLAGS_storage_client_timeout_ms);
      // Result is a pair of <Request&, bool>
      auto start = time::WallClock::fastNowInMicroSec();
      context
          ->serverMethod(client.get(), *res.first)
          // Future process code will be executed on the IO thread
          // Since all requests are sent using the same eventbase, all
          // then-callback will be executed on the same IO thread
          .via(evb)
          .thenValue([this, context, host, spaceId, start](Response&& resp) {
            auto& result = resp.get_result();
            bool hasFailure{false};
            for (auto& code : result.get_failed_parts()) {
              VLOG(3) << "Failure! Failed part " << code.get_part_id() << ", failed code "
                      << static_cast<int32_t>(code.get_code());
              hasFailure = true;
              context->resp.emplaceFailedPart(code.get_part_id(), code.get_code());
              if (code.get_code() == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
                auto* leader = code.get_leader();
                if (isValidHostPtr(leader)) {
                  updateLeader(spaceId, code.get_part_id(), *leader);
                } else {
                  invalidLeader(spaceId, code.get_part_id());
                }
              } else if (code.get_code() == nebula::cpp2::ErrorCode::E_PART_NOT_FOUND ||
                         code.get_code() == nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
                invalidLeader(spaceId, code.get_part_id());
              } else {
                // do nothing
              }
            }
            if (hasFailure) {
              context->resp.markFailure();
            }

            // Adjust the latency
            auto latency = result.get_latency_in_us();
            context->resp.setLatency(host, latency, time::WallClock::fastNowInMicroSec() - start);

            // Keep the response
            context->resp.addResponse(std::move(resp));
          })
          .thenError(folly::tag_t<TransportException>{},
                     [this, context, host, spaceId](TransportException&& ex) {
                       auto& r = context->findRequest(host);
                       auto parts = getReqPartsId(r);
                       if (ex.getType() == TransportException::TIMED_OUT) {
                         LOG(ERROR) << "Request to " << host << " time out: " << ex.what();
                       } else {
                         invalidLeader(spaceId, parts);
                         LOG(ERROR) << "Request to " << host << " failed: " << ex.what();
                       }
                       context->resp.appendFailedParts(parts,
                                                       nebula::cpp2::ErrorCode::E_RPC_FAILURE);
                       context->resp.markFailure();
                     })
          .thenError(folly::tag_t<std::exception>{},
                     [this, context, host, spaceId](std::exception&& ex) {
                       auto& r = context->findRequest(host);
                       auto parts = getReqPartsId(r);
                       LOG(ERROR) << "Request to " << host << " failed: " << ex.what();
                       invalidLeader(spaceId, parts);
                       context->resp.appendFailedParts(parts,
                                                       nebula::cpp2::ErrorCode::E_RPC_FAILURE);
                       context->resp.markFailure();
                     })
          .ensure([context, host] {
            if (context->removeRequest(host)) {
              // Received all responses
              context->promise.setValue(std::move(context->resp));
            }
          });
    });  // via
  }      // for

  if (context->finishSending()) {
    // Received all responses, most likely, all rpc failed
    context->promise.setValue(std::move(context->resp));
  }

  return context->promise.getSemiFuture();
}

template <typename ClientType, typename ClientManagerType>
template <class Request, class RemoteFunc, class Response>
folly::Future<StatusOr<Response>> StorageClientBase<ClientType, ClientManagerType>::getResponse(
    folly::EventBase* evb, std::pair<HostAddr, Request>&& request, RemoteFunc&& remoteFunc) {
  auto pro = std::make_shared<folly::Promise<StatusOr<Response>>>();
  auto f = pro->getFuture();
  getResponseImpl(
      evb, std::forward<decltype(request)>(request), std::forward<RemoteFunc>(remoteFunc), pro);
  return f;
}

template <typename ClientType, typename ClientManagerType>
template <class Request, class RemoteFunc, class Response>
void StorageClientBase<ClientType, ClientManagerType>::getResponseImpl(
    folly::EventBase* evb,
    std::pair<HostAddr, Request> request,
    RemoteFunc remoteFunc,
    std::shared_ptr<folly::Promise<StatusOr<Response>>> pro) {
  stats::StatsManager::addValue(kNumRpcSentToStoraged);
  using TransportException = apache::thrift::transport::TTransportException;
  if (evb == nullptr) {
    DCHECK(!!ioThreadPool_);
    evb = ioThreadPool_->getEventBase();
  }
  auto reqPtr = std::make_shared<std::pair<HostAddr, Request>>(std::move(request.first),
                                                               std::move(request.second));
  folly::via(
      evb,
      [evb, request = std::move(reqPtr), remoteFunc = std::move(remoteFunc), pro, this]() mutable {
        auto host = request->first;
        auto client = clientsMan_->client(host, evb, false, FLAGS_storage_client_timeout_ms);
        auto spaceId = request->second.get_space_id();
        auto partsId = getReqPartsId(request->second);
        remoteFunc(client.get(), request->second)
            .via(evb)
            .thenValue([spaceId, pro, request, this](Response&& resp) mutable {
              auto& result = resp.get_result();
              for (auto& code : result.get_failed_parts()) {
                VLOG(3) << "Failure! Failed part " << code.get_part_id() << ", failed code "
                        << static_cast<int32_t>(code.get_code());
                if (code.get_code() == nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
                  auto* leader = code.get_leader();
                  if (isValidHostPtr(leader)) {
                    updateLeader(spaceId, code.get_part_id(), *leader);
                  } else {
                    invalidLeader(spaceId, code.get_part_id());
                  }
                } else if (code.get_code() == nebula::cpp2::ErrorCode::E_PART_NOT_FOUND ||
                           code.get_code() == nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND) {
                  invalidLeader(spaceId, code.get_part_id());
                }
              }
              pro->setValue(std::move(resp));
            })
            .thenError(folly::tag_t<TransportException>{},
                       [spaceId, partsId = std::move(partsId), host, pro, this](
                           TransportException&& ex) mutable {
                         stats::StatsManager::addValue(kNumRpcSentToStoragedFailed);
                         if (ex.getType() == TransportException::TIMED_OUT) {
                           LOG(ERROR) << "Request to " << host << " time out: " << ex.what();
                         } else {
                           invalidLeader(spaceId, partsId);
                           LOG(ERROR) << "Request to " << host << " failed: " << ex.what();
                         }
                         pro->setValue(Status::Error(
                             folly::stringPrintf("RPC failure in StorageClient: %s", ex.what())));
                       })
            .thenError(folly::tag_t<std::exception>{},
                       [spaceId, partsId = std::move(partsId), host, pro, this](
                           std::exception&& ex) mutable {
                         stats::StatsManager::addValue(kNumRpcSentToStoragedFailed);
                         // exception occurred during RPC
                         pro->setValue(Status::Error(
                             folly::stringPrintf("RPC failure in StorageClient: %s", ex.what())));
                         invalidLeader(spaceId, partsId);
                       });
      });  // via
}

template <typename ClientType, typename ClientManagerType>
template <class Container, class GetIdFunc>
StatusOr<std::unordered_map<
    HostAddr,
    std::unordered_map<PartitionID, std::vector<typename Container::value_type>>>>
StorageClientBase<ClientType, ClientManagerType>::clusterIdsToHosts(GraphSpaceID spaceId,
                                                                    const Container& ids,
                                                                    GetIdFunc f) const {
  std::unordered_map<HostAddr,
                     std::unordered_map<PartitionID, std::vector<typename Container::value_type>>>
      clusters;

  auto status = metaClient_->partsNum(spaceId);
  if (!status.ok()) {
    return Status::Error("Space not found, spaceid: %d", spaceId);
  }
  auto numParts = status.value();
  std::unordered_map<PartitionID, HostAddr> leaders;
  for (int32_t partId = 1; partId <= numParts; ++partId) {
    auto leader = getLeader(spaceId, partId);
    if (!leader.ok()) {
      return leader.status();
    }
    leaders[partId] = std::move(leader).value();
  }
  for (auto& id : ids) {
    CHECK(!!metaClient_);
    status = metaClient_->partId(numParts, f(id));
    if (!status.ok()) {
      return status.status();
    }

    auto part = status.value();
    const auto& leader = leaders[part];
    clusters[leader][part].emplace_back(std::move(id));
  }
  return clusters;
}

template <typename ClientType, typename ClientManagerType>
StatusOr<std::unordered_map<HostAddr, std::vector<PartitionID>>>
StorageClientBase<ClientType, ClientManagerType>::getHostParts(GraphSpaceID spaceId) const {
  std::unordered_map<HostAddr, std::vector<PartitionID>> hostParts;
  auto status = metaClient_->partsNum(spaceId);
  if (!status.ok()) {
    return Status::Error("Space not found, spaceid: %d", spaceId);
  }

  auto parts = status.value();
  for (auto partId = 1; partId <= parts; partId++) {
    auto leader = getLeader(spaceId, partId);
    if (!leader.ok()) {
      return leader.status();
    }
    hostParts[leader.value()].emplace_back(partId);
  }
  return hostParts;
}

template <typename ClientType, typename ClientManagerType>
StatusOr<std::unordered_map<HostAddr, std::unordered_map<PartitionID, cpp2::ScanCursor>>>
StorageClientBase<ClientType, ClientManagerType>::getHostPartsWithCursor(
    GraphSpaceID spaceId) const {
  std::unordered_map<HostAddr, std::unordered_map<PartitionID, cpp2::ScanCursor>> hostParts;
  auto status = metaClient_->partsNum(spaceId);
  if (!status.ok()) {
    return Status::Error("Space not found, spaceid: %d", spaceId);
  }

  // TODO support cursor
  cpp2::ScanCursor c;
  auto parts = status.value();
  for (auto partId = 1; partId <= parts; partId++) {
    auto leader = getLeader(spaceId, partId);
    if (!leader.ok()) {
      return leader.status();
    }
    hostParts[leader.value()].emplace(partId, c);
  }
  return hostParts;
}

}  // namespace storage
}  // namespace nebula
#endif
