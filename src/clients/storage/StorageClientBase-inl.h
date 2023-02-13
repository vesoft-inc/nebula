/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef CLIENTS_STORAGE_STORAGECLIENTBASE_INL_H
#define CLIENTS_STORAGE_STORAGECLIENTBASE_INL_H

#include <folly/ExceptionWrapper.h>
#include <folly/Try.h>
#include <folly/futures/Future.h>

#include <optional>
#include <unordered_map>

#include "clients/storage/stats/StorageClientStats.h"
#include "common/base/Logging.h"
#include "common/base/StatusOr.h"
#include "common/datatypes/HostAddr.h"
#include "common/memory/MemoryTracker.h"
#include "common/ssl/SSLConfig.h"
#include "common/stats/StatsManager.h"
#include "common/thrift/ThriftTypes.h"
#include "common/time/WallClock.h"
#include "interface/gen-cpp2/common_types.h"

namespace nebula {
namespace storage {

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
  memory::MemoryCheckOffGuard offGuard;
  std::vector<folly::Future<StatusOr<Response>>> respFutures;
  respFutures.reserve(requests.size());

  auto hosts = std::make_shared<std::vector<HostAddr>>(requests.size());
  auto totalLatencies = std::make_shared<std::vector<int32_t>>(requests.size());

  for (const auto& req : requests) {
    auto start = time::WallClock::fastNowInMicroSec();

    size_t i = respFutures.size();
    (*hosts)[i] = req.first;
    // Future process code will be executed on the IO thread
    // Since all requests are sent using the same eventbase, all
    // then-callback will be executed on the same IO thread
    auto fut = getResponse(evb, req.first, req.second, std::move(remoteFunc))
                   .ensure([totalLatencies, i, start]() {
                     (*totalLatencies)[i] = time::WallClock::fastNowInMicroSec() - start;
                   });

    respFutures.emplace_back(std::move(fut));
  }

  return folly::collectAll(respFutures)
      .deferValue([this, requests = std::move(requests), totalLatencies, hosts](
                      std::vector<folly::Try<StatusOr<Response>>>&& resps) {
        // throw in MemoryCheckGuard verified
        memory::MemoryCheckGuard guard;
        StorageRpcResponse<Response> rpcResp(resps.size());
        for (size_t i = 0; i < resps.size(); i++) {
          const auto& host = hosts->at(i);
          folly::Try<StatusOr<Response>>& tryResp = resps[i];
          if (tryResp.hasException()) {
            std::string errMsg = tryResp.exception().what().toStdString();
            rpcResp.markFailureUnsafe();
            LOG(ERROR) << "There some RPC errors: " << errMsg;
            const auto& req = requests.at(host);
            const auto& parts = getReqPartsId(req);
            rpcResp.appendFailedPartsUnsafe(parts, nebula::cpp2::ErrorCode::E_RPC_FAILURE);
          } else {
            StatusOr<Response> status = std::move(tryResp).value();
            if (status.ok()) {
              auto resp = std::move(status).value();
              const auto& result = resp.get_result();

              if (!result.get_failed_parts().empty()) {
                rpcResp.markFailureUnsafe();
                for (auto& part : result.get_failed_parts()) {
                  rpcResp.emplaceFailedPartUnsafe(part.get_part_id(), part.get_code());
                }
              }

              // Adjust the latency
              auto latency = result.get_latency_in_us();
              rpcResp.setLatencyUnsafe(host, latency, totalLatencies->at(i));
              // Keep the response
              rpcResp.addResponseUnsafe(std::move(resp));
            } else {
              rpcResp.markFailureUnsafe();
              Status s = std::move(status).status();
              nebula::cpp2::ErrorCode errorCode =
                  s.code() == Status::Code::kGraphMemoryExceeded
                      ? nebula::cpp2::ErrorCode::E_GRAPH_MEMORY_EXCEEDED
                      : nebula::cpp2::ErrorCode::E_RPC_FAILURE;
              LOG(ERROR) << "There some RPC errors: " << s.message();
              const auto& req = requests.at(host);
              const auto& parts = getReqPartsId(req);
              rpcResp.appendFailedPartsUnsafe(parts, errorCode);
            }
          }
        }

        return rpcResp;
      });
}

template <typename ClientType, typename ClientManagerType>
template <class Request, class RemoteFunc, class Response>
folly::Future<StatusOr<Response>> StorageClientBase<ClientType, ClientManagerType>::getResponse(
    folly::EventBase* evb, const HostAddr& host, const Request& request, RemoteFunc&& remoteFunc) {
  static_assert(
      folly::isFuture<std::invoke_result_t<RemoteFunc, ClientType*, const Request&>>::value);

  stats::StatsManager::addValue(kNumRpcSentToStoraged);
  if (evb == nullptr) {
    evb = DCHECK_NOTNULL(ioThreadPool_)->getEventBase();
  }

  auto spaceId = request.get_space_id();
  return folly::via(evb)
      .thenValue([remoteFunc = std::move(remoteFunc), request, evb, host, this](auto&&) {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        // NOTE: Create new channel on each thread to avoid TIMEOUT RPC error
        auto client = clientsMan_->client(host, evb, false, FLAGS_storage_client_timeout_ms);
        // Encoding invoke Cpp2Ops::write the request to protocol is in current thread,
        // do not need to turn on in Cpp2Ops::write
        return remoteFunc(client.get(), request);
      })
      .thenValue([spaceId, this](Response&& resp) mutable -> StatusOr<Response> {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        auto& result = resp.get_result();
        for (auto& part : result.get_failed_parts()) {
          auto partId = part.get_part_id();
          auto code = part.get_code();

          VLOG(3) << "Failure! Failed part " << partId << ", failed part "
                  << static_cast<int32_t>(code);

          switch (code) {
            case nebula::cpp2::ErrorCode::E_LEADER_CHANGED: {
              auto* leader = part.get_leader();
              if (isValidHostPtr(leader)) {
                updateLeader(spaceId, partId, *leader);
              } else {
                invalidLeader(spaceId, partId);
              }
              break;
            }
            case nebula::cpp2::ErrorCode::E_PART_NOT_FOUND:
            case nebula::cpp2::ErrorCode::E_SPACE_NOT_FOUND: {
              invalidLeader(spaceId, partId);
              break;
            }
            default:
              break;
          }
        }
        return std::move(resp);
      })
      .thenError(
          folly::tag_t<std::bad_alloc>{},
          [](const std::bad_alloc&) {
            return folly::makeFuture<StatusOr<Response>>(Status::GraphMemoryExceeded(
                "(%d)", static_cast<int32_t>(nebula::cpp2::ErrorCode::E_GRAPH_MEMORY_EXCEEDED)));
          })
      .thenError([request, host, spaceId, this](
                     folly::exception_wrapper&& exWrapper) mutable -> StatusOr<Response> {
        stats::StatsManager::addValue(kNumRpcSentToStoragedFailed);

        using TransportException = apache::thrift::transport::TTransportException;
        auto ex = exWrapper.get_exception<TransportException>();
        if (ex) {
          if (ex->getType() == TransportException::TIMED_OUT) {
            LOG(ERROR) << "Request to " << host << " time out: " << ex->what();
            return Status::Error("RPC failure in StorageClient with timeout: %s", ex->what());
          } else {
            LOG(ERROR) << "Request to " << host << " failed: " << ex->what();
            return Status::Error("RPC failure in StorageClient: %s", ex->what());
          }
        } else {
          auto partsId = getReqPartsId(request);
          invalidLeader(spaceId, partsId);
          LOG(ERROR) << "Request to " << host << " failed.";
          return Status::Error("RPC failure in StorageClient.");
        }
      });
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
