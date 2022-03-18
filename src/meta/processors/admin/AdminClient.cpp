/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/admin/AdminClient.h"

#include <thrift/lib/cpp/util/EnumUtils.h>

#include "common/utils/MetaKeyUtils.h"
#include "common/utils/Utils.h"
#include "kvstore/Part.h"
#include "meta/ActiveHostsMan.h"
#include "meta/processors/Common.h"

DEFINE_int32(max_retry_times_admin_op, 30, "max retry times for admin request!");

namespace nebula {
namespace meta {

folly::Future<Status> AdminClient::transLeader(GraphSpaceID spaceId,
                                               PartitionID partId,
                                               const HostAddr& src,
                                               const HostAddr& dst) {
  storage::cpp2::TransLeaderReq req;
  req.space_id_ref() = spaceId;
  req.part_id_ref() = partId;
  auto partHosts = getPeers(spaceId, partId);
  if (!nebula::ok(partHosts)) {
    LOG(INFO) << "Get peers failed: "
              << apache::thrift::util::enumNameSafe(nebula::error(partHosts));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(partHosts));
  auto it = std::find(peers.begin(), peers.end(), src);
  if (it == peers.end()) {
    return Status::PartNotFound();
  }
  if (peers.size() == 1 && peers.front() == src) {
    // if there is only one replica, skip transfer leader phase
    return Status::OK();
  }
  auto target = dst;
  if (dst == kRandomPeer) {
    for (auto& p : peers) {
      if (p != src) {
        auto retCode = ActiveHostsMan::isLived(kv_, p);
        if (nebula::ok(retCode) && nebula::value(retCode)) {
          target = p;
          break;
        }
      }
    }
  }
  req.new_leader_ref() = std::move(target);
  return getResponseFromPart(
      Utils::getAdminAddrFromStoreAddr(src),
      std::move(req),
      [](auto client, auto request) { return client->future_transLeader(request); },
      [](auto&& resp) -> Status {
        switch (resp.get_code()) {
          case nebula::cpp2::ErrorCode::SUCCEEDED:
          case nebula::cpp2::ErrorCode::E_LEADER_CHANGED: {
            return Status::OK();
          }
          case nebula::cpp2::ErrorCode::E_PART_NOT_FOUND: {
            return Status::PartNotFound();
          }
          default:
            return Status::Error("Transfer leader failed: %s",
                                 apache::thrift::util::enumNameSafe(resp.get_code()).c_str());
        }
      });
}

folly::Future<Status> AdminClient::addPart(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           const HostAddr& host,
                                           bool asLearner) {
  storage::cpp2::AddPartReq req;
  req.space_id_ref() = spaceId;
  req.part_id_ref() = partId;
  req.as_learner_ref() = asLearner;
  auto partHosts = getPeers(spaceId, partId);
  if (!nebula::ok(partHosts)) {
    LOG(INFO) << "Get peers failed: "
              << apache::thrift::util::enumNameSafe(nebula::error(partHosts));
    return Status::Error("Get peers failed");
  }

  req.peers_ref() = std::move(nebula::value(partHosts));
  return getResponseFromPart(
      Utils::getAdminAddrFromStoreAddr(host),
      std::move(req),
      [](auto client, auto request) { return client->future_addPart(request); },
      [](auto&& resp) -> Status {
        if (resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
          return Status::OK();
        } else {
          return Status::Error("Add part failed: %s",
                               apache::thrift::util::enumNameSafe(resp.get_code()).c_str());
        }
      });
}

folly::Future<Status> AdminClient::addLearner(GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const HostAddr& learner) {
  storage::cpp2::AddLearnerReq req;
  req.space_id_ref() = spaceId;
  req.part_id_ref() = partId;
  req.learner_ref() = learner;
  auto partHosts = getPeers(spaceId, partId);
  if (!nebula::ok(partHosts)) {
    LOG(INFO) << "Get peers failed: "
              << apache::thrift::util::enumNameSafe(nebula::error(partHosts));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(partHosts));
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  getResponseFromLeader(
      getAdminAddrFromPeers(peers),
      0,
      std::move(req),
      [](auto client, auto request) { return client->future_addLearner(request); },
      0,
      std::move(pro),
      FLAGS_max_retry_times_admin_op);
  return f;
}

folly::Future<Status> AdminClient::waitingForCatchUpData(GraphSpaceID spaceId,
                                                         PartitionID partId,
                                                         const HostAddr& target) {
  storage::cpp2::CatchUpDataReq req;
  req.space_id_ref() = spaceId;
  req.part_id_ref() = partId;
  req.target_ref() = target;
  auto partHosts = getPeers(spaceId, partId);
  if (!nebula::ok(partHosts)) {
    LOG(INFO) << "Get peers failed: "
              << apache::thrift::util::enumNameSafe(nebula::error(partHosts));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(partHosts));
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  getResponseFromLeader(
      getAdminAddrFromPeers(peers),
      0,
      std::move(req),
      [](auto client, auto request) { return client->future_waitingForCatchUpData(request); },
      0,
      std::move(pro),
      FLAGS_max_retry_times_admin_op);
  return f;
}

folly::Future<Status> AdminClient::memberChange(GraphSpaceID spaceId,
                                                PartitionID partId,
                                                const HostAddr& peer,
                                                bool added) {
  storage::cpp2::MemberChangeReq req;
  req.space_id_ref() = spaceId;
  req.part_id_ref() = partId;
  req.add_ref() = added;
  req.peer_ref() = peer;
  auto partHosts = getPeers(spaceId, partId);
  if (!nebula::ok(partHosts)) {
    LOG(INFO) << "Get peers failed: "
              << apache::thrift::util::enumNameSafe(nebula::error(partHosts));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(partHosts));
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  getResponseFromLeader(
      getAdminAddrFromPeers(peers),
      0,
      std::move(req),
      [](auto client, auto request) { return client->future_memberChange(request); },
      0,
      std::move(pro),
      FLAGS_max_retry_times_admin_op);
  return f;
}

folly::Future<Status> AdminClient::updateMeta(GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const HostAddr& src,
                                              const HostAddr& dst) {
  CHECK_NOTNULL(kv_);
  auto ret = getPeers(spaceId, partId);
  auto partHosts = getPeers(spaceId, partId);
  if (!nebula::ok(partHosts)) {
    LOG(INFO) << "Get peers failed: "
              << apache::thrift::util::enumNameSafe(nebula::error(partHosts));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(partHosts));
  auto strHosts = [](const std::vector<HostAddr>& hosts) -> std::string {
    std::stringstream peersStr;
    for (auto& h : hosts) {
      peersStr << h << ",";
    }
    return peersStr.str();
  };
  LOG(INFO) << "[space:" << spaceId << ", part:" << partId << "] Update original peers "
            << strHosts(peers) << " remove " << src << ", add " << dst;
  auto it = std::find(peers.begin(), peers.end(), src);
  if (it == peers.end()) {
    LOG(INFO) << "src " << src << " has been removed in [" << spaceId << ", " << partId << "]";
  } else {
    LOG(INFO) << "remove [" << spaceId << ", " << partId << "] from " << src;
    peers.erase(it);
  }

  if (std::find(peers.begin(), peers.end(), dst) != peers.end()) {
    LOG(INFO) << "dst " << dst << " has been added to [" << spaceId << ", " << partId << "]";
  } else {
    LOG(INFO) << "add [" << spaceId << ", " << partId << "] to " << dst;
    peers.emplace_back(dst);
  }

  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  std::vector<kvstore::KV> data;
  data.emplace_back(MetaKeyUtils::partKey(spaceId, partId), MetaKeyUtils::partVal(peers));
  kv_->asyncMultiPut(
      kDefaultSpaceId,
      kDefaultPartId,
      std::move(data),
      [this, p = std::move(pro)](nebula::cpp2::ErrorCode code) mutable {
        folly::via(ioThreadPool_.get(), [code, p = std::move(p)]() mutable {
          if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
            p.setValue(Status::OK());
          } else {
            p.setValue(Status::Error("Access kv failed, code: %s",
                                     apache::thrift::util::enumNameSafe(code).c_str()));
          }
        });
      });
  return f;
}

folly::Future<Status> AdminClient::removePart(GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const HostAddr& host) {
  storage::cpp2::RemovePartReq req;
  req.space_id_ref() = spaceId;
  req.part_id_ref() = partId;
  return getResponseFromPart(
      Utils::getAdminAddrFromStoreAddr(host),
      std::move(req),
      [](auto client, auto request) { return client->future_removePart(request); },
      [](auto&& resp) -> Status {
        if (resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
          return Status::OK();
        } else {
          return Status::Error("Remove part failed, code: %s",
                               apache::thrift::util::enumNameSafe(resp.get_code()).c_str());
        }
      });
}

folly::Future<Status> AdminClient::checkPeers(GraphSpaceID spaceId, PartitionID partId) {
  storage::cpp2::CheckPeersReq req;
  req.space_id_ref() = spaceId;
  req.part_id_ref() = partId;
  auto partHosts = getPeers(spaceId, partId);
  if (!nebula::ok(partHosts)) {
    LOG(INFO) << "Get peers failed: "
              << apache::thrift::util::enumNameSafe(nebula::error(partHosts));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(partHosts));
  req.peers_ref() = peers;
  folly::Promise<Status> pro;
  auto fut = pro.getFuture();
  std::vector<folly::Future<Status>> futures;
  for (auto& p : peers) {
    auto ret = ActiveHostsMan::isLived(kv_, p);
    if (!nebula::ok(ret)) {
      auto retCode = nebula::error(ret);
      LOG(INFO) << "Get active host failed, error: " << apache::thrift::util::enumNameSafe(retCode);
      return Status::Error("Get active host failed");
    } else {
      auto isLive = nebula::value(ret);
      if (!isLive) {
        LOG(INFO) << "[" << spaceId << ":" << partId << "], Skip the dead host " << p;
        continue;
      }
    }
    auto f = getResponseFromPart(
        Utils::getAdminAddrFromStoreAddr(p),
        req,
        [](auto client, auto request) { return client->future_checkPeers(request); },
        [](auto&& resp) -> Status {
          if (resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
            return Status::OK();
          } else {
            return Status::Error("Check peers failed, code: %s",
                                 apache::thrift::util::enumNameSafe(resp.get_code()).c_str());
          }
        });
    futures.emplace_back(std::move(f));
  }

  folly::collectAll(std::move(futures))
      .via(ioThreadPool_.get())
      .thenTry([p = std::move(pro)](auto&& t) mutable {
        if (t.hasException()) {
          p.setValue(Status::Error("Check failed!"));
        } else {
          auto v = std::move(t).value();
          for (auto& resp : v) {
            // The exception has been catched inside getResponseFromPart.
            CHECK(!resp.hasException());
            auto st = std::move(resp).value();
            if (!st.ok()) {
              p.setValue(st);
              return;
            }
          }
          p.setValue(Status::OK());
        }
      });
  return fut;
}

folly::Future<nebula::cpp2::ErrorCode> AdminClient::clearSpace(GraphSpaceID spaceId,
                                                               const std::vector<HostAddr>& hosts) {
  folly::Promise<nebula::cpp2::ErrorCode> promise;
  auto f = promise.getFuture();

  std::vector<folly::Future<StatusOr<nebula::cpp2::ErrorCode>>> futures;
  for (auto& host : hosts) {
    folly::Promise<StatusOr<nebula::cpp2::ErrorCode>> pro;
    futures.emplace_back(pro.getFuture());

    storage::cpp2::ClearSpaceReq req;
    req.space_id_ref() = spaceId;
    getResponseFromHost(
        Utils::getAdminAddrFromStoreAddr(host),
        std::move(req),
        [](auto client, auto request) { return client->future_clearSpace(request); },
        [](auto&& resp) -> nebula::cpp2::ErrorCode { return resp.get_code(); },
        std::move(pro));
  }

  folly::collectAll(std::move(futures))
      .via(ioThreadPool_.get())
      .thenTry([pro = std::move(promise)](auto&& futureRet) mutable {
        if (futureRet.hasException()) {
          pro.setValue(nebula::cpp2::ErrorCode::E_RPC_FAILURE);
        } else {
          auto vec = std::move(futureRet).value();
          bool isAllOk = true;
          for (auto& v : vec) {
            auto resp = std::move(v).value();
            if (!resp.ok()) {
              pro.setValue(nebula::cpp2::ErrorCode::E_RPC_FAILURE);
              isAllOk = false;
              break;
            }
          }
          if (isAllOk) {
            pro.setValue(nebula::cpp2::ErrorCode::SUCCEEDED);
          }
        }
      });
  return f;
}

template <typename Request, typename RemoteFunc, typename RespGenerator>
folly::Future<Status> AdminClient::getResponseFromPart(const HostAddr& host,
                                                       Request req,
                                                       RemoteFunc remoteFunc,
                                                       RespGenerator respGen) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  auto* evb = ioThreadPool_->getEventBase();
  auto partId = req.get_part_id();
  folly::via(
      evb,
      [evb,
       pro = std::move(pro),
       host,
       req = std::move(req),
       partId,
       remoteFunc = std::move(remoteFunc),
       respGen = std::move(respGen),
       this]() mutable {
        auto client = clientsMan_->client(host, evb);
        remoteFunc(client, std::move(req))
            .via(evb)
            .then([p = std::move(pro), partId, respGen = std::move(respGen), host](
                      folly::Try<storage::cpp2::AdminExecResp>&& t) mutable {
              // exception occurred during RPC
              if (t.hasException()) {
                p.setValue(Status::Error(folly::stringPrintf("[%s] RPC failure in AdminClient: %s",
                                                             host.toString().c_str(),
                                                             t.exception().what().c_str())));
                return;
              }
              auto&& result = std::move(t).value().get_result();
              if (result.get_failed_parts().empty()) {
                storage::cpp2::PartitionResult resultCode;
                resultCode.code_ref() = nebula::cpp2::ErrorCode::SUCCEEDED;
                resultCode.part_id_ref() = partId;
                p.setValue(respGen(resultCode));
              } else {
                auto resp = result.get_failed_parts().front();
                p.setValue(respGen(std::move(resp)));
              }
            });
      });
  return f;
}

template <typename Request,
          typename RemoteFunc,
          typename RespGenerator,
          typename RpcResponse,
          typename Response>
void AdminClient::getResponseFromHost(const HostAddr& host,
                                      Request req,
                                      RemoteFunc remoteFunc,
                                      RespGenerator respGen,
                                      folly::Promise<StatusOr<Response>> pro) {
  auto* evb = ioThreadPool_->getEventBase();
  folly::via(
      evb,
      [evb,
       pro = std::move(pro),
       host,
       req = std::move(req),
       remoteFunc = std::move(remoteFunc),
       respGen = std::move(respGen),
       this]() mutable {
        auto client = clientsMan_->client(host, evb);
        remoteFunc(client, std::move(req))
            .via(evb)
            .then([p = std::move(pro), respGen = std::move(respGen), host](
                      folly::Try<RpcResponse>&& t) mutable {
              // exception occurred during RPC
              if (t.hasException()) {
                p.setValue(Status::Error(folly::stringPrintf("[%s] RPC failure in AdminClient: %s",
                                                             host.toString().c_str(),
                                                             t.exception().what().c_str())));
                return;
              }
              auto&& result = std::move(t).value();
              if (result.get_code() != nebula::cpp2::ErrorCode::SUCCEEDED) {
                p.setValue(
                    Status::Error("Operattion failed in AdminClient: %s",
                                  apache::thrift::util::enumNameSafe(result.get_code()).c_str()));
              } else {
                p.setValue(respGen(std::move(result)));
              }
            });
      });
}

template <typename Request, typename RemoteFunc>
void AdminClient::getResponseFromLeader(std::vector<HostAddr> hosts,
                                        int32_t index,
                                        Request req,
                                        RemoteFunc remoteFunc,
                                        int32_t retry,
                                        folly::Promise<Status> pro,
                                        int32_t retryLimit) {
  auto* evb = ioThreadPool_->getEventBase();
  CHECK_GE(index, 0);
  CHECK_LT(index, hosts.size());
  folly::via(evb,
             [evb,
              hosts = std::move(hosts),
              index,
              req = std::move(req),
              remoteFunc = std::move(remoteFunc),
              retry,
              pro = std::move(pro),
              retryLimit,
              this]() mutable {
               auto client = clientsMan_->client(hosts[index], evb);
               remoteFunc(client, req)
                   .via(evb)
                   .then([p = std::move(pro),
                          hosts = std::move(hosts),
                          index,
                          req = std::move(req),
                          remoteFunc = std::move(remoteFunc),
                          retry,
                          retryLimit,
                          this](folly::Try<storage::cpp2::AdminExecResp>&& t) mutable {
                     // exception occurred during RPC
                     if (t.hasException()) {
                       if (retry < retryLimit) {
                         LOG(INFO) << "Rpc failure to " << hosts[index] << ", retry " << retry
                                   << ", limit " << retryLimit << ", error: " << t.exception();
                         index = (index + 1) % hosts.size();
                         getResponseFromLeader(std::move(hosts),
                                               index,
                                               std::move(req),
                                               std::move(remoteFunc),
                                               retry + 1,
                                               std::move(p),
                                               retryLimit);
                         return;
                       }
                       p.setValue(Status::Error(folly::stringPrintf(
                           "RPC failure in AdminClient: %s", t.exception().what().c_str())));
                       return;
                     }
                     auto&& adminResp = std::move(t.value());
                     if (adminResp.result.get_failed_parts().empty()) {
                       // succeeded
                       p.setValue(Status::OK());
                       return;
                     }
                     auto resp = adminResp.result.get_failed_parts().front();
                     switch (resp.get_code()) {
                       case nebula::cpp2::ErrorCode::E_LEADER_CHANGED: {
                         // storage will return the address of data service ip:port
                         if (retry < retryLimit) {
                           HostAddr leader("", 0);
                           if (resp.get_leader() != nullptr) {
                             leader = *resp.get_leader();
                           }
                           if (leader == HostAddr("", 0)) {
                             usleep(1000 * 50);
                             LOG(INFO) << "The leader is in election"
                                       << ", retry " << retry << ", limit " << retryLimit;
                             index = (index + 1) % hosts.size();
                             getResponseFromLeader(std::move(hosts),
                                                   index,
                                                   std::move(req),
                                                   std::move(remoteFunc),
                                                   retry + 1,
                                                   std::move(p),
                                                   retryLimit);
                             return;
                           }
                           // convert to admin addr
                           leader = Utils::getAdminAddrFromStoreAddr(leader);
                           int32_t leaderIndex = 0;
                           for (auto& h : hosts) {
                             if (h == leader) {
                               break;
                             }
                             leaderIndex++;
                           }
                           if (leaderIndex == (int32_t)hosts.size()) {
                             // In some cases (e.g. balance task is failed in member
                             // change remove phase, and the new host is elected as
                             // leader somehow), the peers of this partition in meta
                             // doesn't include new host, which will make this task
                             // failed forever.
                             index = leaderIndex;
                             hosts.emplace_back(leader);
                           }
                           LOG(INFO)
                               << "Return leader change from " << hosts[index] << ", new leader is "
                               << leader << ", retry " << retry << ", limit " << retryLimit;
                           CHECK_LT(leaderIndex, hosts.size());
                           getResponseFromLeader(std::move(hosts),
                                                 leaderIndex,
                                                 std::move(req),
                                                 std::move(remoteFunc),
                                                 retry + 1,
                                                 std::move(p),
                                                 retryLimit);
                           return;
                         }
                         p.setValue(Status::Error("Leader changed!"));
                         return;
                       }
                       default: {
                         if (retry < retryLimit) {
                           LOG(INFO) << "Unknown code " << static_cast<int32_t>(resp.get_code())
                                     << " from " << hosts[index] << ", retry " << retry
                                     << ", limit " << retryLimit;
                           index = (index + 1) % hosts.size();
                           getResponseFromLeader(std::move(hosts),
                                                 index,
                                                 std::move(req),
                                                 std::move(remoteFunc),
                                                 retry + 1,
                                                 std::move(p),
                                                 retryLimit);
                           return;
                         }
                         p.setValue(Status::Error("Unknown code %d",
                                                  static_cast<int32_t>(resp.get_code())));
                         return;
                       }
                     }
                   });  // then
             });        // via
}

// todo(doodle): add related locks
ErrorOr<nebula::cpp2::ErrorCode, std::vector<HostAddr>> AdminClient::getPeers(GraphSpaceID spaceId,
                                                                              PartitionID partId) {
  CHECK_NOTNULL(kv_);
  auto partKey = MetaKeyUtils::partKey(spaceId, partId);
  std::string value;
  auto code = kv_->get(kDefaultSpaceId, kDefaultPartId, partKey, &value);
  if (code == nebula::cpp2::ErrorCode::SUCCEEDED) {
    return MetaKeyUtils::parsePartVal(value);
  }
  return code;
}

std::vector<HostAddr> AdminClient::getAdminAddrFromPeers(const std::vector<HostAddr>& peers) {
  std::vector<HostAddr> adminHosts;
  adminHosts.resize(peers.size());
  std::transform(peers.begin(), peers.end(), adminHosts.begin(), [](const auto& h) {
    return Utils::getAdminAddrFromStoreAddr(h);
  });
  return adminHosts;
}

folly::Future<StatusOr<std::unordered_map<GraphSpaceID, std::vector<PartitionID>>>>
AdminClient::getLeaderDist(const HostAddr& host) {
  folly::Promise<StatusOr<std::unordered_map<GraphSpaceID, std::vector<PartitionID>>>> pro;
  auto f = pro.getFuture();

  auto adminAddr = Utils::getAdminAddrFromStoreAddr(host);
  storage::cpp2::GetLeaderReq req;
  getResponseFromHost(
      adminAddr,
      std::move(req),
      [](auto client, auto request) { return client->future_getLeaderParts(request); },
      [](storage::cpp2::GetLeaderPartsResp&& resp) -> decltype(auto) {
        return resp.get_leader_parts();
      },
      std::move(pro));
  return f;
}

folly::Future<Status> AdminClient::getLeaderDist(HostLeaderMap* result) {
  folly::Promise<Status> promise;
  auto future = promise.getFuture();
  auto allHostsRet = ActiveHostsMan::getActiveHosts(kv_);
  if (!nebula::ok(allHostsRet)) {
    promise.setValue(Status::Error("Get leader failed"));
    return future;
  }

  auto allHosts = nebula::value(allHostsRet);
  std::vector<folly::Future<StatusOr<std::unordered_map<GraphSpaceID, std::vector<PartitionID>>>>>
      hostFutures;
  for (const auto& h : allHosts) {
    hostFutures.emplace_back(getLeaderDist(h));
  }

  folly::collectAll(std::move(hostFutures))
      .via(ioThreadPool_.get())
      .thenValue([p = std::move(promise), result, allHosts](auto&& tries) mutable {
        for (size_t i = 0; i < allHosts.size(); i++) {
          CHECK(!tries[i].hasException());
          auto hostLeader = std::move(tries[i].value());
          if (!hostLeader.ok()) {
            continue;
          }
          result->emplace(allHosts[i], hostLeader.value());
        }
        p.setValue(Status::OK());
      })
      .thenError([p = std::move(promise)](auto&& e) mutable {
        p.setValue(Status::Error("Get leader failed, %s", e.what().c_str()));
      });

  return future;
}

folly::Future<StatusOr<cpp2::HostBackupInfo>> AdminClient::createSnapshot(
    const std::set<GraphSpaceID>& spaceIds, const std::string& name, const HostAddr& host) {
  folly::Promise<StatusOr<cpp2::HostBackupInfo>> pro;
  auto f = pro.getFuture();

  auto adminAddr = Utils::getAdminAddrFromStoreAddr(host);
  storage::cpp2::CreateCPRequest req;
  std::vector<GraphSpaceID> idList(spaceIds.begin(), spaceIds.end());
  req.space_ids_ref() = idList;
  req.name_ref() = name;
  getResponseFromHost(
      adminAddr,
      std::move(req),
      [](auto client, auto request) { return client->future_createCheckpoint(request); },
      [host](storage::cpp2::CreateCPResp&& resp) -> StatusOr<cpp2::HostBackupInfo> {
        if (resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
          cpp2::HostBackupInfo hostBackupInfo;
          hostBackupInfo.host_ref() = host;
          hostBackupInfo.checkpoints_ref() = std::move(resp.get_info());
          return hostBackupInfo;
        } else {
          return Status::Error("Create snapshot failed: %s",
                               apache::thrift::util::enumNameSafe(resp.get_code()).c_str());
        }
      },
      std::move(pro));
  return f;
}

folly::Future<StatusOr<bool>> AdminClient::dropSnapshot(const std::set<GraphSpaceID>& spaceIds,
                                                        const std::string& name,
                                                        const HostAddr& host) {
  folly::Promise<StatusOr<bool>> pro;
  auto f = pro.getFuture();
  auto adminAddr = Utils::getAdminAddrFromStoreAddr(host);
  std::vector<GraphSpaceID> idList(spaceIds.begin(), spaceIds.end());
  storage::cpp2::DropCPRequest req;
  req.space_ids_ref() = idList;
  req.name_ref() = name;
  getResponseFromHost(
      adminAddr,
      std::move(req),
      [](auto client, auto request) { return client->future_dropCheckpoint(request); },
      [](storage::cpp2::DropCPResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(pro));
  return f;
}

folly::Future<StatusOr<bool>> AdminClient::blockingWrites(const std::set<GraphSpaceID>& spaceIds,
                                                          storage::cpp2::EngineSignType sign,
                                                          const HostAddr& host) {
  folly::Promise<StatusOr<bool>> pro;
  auto f = pro.getFuture();
  auto adminAddr = Utils::getAdminAddrFromStoreAddr(host);
  storage::cpp2::BlockingSignRequest req;
  std::vector<GraphSpaceID> idList(spaceIds.begin(), spaceIds.end());
  req.space_ids_ref() = idList;
  req.sign_ref() = sign;
  getResponseFromHost(
      adminAddr,
      std::move(req),
      [](auto client, auto request) { return client->future_blockingWrites(request); },
      [](storage::cpp2::BlockingSignResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(pro));
  return f;
}

folly::Future<StatusOr<bool>> AdminClient::addTask(
    cpp2::JobType type,
    int32_t jobId,
    int32_t taskId,
    GraphSpaceID spaceId,
    const HostAddr& host,
    const std::vector<std::string>& taskSpecificParas,
    std::vector<PartitionID> parts) {
  folly::Promise<StatusOr<bool>> pro;
  auto f = pro.getFuture();
  auto adminAddr = Utils::getAdminAddrFromStoreAddr(host);

  storage::cpp2::AddTaskRequest req;
  req.job_type_ref() = type;
  req.job_id_ref() = jobId;
  req.task_id_ref() = taskId;

  storage::cpp2::TaskPara para;
  para.space_id_ref() = spaceId;
  para.parts_ref() = std::move(parts);
  para.task_specific_paras_ref() = taskSpecificParas;
  req.para_ref() = std::move(para);

  getResponseFromHost(
      adminAddr,
      std::move(req),
      [](auto client, auto request) { return client->future_addAdminTask(request); },
      [](storage::cpp2::AddTaskResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(pro));
  return f;
}

folly::Future<StatusOr<bool>> AdminClient::stopTask(const HostAddr& host,
                                                    int32_t jobId,
                                                    int32_t taskId) {
  folly::Promise<StatusOr<bool>> pro;
  auto f = pro.getFuture();

  auto adminAddr = Utils::getAdminAddrFromStoreAddr(host);
  storage::cpp2::StopTaskRequest req;
  req.job_id_ref() = jobId;
  req.task_id_ref() = taskId;
  getResponseFromHost(
      adminAddr,
      std::move(req),
      [](auto client, auto request) { return client->future_stopAdminTask(request); },
      [](storage::cpp2::StopTaskResp&& resp) -> bool {
        return resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED;
      },
      std::move(pro));
  return f;
}

}  // namespace meta
}  // namespace nebula
