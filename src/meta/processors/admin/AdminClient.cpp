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
                                               const HostAddr& leader,
                                               const HostAddr& dst) {
  storage::cpp2::TransLeaderReq req;
  req.space_id_ref() = spaceId;
  req.part_id_ref() = partId;
  auto ret = getPeers(spaceId, partId);
  if (!nebula::ok(ret)) {
    LOG(INFO) << "Get peers failed: " << static_cast<int32_t>(nebula::error(ret));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(ret));
  auto it = std::find(peers.begin(), peers.end(), leader);
  if (it == peers.end()) {
    return Status::PartNotFound();
  }
  if (peers.size() == 1 && peers.front() == leader) {
    // if there is only one replica, skip transfer leader phase
    return Status::OK();
  }
  auto target = dst;
  if (dst == kRandomPeer) {
    for (auto& p : peers) {
      if (p != leader) {
        auto retCode = ActiveHostsMan::isLived(kv_, p);
        if (nebula::ok(retCode) && nebula::value(retCode)) {
          target = p;
          break;
        }
      }
    }
  }
  req.new_leader_ref() = std::move(target);
  return getResponse(
      Utils::getAdminAddrFromStoreAddr(leader),
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
            return Status::Error("Unknown code %d", static_cast<int32_t>(resp.get_code()));
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
  auto ret = getPeers(spaceId, partId);
  if (!nebula::ok(ret)) {
    LOG(INFO) << "Get peers failed: " << static_cast<int32_t>(nebula::error(ret));
    return Status::Error("Get peers failed");
  }

  req.peers_ref() = std::move(nebula::value(ret));
  return getResponse(
      Utils::getAdminAddrFromStoreAddr(host),
      std::move(req),
      [](auto client, auto request) { return client->future_addPart(request); },
      [](auto&& resp) -> Status {
        if (resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
          return Status::OK();
        } else {
          return Status::Error("Add part failed! code=%d", static_cast<int32_t>(resp.get_code()));
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
  auto ret = getPeers(spaceId, partId);
  if (!nebula::ok(ret)) {
    LOG(INFO) << "Get peers failed: " << static_cast<int32_t>(nebula::error(ret));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(ret));
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  getResponse(
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
  auto ret = getPeers(spaceId, partId);
  if (!nebula::ok(ret)) {
    LOG(INFO) << "Get peers failed: " << static_cast<int32_t>(nebula::error(ret));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(ret));
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  getResponse(
      getAdminAddrFromPeers(peers),
      0,
      std::move(req),
      [](auto client, auto request) { return client->future_waitingForCatchUpData(request); },
      0,
      std::move(pro),
      3);
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
  auto ret = getPeers(spaceId, partId);
  if (!nebula::ok(ret)) {
    LOG(INFO) << "Get peers failed: " << static_cast<int32_t>(nebula::error(ret));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(ret));
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  getResponse(
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
  if (!nebula::ok(ret)) {
    LOG(INFO) << "Get peers failed: " << static_cast<int32_t>(nebula::error(ret));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(ret));
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
  return getResponse(
      Utils::getAdminAddrFromStoreAddr(host),
      std::move(req),
      [](auto client, auto request) { return client->future_removePart(request); },
      [](auto&& resp) -> Status {
        if (resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
          return Status::OK();
        } else {
          return Status::Error("Remove part failed! code=%d",
                               static_cast<int32_t>(resp.get_code()));
        }
      });
}

folly::Future<Status> AdminClient::checkPeers(GraphSpaceID spaceId, PartitionID partId) {
  storage::cpp2::CheckPeersReq req;
  req.space_id_ref() = spaceId;
  req.part_id_ref() = partId;
  auto peerRet = getPeers(spaceId, partId);
  if (!nebula::ok(peerRet)) {
    LOG(INFO) << "Get peers failed: " << static_cast<int32_t>(nebula::error(peerRet));
    return Status::Error("Get peers failed");
  }

  auto peers = std::move(nebula::value(peerRet));
  req.peers_ref() = peers;
  folly::Promise<Status> pro;
  auto fut = pro.getFuture();
  std::vector<folly::Future<Status>> futures;
  for (auto& p : peers) {
    auto ret = ActiveHostsMan::isLived(kv_, p);
    if (!nebula::ok(ret)) {
      auto retCode = nebula::error(ret);
      LOG(INFO) << "Get active host failed, error: " << static_cast<int32_t>(retCode);
      return Status::Error("Get peers failed");
    } else {
      auto isLive = nebula::value(ret);
      if (!isLive) {
        LOG(INFO) << "[" << spaceId << ":" << partId << "], Skip the dead host " << p;
        continue;
      }
    }
    auto f = getResponse(
        Utils::getAdminAddrFromStoreAddr(p),
        req,
        [](auto client, auto request) { return client->future_checkPeers(request); },
        [](auto&& resp) -> Status {
          if (resp.get_code() == nebula::cpp2::ErrorCode::SUCCEEDED) {
            return Status::OK();
          } else {
            return Status::Error("Check peers failed! code=%d",
                                 static_cast<int32_t>(resp.get_code()));
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
            // The exception has been catched inside getResponse.
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

template <typename Request, typename RemoteFunc, typename RespGenerator>
folly::Future<Status> AdminClient::getResponse(const HostAddr& host,
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

template <typename Request, typename RemoteFunc>
void AdminClient::getResponse(std::vector<HostAddr> hosts,
                              int32_t index,
                              Request req,
                              RemoteFunc remoteFunc,
                              int32_t retry,
                              folly::Promise<Status> pro,
                              int32_t retryLimit,
                              HandleResultOpt respGen) {
  auto* evb = ioThreadPool_->getEventBase();
  CHECK_GE(index, 0);
  CHECK_LT(index, hosts.size());
  folly::via(evb,
             [evb,
              hosts = std::move(hosts),
              index,
              req = std::move(req),
              remoteFunc = std::move(remoteFunc),
              respGen = std::move(respGen),
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
                          respGen = std::move(respGen),
                          retry,
                          retryLimit,
                          this](folly::Try<storage::cpp2::AdminExecResp>&& t) mutable {
                     // exception occurred during RPC
                     if (t.hasException()) {
                       if (retry < retryLimit) {
                         LOG(INFO) << "Rpc failure to " << hosts[index] << ", retry " << retry
                                   << ", limit " << retryLimit << ", error: " << t.exception();
                         index = (index + 1) % hosts.size();
                         getResponse(std::move(hosts),
                                     index,
                                     std::move(req),
                                     std::move(remoteFunc),
                                     retry + 1,
                                     std::move(p),
                                     retryLimit,
                                     std::move(respGen));
                         return;
                       }
                       p.setValue(Status::Error(folly::stringPrintf(
                           "RPC failure in AdminClient: %s", t.exception().what().c_str())));
                       return;
                     }
                     auto&& adminResp = std::move(t.value());
                     if (adminResp.result.get_failed_parts().empty()) {
                       // succeeded
                       if (respGen != folly::none) {
                         auto val = respGen.value();
                         val(std::move(adminResp));
                       }
                       p.setValue(Status::OK());
                       return;
                     }
                     auto resp = adminResp.result.get_failed_parts().front();
                     switch (resp.get_code()) {
                       case nebula::cpp2::ErrorCode::E_LEADER_CHANGED: {
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
                             getResponse(std::move(hosts),
                                         index,
                                         std::move(req),
                                         std::move(remoteFunc),
                                         retry + 1,
                                         std::move(p),
                                         retryLimit,
                                         std::move(respGen));
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
                           getResponse(std::move(hosts),
                                       leaderIndex,
                                       std::move(req),
                                       std::move(remoteFunc),
                                       retry + 1,
                                       std::move(p),
                                       retryLimit,
                                       std::move(respGen));
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
                           getResponse(std::move(hosts),
                                       index,
                                       std::move(req),
                                       std::move(remoteFunc),
                                       retry + 1,
                                       std::move(p),
                                       retryLimit,
                                       std::move(respGen));
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

void AdminClient::getLeaderDist(const HostAddr& host,
                                folly::Promise<StatusOr<storage::cpp2::GetLeaderPartsResp>>&& pro,
                                int32_t retry,
                                int32_t retryLimit) {
  auto* evb = ioThreadPool_->getEventBase();
  folly::via(evb, [evb, host, pro = std::move(pro), retry, retryLimit, this]() mutable {
    storage::cpp2::GetLeaderReq req;
    auto client = clientsMan_->client(host, evb);
    client->future_getLeaderParts(std::move(req))
        .via(evb)
        .then([pro = std::move(pro), host, retry, retryLimit, this](
                  folly::Try<storage::cpp2::GetLeaderPartsResp>&& t) mutable {
          if (t.hasException()) {
            LOG(INFO) << folly::stringPrintf("RPC failure in AdminClient: %s",
                                             t.exception().what().c_str());
            if (retry < retryLimit) {
              usleep(1000 * 50);
              getLeaderDist(
                  Utils::getAdminAddrFromStoreAddr(host), std::move(pro), retry + 1, retryLimit);
            } else {
              pro.setValue(Status::Error("RPC failure in AdminClient"));
            }
            return;
          }
          auto&& resp = std::move(t).value();
          LOG(INFO) << "Get leader for host " << host;
          pro.setValue(std::move(resp));
        });
  });
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
  std::vector<folly::Future<StatusOr<storage::cpp2::GetLeaderPartsResp>>> hostFutures;
  for (const auto& h : allHosts) {
    folly::Promise<StatusOr<storage::cpp2::GetLeaderPartsResp>> pro;
    auto fut = pro.getFuture();
    getLeaderDist(Utils::getAdminAddrFromStoreAddr(h), std::move(pro), 0, 3);
    hostFutures.emplace_back(std::move(fut));
  }

  folly::collectAll(std::move(hostFutures))
      .via(ioThreadPool_.get())
      .thenValue([p = std::move(promise), result, allHosts](auto&& tries) mutable {
        size_t idx = 0;
        for (auto& t : tries) {
          if (t.hasException()) {
            ++idx;
            continue;
          }
          auto&& status = std::move(t.value());
          if (!status.ok()) {
            ++idx;
            continue;
          }
          auto resp = status.value();
          result->emplace(allHosts[idx], std::move(*resp.leader_parts_ref()));
          ++idx;
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

  auto* evb = ioThreadPool_->getEventBase();
  auto storageHost = Utils::getAdminAddrFromStoreAddr(host);
  folly::via(evb, [evb, storageHost, host, pro = std::move(pro), spaceIds, name, this]() mutable {
    auto client = clientsMan_->client(storageHost, evb);
    storage::cpp2::CreateCPRequest req;
    std::vector<GraphSpaceID> idList;
    idList.insert(idList.end(), spaceIds.begin(), spaceIds.end());
    req.space_ids_ref() = idList;
    req.name_ref() = name;
    client->future_createCheckpoint(std::move(req))
        .via(evb)
        .then([p = std::move(pro), storageHost, host](
                  folly::Try<storage::cpp2::CreateCPResp>&& t) mutable {
          if (t.hasException()) {
            LOG(INFO) << folly::stringPrintf("RPC failure in AdminClient: %s",
                                             t.exception().what().c_str());
            p.setValue(Status::Error("RPC failure in createCheckpoint"));
            return;
          }
          auto&& resp = std::move(t).value();
          auto&& result = resp.get_result();
          if (result.get_failed_parts().empty()) {
            cpp2::HostBackupInfo hostBackupInfo;
            hostBackupInfo.host_ref() = host;
            hostBackupInfo.checkpoints_ref() = std::move(resp.get_info());
            p.setValue(std::move(hostBackupInfo));
            return;
          }
          p.setValue(Status::Error("create checkpoint failed"));
        });
  });

  return f;
}

folly::Future<Status> AdminClient::dropSnapshot(const std::set<GraphSpaceID>& spaceIds,
                                                const std::string& name,
                                                const HostAddr& host) {
  storage::cpp2::DropCPRequest req;
  std::vector<GraphSpaceID> idList;
  idList.insert(idList.end(), spaceIds.begin(), spaceIds.end());
  req.space_ids_ref() = idList;
  req.name_ref() = name;
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  getResponse(
      {Utils::getAdminAddrFromStoreAddr(host)},
      0,
      std::move(req),
      [](auto client, auto request) { return client->future_dropCheckpoint(request); },
      0,
      std::move(pro),
      3 /*The snapshot operation need to retry 3 times*/);
  return f;
}

folly::Future<Status> AdminClient::blockingWrites(const std::set<GraphSpaceID>& spaceIds,
                                                  storage::cpp2::EngineSignType sign,
                                                  const HostAddr& host) {
  storage::cpp2::BlockingSignRequest req;
  std::vector<GraphSpaceID> idList;
  idList.insert(idList.end(), spaceIds.begin(), spaceIds.end());
  req.space_ids_ref() = idList;
  req.sign_ref() = sign;
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  getResponse(
      {Utils::getAdminAddrFromStoreAddr(host)},
      0,
      std::move(req),
      [](auto client, auto request) { return client->future_blockingWrites(request); },
      0,
      std::move(pro),
      32 /*The blocking need to retry 32 times*/);
  return f;
}

folly::Future<Status> AdminClient::addTask(cpp2::AdminCmd cmd,
                                           int32_t jobId,
                                           int32_t taskId,
                                           GraphSpaceID spaceId,
                                           const std::vector<HostAddr>& targetHost,
                                           const std::vector<std::string>& taskSpecificParas,
                                           std::vector<PartitionID> parts,
                                           int concurrency,
                                           cpp2::StatsItem* statsResult) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  std::vector<HostAddr> hosts;
  if (targetHost.empty()) {
    auto activeHostsRet = ActiveHostsMan::getActiveAdminHosts(kv_);
    if (!nebula::ok(activeHostsRet)) {
      pro.setValue(Status::Error("Get active hosts failed"));
      return f;
    } else {
      hosts = nebula::value(activeHostsRet);
    }
  } else {
    hosts = targetHost;
  }

  storage::cpp2::AddAdminTaskRequest req;
  req.cmd_ref() = cmd;
  req.job_id_ref() = jobId;
  req.task_id_ref() = taskId;
  req.concurrency_ref() = concurrency;

  storage::cpp2::TaskPara para;
  para.space_id_ref() = spaceId;
  para.parts_ref() = std::move(parts);
  para.task_specific_paras_ref() = taskSpecificParas;
  req.para_ref() = std::move(para);

  std::function<void(storage::cpp2::AdminExecResp && resp)> respGen =
      [statsResult](storage::cpp2::AdminExecResp&& resp) -> void {
    if (statsResult && resp.stats_ref().has_value()) {
      *statsResult = *resp.stats_ref();
    }
  };

  getResponse(
      hosts,
      0,
      std::move(req),
      [](auto client, auto request) { return client->future_addAdminTask(request); },
      0,
      std::move(pro),
      0,
      std::move(respGen));
  return f;
}

folly::Future<Status> AdminClient::stopTask(const std::vector<HostAddr>& target,
                                            int32_t jobId,
                                            int32_t taskId) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  std::vector<HostAddr> hosts;
  if (target.empty()) {
    auto activeHostsRet = ActiveHostsMan::getActiveAdminHosts(kv_);
    if (!nebula::ok(activeHostsRet)) {
      pro.setValue(Status::Error("Get active hosts failed"));
      return f;
    } else {
      hosts = nebula::value(activeHostsRet);
    }
  } else {
    hosts = target;
  }

  storage::cpp2::StopAdminTaskRequest req;
  req.job_id_ref() = jobId;
  req.task_id_ref() = taskId;

  getResponse(
      hosts,
      0,
      std::move(req),
      [](auto client, auto request) { return client->future_stopAdminTask(request); },
      0,
      std::move(pro),
      1);
  return f;
}

}  // namespace meta
}  // namespace nebula
