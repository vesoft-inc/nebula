/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/processors/admin/AdminClient.h"
#include "meta/MetaServiceUtils.h"
#include "meta/processors/Common.h"
#include "meta/ActiveHostsMan.h"
#include "kvstore/Part.h"
#include "utils/Utils.h"

DEFINE_int32(max_retry_times_admin_op, 30, "max retry times for admin request!");

namespace nebula {
namespace meta {

folly::Future<Status> AdminClient::transLeader(GraphSpaceID spaceId,
                                               PartitionID partId,
                                               const HostAddr& leader,
                                               const HostAddr& dst) {
    if (injector_) {
        return injector_->transLeader();
    }
    storage::cpp2::TransLeaderReq req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    auto ret = getPeers(spaceId, partId);
    if (!ret.ok()) {
        return ret.status();
    }
    auto& peers = ret.value();
    auto it = std::find(peers.begin(), peers.end(), leader);
    if (it == peers.end()) {
        LOG(WARNING) << "Can't find part " << partId << " on " << leader;
        return Status::PartNotFound();
    }
    auto target = dst;
    if (dst == kRandomPeer) {
        for (auto& p : peers) {
            if (p != leader && ActiveHostsMan::isLived(kv_, p)) {
                target = p;
                break;
            }
        }
    }
    req.set_new_leader(toThriftHost(target));
    return getResponse(leader, std::move(req), [] (auto client, auto request) {
               return client->future_transLeader(request);
           }, [] (auto&& resp) -> Status {
               switch (resp.get_code()) {
                   case storage::cpp2::ErrorCode::SUCCEEDED:
                   case storage::cpp2::ErrorCode::E_LEADER_CHANGED: {
                       return Status::OK();
                   }
                   case storage::cpp2::ErrorCode::E_PART_NOT_FOUND: {
                       return Status::PartNotFound();
                   }
                   default:
                       return Status::Error("Unknown code %d",
                                            static_cast<int32_t>(resp.get_code()));
               }
           });
}

folly::Future<Status> AdminClient::addPart(GraphSpaceID spaceId,
                                           PartitionID partId,
                                           const HostAddr& host,
                                           bool asLearner) {
    if (injector_) {
        return injector_->addPart();
    }
    storage::cpp2::AddPartReq req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    req.set_as_learner(asLearner);
    auto ret = getPeers(spaceId, partId);
    if (!ret.ok()) {
        return ret.status();
    }
    auto peers = std::move(ret).value();
    std::vector<HostAddr> thriftPeers;
    thriftPeers.resize(peers.size());
    std::transform(peers.begin(), peers.end(), thriftPeers.begin(), [this](const auto& h) {
        return toThriftHost(h);
    });
    req.set_peers(std::move(thriftPeers));
    return getResponse(host, std::move(req), [] (auto client, auto request) {
               return client->future_addPart(request);
           }, [] (auto&& resp) -> Status {
               if (resp.get_code() == storage::cpp2::ErrorCode::SUCCEEDED) {
                   return Status::OK();
               } else {
                   return Status::Error("Add part failed! code=%d",
                                        static_cast<int32_t>(resp.get_code()));
               }
           });
}

folly::Future<Status> AdminClient::addLearner(GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const HostAddr& learner) {
    if (injector_) {
        return injector_->addLearner();
    }
    storage::cpp2::AddLearnerReq req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    req.set_learner(toThriftHost(learner));
    auto ret = getPeers(spaceId, partId);
    if (!ret.ok()) {
        return ret.status();
    }
    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    getResponse(std::move(ret).value(), 0, std::move(req), [] (auto client, auto request) {
        return client->future_addLearner(request);
    }, 0, std::move(pro), FLAGS_max_retry_times_admin_op);
    return f;
}

folly::Future<Status> AdminClient::waitingForCatchUpData(GraphSpaceID spaceId,
                                                         PartitionID partId,
                                                         const HostAddr& target) {
    if (injector_) {
        return injector_->waitingForCatchUpData();
    }
    storage::cpp2::CatchUpDataReq req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    req.set_target(toThriftHost(target));
    auto ret = getPeers(spaceId, partId);
    if (!ret.ok()) {
        return ret.status();
    }
    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    getResponse(std::move(ret).value(), 0, std::move(req), [] (auto client, auto request) {
        return client->future_waitingForCatchUpData(request);
    }, 0, std::move(pro), 3);
    return f;
}

folly::Future<Status> AdminClient::memberChange(GraphSpaceID spaceId,
                                                PartitionID partId,
                                                const HostAddr& peer,
                                                bool added) {
    if (injector_) {
        return injector_->memberChange();
    }
    storage::cpp2::MemberChangeReq req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    req.set_add(added);
    req.set_peer(toThriftHost(peer));
    auto ret = getPeers(spaceId, partId);
    if (!ret.ok()) {
        return ret.status();
    }
    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    getResponse(std::move(ret).value(), 0, std::move(req), [] (auto client, auto request) {
        return client->future_memberChange(request);
    }, 0, std::move(pro), FLAGS_max_retry_times_admin_op);
    return f;
}

folly::Future<Status> AdminClient::updateMeta(GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const HostAddr& src,
                                              const HostAddr& dst) {
    if (injector_) {
        return injector_->updateMeta();
    }
    CHECK_NOTNULL(kv_);
    auto ret = getPeers(spaceId, partId);
    if (!ret.ok()) {
        return ret.status();
    }
    auto peers = std::move(ret).value();
    auto strHosts = [] (const std::vector<HostAddr>& hosts) -> std::string {
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
        // In this case, the dst should be existed in peers.
        if (std::find(peers.begin(), peers.end(), dst) == peers.end()) {
            LOG(ERROR) << "[space:" << spaceId << ", part:" << partId << "] dst "
                       << dst << "should be existed in peers!";
            return Status::Error("dst not exist in peers");
        }
        return Status::OK();
    }
    peers.erase(it);

    if (std::find(peers.begin(), peers.end(), dst) != peers.end()) {
        LOG(ERROR) << "[space:" << spaceId << ", part:" << partId << "] dst "
                   << dst << " has been existed!";
        return Status::Error("dst has been existed in peers");
    }
    peers.emplace_back(dst);
    std::vector<HostAddr> thriftPeers;
    thriftPeers.resize(peers.size());
    std::transform(peers.begin(), peers.end(), thriftPeers.begin(), [this](const auto& h) {
        return toThriftHost(h);
    });

    auto partRet = kv_->part(kDefaultSpaceId, kDefaultPartId);
    CHECK(ok(partRet));
    auto part = nebula::value(partRet);

    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    std::vector<kvstore::KV> data;
    data.emplace_back(MetaServiceUtils::partKey(spaceId, partId),
                      MetaServiceUtils::partVal(thriftPeers));
    part->asyncMultiPut(std::move(data), [] (kvstore::ResultCode) {});
    part->sync([this, p = std::move(pro)] (kvstore::ResultCode code) mutable {
        // To avoid dead lock, we call future callback in ioThreadPool_
        folly::via(ioThreadPool_.get(), [code, p = std::move(p)] () mutable {
            if (code == kvstore::ResultCode::SUCCEEDED) {
                p.setValue(Status::OK());
            } else {
                p.setValue(Status::Error("Access kv failed, code:%d", static_cast<int32_t>(code)));
            }
        });
    });
    return f;
}

folly::Future<Status> AdminClient::removePart(GraphSpaceID spaceId,
                                              PartitionID partId,
                                              const HostAddr& host) {
    if (injector_) {
        return injector_->removePart();
    }
    storage::cpp2::RemovePartReq req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    return getResponse(host, std::move(req), [] (auto client, auto request) {
               return client->future_removePart(request);
           }, [] (auto&& resp) -> Status {
               if (resp.get_code() == storage::cpp2::ErrorCode::SUCCEEDED) {
                   return Status::OK();
               } else {
                   return Status::Error("Remove part failed! code=%d",
                                        static_cast<int32_t>(resp.get_code()));
               }
           });
}

folly::Future<Status> AdminClient::checkPeers(GraphSpaceID spaceId, PartitionID partId) {
    if (injector_) {
        return injector_->checkPeers();
    }
    storage::cpp2::CheckPeersReq req;
    req.set_space_id(spaceId);
    req.set_part_id(partId);
    auto ret = getPeers(spaceId, partId);
    if (!ret.ok()) {
        return ret.status();
    }
    auto peers = std::move(ret).value();
    std::vector<HostAddr> thriftPeers;
    thriftPeers.resize(peers.size());
    std::transform(peers.begin(), peers.end(), thriftPeers.begin(), [this](const auto& h) {
        return toThriftHost(h);
    });
    req.set_peers(std::move(thriftPeers));
    folly::Promise<Status> pro;
    auto fut = pro.getFuture();
    std::vector<folly::Future<Status>> futures;
    for (auto& p : peers) {
        if (!ActiveHostsMan::isLived(kv_, p)) {
            LOG(INFO) << "[" << spaceId << ":" << partId << "], Skip the dead host " << p;
            continue;
        }
        auto f = getResponse(p, req, [] (auto client, auto request) {
                    return client->future_checkPeers(request);
                 }, [] (auto&& resp) -> Status {
                    if (resp.get_code() == storage::cpp2::ErrorCode::SUCCEEDED) {
                        return Status::OK();
                    } else {
                        return Status::Error("Check peers failed! code=%d",
                                             static_cast<int32_t>(resp.get_code()));
                    }
                 });
        futures.emplace_back(std::move(f));
    }
    folly::collectAll(std::move(futures)).thenTry([p = std::move(pro)] (auto&& t) mutable {
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

template<typename Request,
         typename RemoteFunc,
         typename RespGenerator>
folly::Future<Status> AdminClient::getResponse(
                                     const HostAddr& host,
                                     Request req,
                                     RemoteFunc remoteFunc,
                                     RespGenerator respGen) {
    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    auto* evb = ioThreadPool_->getEventBase();
    auto partId = req.get_part_id();
    folly::via(evb, [evb, pro = std::move(pro), host, req = std::move(req), partId,
                     remoteFunc = std::move(remoteFunc), respGen = std::move(respGen),
                     this] () mutable {
        auto client = clientsMan_->client(host, evb);
        remoteFunc(client, std::move(req)).via(evb)
            .then([p = std::move(pro), partId, respGen = std::move(respGen), host](
                           folly::Try<storage::cpp2::AdminExecResp>&& t) mutable {
                // exception occurred during RPC
                if (t.hasException()) {
                    p.setValue(Status::Error(folly::stringPrintf(
                                                    "[%s] RPC failure in AdminClient: %s",
                                                     host.toString().c_str(),
                                                     t.exception().what().c_str())));
                    return;
                }
                auto&& result = std::move(t).value().get_result();
                if (result.get_failed_parts().empty()) {
                    storage::cpp2::PartitionResult resultCode;
                    resultCode.set_code(storage::cpp2::ErrorCode::SUCCEEDED);
                    resultCode.set_part_id(partId);
                    p.setValue(respGen(resultCode));
                } else {
                    auto resp = result.get_failed_parts().front();
                    p.setValue(respGen(std::move(resp)));
                }
            });
    });
    return f;
}

template<typename Request, typename RemoteFunc>
void AdminClient::getResponse(std::vector<HostAddr> hosts,
                              int32_t index,
                              Request req,
                              RemoteFunc remoteFunc,
                              int32_t retry,
                              folly::Promise<Status> pro,
                              int32_t retryLimit) {
    auto* evb = ioThreadPool_->getEventBase();
    CHECK_GE(index, 0);
    CHECK_LT(index, hosts.size());
    folly::via(evb, [evb, hosts = std::move(hosts), index, req = std::move(req),
                     remoteFunc = std::move(remoteFunc), retry, pro = std::move(pro),
                     retryLimit, this] () mutable {
        auto client = clientsMan_->client(hosts[index], evb);
        remoteFunc(client, req).via(evb)
            .then([p = std::move(pro), hosts = std::move(hosts), index, req = std::move(req),
                   remoteFunc = std::move(remoteFunc), retry, retryLimit,
                   this] (folly::Try<storage::cpp2::AdminExecResp>&& t) mutable {
            // exception occurred during RPC
            if (t.hasException()) {
                if (retry < retryLimit) {
                    LOG(INFO) << "Rpc failure to " << hosts[index]
                              << ", retry " << retry
                              << ", limit " << retryLimit
                              << ", error: " << t.exception();
                    index = (index + 1) % hosts.size();
                    getResponse(std::move(hosts),
                                index,
                                std::move(req),
                                remoteFunc,
                                retry + 1,
                                std::move(p),
                                retryLimit);
                    return;
                }
                p.setValue(Status::Error(folly::stringPrintf("RPC failure in AdminClient: %s",
                                                             t.exception().what().c_str())));
                return;
            }
            auto&& result = std::move(t).value().get_result();
            if (result.get_failed_parts().empty()) {
                p.setValue(Status::OK());
                return;
            }
            auto resp = result.get_failed_parts().front();
            switch (resp.get_code()) {
                case storage::cpp2::ErrorCode::E_LEADER_CHANGED: {
                    if (retry < retryLimit) {
                        HostAddr leader("", 0);
                        if (resp.get_leader() != nullptr) {
                            leader = HostAddr(resp.get_leader()->host,
                                              resp.get_leader()->port);
                        }
                        if (leader == HostAddr("", 0)) {
                            usleep(1000 * 50);
                            LOG(INFO) << "The leader is in election"
                                      << ", retry " << retry
                                      << ", limit " << retryLimit;
                            index = (index + 1) % hosts.size();
                            getResponse(std::move(hosts),
                                    index,
                                    std::move(req),
                                    std::move(remoteFunc),
                                    retry + 1,
                                    std::move(p),
                                    retryLimit);
                            return;
                        }
                        int32_t leaderIndex = 0;
                        for (auto& h : hosts) {
                            if (h == leader) {
                                break;
                            }
                            leaderIndex++;
                        }
                        if (leaderIndex == (int32_t)hosts.size()) {
                            // In some cases (e.g. balance task is failed in member change remove
                            // phase, and the new host is elected as leader somehow), the peers of
                            // this partition in meta doesn't include new host, which will make
                            // this task failed forever.
                            index = leaderIndex;
                            hosts.emplace_back(leader);
                        }
                        LOG(INFO) << "Return leader change from " << hosts[index]
                                  << ", new leader is " << leader
                                  << ", retry " << retry
                                  << ", limit " << retryLimit;
                        CHECK_LT(leaderIndex, hosts.size());
                        getResponse(std::move(hosts),
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
                                  << " from " << hosts[index]
                                  << ", retry " << retry
                                  << ", limit " << retryLimit;
                        index = (index + 1) % hosts.size();
                        getResponse(std::move(hosts),
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
    });  // via
}

HostAddr AdminClient::toThriftHost(const HostAddr& addr) {
    return addr;
}

StatusOr<std::vector<HostAddr>> AdminClient::getPeers(GraphSpaceID spaceId, PartitionID partId) {
    CHECK_NOTNULL(kv_);
    auto partKey = MetaServiceUtils::partKey(spaceId, partId);
    std::string value;
    auto code = kv_->get(kDefaultSpaceId, kDefaultPartId, partKey, &value);
    switch (code) {
        case kvstore::ResultCode::SUCCEEDED: {
            auto partHosts = MetaServiceUtils::parsePartVal(value);
            std::vector<HostAddr> hosts;
            hosts.resize(partHosts.size());
            std::transform(partHosts.begin(), partHosts.end(), hosts.begin(), [](const auto& h) {
                return HostAddr(h.host, h.port);
            });
            return hosts;
        }
        case kvstore::ResultCode::ERR_KEY_NOT_FOUND:
            return Status::Error("Key Not Found");
        default:
            LOG(WARNING) << "Get peers failed, error " << static_cast<int32_t>(code);
            break;
    }
    return Status::Error("Get Failed");
}

void AdminClient::getLeaderDist(const HostAddr& host,
                                folly::Promise<StatusOr<storage::cpp2::GetLeaderPartsResp>>&& pro,
                                int32_t retry,
                                int32_t retryLimit) {
    auto* evb = ioThreadPool_->getEventBase();
    folly::via(evb, [evb, host, pro = std::move(pro), retry, retryLimit, this] () mutable {
        storage::cpp2::GetLeaderReq req;
        auto client = clientsMan_->client(host, evb);
        client->future_getLeaderParts(std::move(req)).via(evb)
            .then([pro = std::move(pro), host, retry, retryLimit, this]
                       (folly::Try<storage::cpp2::GetLeaderPartsResp>&& t) mutable {
            if (t.hasException()) {
                LOG(ERROR) << folly::stringPrintf("RPC failure in AdminClient: %s",
                                                  t.exception().what().c_str());
                if (retry < retryLimit) {
                    usleep(1000 * 50);
                    getLeaderDist(Utils::getAdminAddrFromStoreAddr(host),
                                  std::move(pro),
                                  retry + 1,
                                  retryLimit);
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
    if (injector_) {
        return injector_->getLeaderDist(result);
    }
    folly::Promise<Status> promise;
    auto future = promise.getFuture();
    auto allHosts = ActiveHostsMan::getActiveHosts(kv_);

    std::vector<folly::Future<StatusOr<storage::cpp2::GetLeaderPartsResp>>> hostFutures;
    for (const auto& h : allHosts) {
        folly::Promise<StatusOr<storage::cpp2::GetLeaderPartsResp>> pro;
        auto fut = pro.getFuture();
        getLeaderDist(Utils::getAdminAddrFromStoreAddr(h), std::move(pro), 0, 3);
        hostFutures.emplace_back(std::move(fut));
    }

    folly::collectAll(std::move(hostFutures)).thenValue([p = std::move(promise), result, allHosts]
            (std::vector<folly::Try<StatusOr<storage::cpp2::GetLeaderPartsResp>>>&& tries) mutable {
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
            result->emplace(allHosts[idx], std::move(resp.leader_parts));
            ++idx;
        }

        p.setValue(Status::OK());
    }).thenError([p = std::move(promise)] (auto&& e) mutable {
        p.setValue(Status::Error("Get leader failed, %s", e.what().c_str()));
    });

    return future;
}

folly::Future<Status> AdminClient::createSnapshot(GraphSpaceID spaceId,
                                                  const std::string& name,
                                                  const HostAddr& host) {
    if (injector_) {
        return injector_->createSnapshot();
    }

    storage::cpp2::CreateCPRequest req;
    req.set_space_id(spaceId);
    req.set_name(name);

    folly::Promise<Status> pro;
    auto f = pro.getFuture();

    /**
     * Don't need retry.
     * Because existing checkpoint directories leads to fail again.
     **/
    getResponse({Utils::getAdminAddrFromStoreAddr(host)}, 0, std::move(req),
            [] (auto client, auto request) {
        return client->future_createCheckpoint(request);
    }, 0, std::move(pro), 0);
    return f;
}

folly::Future<Status> AdminClient::dropSnapshot(GraphSpaceID spaceId,
                                                const std::string& name,
                                                const HostAddr& host) {
    if (injector_) {
        return injector_->dropSnapshot();
    }

    storage::cpp2::DropCPRequest req;
    req.set_space_id(spaceId);
    req.set_name(name);

    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    getResponse({Utils::getAdminAddrFromStoreAddr(host)}, 0, std::move(req),
            [] (auto client, auto request) {
        return client->future_dropCheckpoint(request);
    }, 0, std::move(pro), 1 /*The snapshot operation only needs to be retried twice*/);
    return f;
}

folly::Future<Status> AdminClient::blockingWrites(GraphSpaceID spaceId,
                                                  storage::cpp2::EngineSignType sign,
                                                  const HostAddr& host) {
    storage::cpp2::BlockingSignRequest req;
    req.set_space_id(spaceId);
    req.set_sign(sign);
    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    getResponse({Utils::getAdminAddrFromStoreAddr(host)}, 0, std::move(req),
            [] (auto client, auto request) {
        return client->future_blockingWrites(request);
    }, 0, std::move(pro), 1 /*The blocking needs to be retried twice*/);
    return f;
}

folly::Future<Status> AdminClient::rebuildTagIndex(const HostAddr& address,
                                                   GraphSpaceID spaceId,
                                                   IndexID indexID,
                                                   std::vector<PartitionID> parts) {
    if (injector_) {
        return injector_->rebuildTagIndex();
    }

    std::vector<HostAddr> hosts{address};
    storage::cpp2::RebuildIndexRequest req;
    req.set_space_id(spaceId);
    req.set_index_id(indexID);
    req.set_parts(std::move(parts));
    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    getResponse(std::move(hosts), 0, std::move(req), [] (auto client, auto request) {
        return client->future_rebuildTagIndex(request);
    }, 0, std::move(pro), 1);
    return f;
}

folly::Future<Status> AdminClient::rebuildEdgeIndex(const HostAddr& address,
                                                    GraphSpaceID spaceId,
                                                    IndexID indexID,
                                                    std::vector<PartitionID> parts) {
    if (injector_) {
        return injector_->rebuildEdgeIndex();
    }

    std::vector<HostAddr> hosts{address};
    storage::cpp2::RebuildIndexRequest req;
    req.set_space_id(spaceId);
    req.set_index_id(indexID);
    req.set_parts(std::move(parts));
    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    getResponse(std::move(hosts), 0, std::move(req), [] (auto client, auto request) {
        return client->future_rebuildEdgeIndex(request);
    }, 0, std::move(pro), 1);
    return f;
}

folly::Future<Status>
AdminClient::addTask(cpp2::AdminCmd cmd,
                    int32_t jobId,
                    int32_t taskId,
                    GraphSpaceID spaceId,
                    const std::vector<HostAddr>& targetHost,
                    const std::vector<std::string>& taskSpecficParas,
                    std::vector<PartitionID> parts,
                    int concurrency) {
    auto hosts = targetHost.empty() ? ActiveHostsMan::getActiveAdminHosts(kv_) : targetHost;
    storage::cpp2::AddAdminTaskRequest req;
    req.set_cmd(cmd);
    req.set_job_id(jobId);
    req.set_task_id(taskId);
    req.set_concurrency(concurrency);

    storage::cpp2::TaskPara para;
    para.set_space_id(spaceId);
    para.set_parts(std::move(parts));
    para.set_task_specfic_paras(taskSpecficParas);

    req.set_para(para);

    folly::Promise<Status> pro;
    auto f = pro.getFuture();

    getResponse(hosts, 0, std::move(req), [] (auto client, auto request) {
        return client->future_addAdminTask(request);
    }, 0, std::move(pro), 0);

    return f;
}

folly::Future<Status>
AdminClient::stopTask(const std::vector<HostAddr>& target,
                      int32_t jobId,
                      int32_t taskId) {
    LOG(INFO) << "AdminClient::stopTask()";
    auto hosts = target.empty() ? ActiveHostsMan::getActiveAdminHosts(kv_) : target;
    storage::cpp2::StopAdminTaskRequest req;
    req.set_job_id(jobId);
    req.set_task_id(taskId);

    folly::Promise<Status> pro;
    auto f = pro.getFuture();
    getResponse(hosts, 0, std::move(req), [] (auto client, auto request) {
        return client->future_stopAdminTask(request);
    }, 0, std::move(pro), 1);

    return f;
}
}  // namespace meta
}  // namespace nebula

