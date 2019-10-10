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
    auto target = dst;
    if (dst == kRandomPeer) {
        auto ret = getPeers(spaceId, partId);
        if (!ret.ok()) {
            return ret.status();
        }
        auto& peers = ret.value();
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
    getResponse(ret.value(), 0, std::move(req), [] (auto client, auto request) {
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
    getResponse(ret.value(), 0, std::move(req), [] (auto client, auto request) {
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
    getResponse(ret.value(), 0, std::move(req), [] (auto client, auto request) {
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
    std::vector<nebula::cpp2::HostAddr> thriftPeers;
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
        remoteFunc(client, std::move(req))
            .then(evb, [p = std::move(pro), partId, respGen = std::move(respGen)](
                           folly::Try<storage::cpp2::AdminExecResp>&& t) mutable {
                // exception occurred during RPC
                if (t.hasException()) {
                    p.setValue(Status::Error(folly::stringPrintf("RPC failure in AdminClient: %s",
                                                                 t.exception().what().c_str())));
                    return;
                }
                auto&& result = std::move(t).value().get_result();
                if (result.get_failed_codes().empty()) {
                    storage::cpp2::ResultCode resultCode;
                    resultCode.set_code(storage::cpp2::ErrorCode::SUCCEEDED);
                    resultCode.set_part_id(partId);
                    p.setValue(respGen(resultCode));
                } else {
                    auto resp = result.get_failed_codes().front();
                    p.setValue(respGen(std::move(resp)));
                }
            });
    });
    return f;
}

template<typename Request, typename RemoteFunc>
void AdminClient::getResponse(
                         std::vector<HostAddr> hosts,
                         int32_t index,
                         Request req,
                         RemoteFunc remoteFunc,
                         int32_t retry,
                         folly::Promise<Status> pro,
                         int32_t retryLimit) {
    auto* evb = ioThreadPool_->getEventBase();
    CHECK_GE(index, 0);
    CHECK_LT(index, hosts.size());
    auto partId = req.get_part_id();
    folly::via(evb, [evb, hosts = std::move(hosts), index, req = std::move(req), partId,
                     remoteFunc = std::move(remoteFunc), retry, pro = std::move(pro),
                     retryLimit, this] () mutable {
        auto client = clientsMan_->client(hosts[index], evb);
        remoteFunc(client, req)
            .then(evb, [p = std::move(pro), hosts = std::move(hosts), index, req = std::move(req),
                        partId, remoteFunc = std::move(remoteFunc), retry, retryLimit,
                        this] (folly::Try<storage::cpp2::AdminExecResp>&& t) mutable {
            // exception occurred during RPC
            if (t.hasException()) {
                if (retry < retryLimit) {
                    LOG(INFO) << "Rpc failure to " << hosts[index]
                              << ", retry " << retry
                              << ", limit " << retryLimit;
                    index = ++index % hosts.size();
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
            if (result.get_failed_codes().empty()) {
                p.setValue(Status::OK());
                return;
            }
            auto resp = result.get_failed_codes().front();
            switch (resp.get_code()) {
                case storage::cpp2::ErrorCode::E_LEADER_CHANGED: {
                    if (retry < retryLimit) {
                        HostAddr leader(0, 0);
                        if (resp.get_leader() != nullptr) {
                            leader = HostAddr(resp.get_leader()->get_ip(),
                                              resp.get_leader()->get_port());
                        }
                        if (leader == HostAddr(0, 0)) {
                            usleep(1000 * 50);
                            LOG(INFO) << "The leader is in election"
                                      << ", retry " << retry
                                      << ", limit " << retryLimit;
                            index = ++index % hosts.size();
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
                            LOG(ERROR) << "The new leader is " << leader;
                            for (auto& h : hosts) {
                                LOG(ERROR) << "The peer is " << h;
                            }
                            p.setValue(Status::Error("Can't find leader in current peers"));
                            return;
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
                        index = ++index % hosts.size();
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

nebula::cpp2::HostAddr AdminClient::toThriftHost(const HostAddr& addr) {
    nebula::cpp2::HostAddr thriftAddr;
    thriftAddr.set_ip(addr.first);
    thriftAddr.set_port(addr.second);
    return thriftAddr;
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
                return HostAddr(h.get_ip(), h.get_port());
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

folly::Future<Status> AdminClient::getLeaderDist(HostLeaderMap* result) {
    if (injector_) {
        return injector_->getLeaderDist(result);
    }
    folly::Promise<Status> promise;
    auto future = promise.getFuture();
    auto allHosts = ActiveHostsMan::getActiveHosts(kv_);

    auto getLeader = [result, this] (const HostAddr& host) {
        folly::Promise<Status> pro;
        auto f = pro.getFuture();
        auto* evb = ioThreadPool_->getEventBase();
        folly::via(evb, [pro = std::move(pro), host, evb, result, this] () mutable {
            storage::cpp2::GetLeaderReq req;
            auto client = clientsMan_->client(host, evb);
            client->future_getLeaderPart(std::move(req))
                .then(evb, [p = std::move(pro), host,
                            result] (folly::Try<storage::cpp2::GetLeaderResp>&& t) mutable {
                if (t.hasException()) {
                    LOG(ERROR) << folly::stringPrintf("RPC failure in AdminClient: %s",
                                                      t.exception().what().c_str());
                    p.setValue(Status::Error("RPC failure in AdminClient"));
                    return;
                }
                auto&& resp = std::move(t).value();
                LOG(INFO) << "Get leader for host " << host;
                result->emplace(host, std::move(resp).get_leader_parts());
                p.setValue(Status::OK());
            });
        });
        return f;
    };

    std::vector<folly::SemiFuture<Status>> hostFutures;
    for (const auto& h : allHosts) {
        auto fut = getLeader(h);
        hostFutures.emplace_back(std::move(fut));
    }

    folly::collectAll(hostFutures)
        .then([p = std::move(promise)] (std::vector<folly::Try<Status>>&& tries) mutable {
        UNUSED(tries);
        p.setValue(Status::OK());
    });

    return future;
}

}  // namespace meta
}  // namespace nebula
