/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSLEADERPROCESSOR_H_
#define STORAGE_TRANSLEADERPROCESSOR_H_

#include "base/Base.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/Part.h"
#include "storage/StorageFlags.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class TransLeaderProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static TransLeaderProcessor* instance(kvstore::KVStore* kvstore) {
        return new TransLeaderProcessor(kvstore);
    }

    void process(const cpp2::TransLeaderReq& req) {
        CHECK_NOTNULL(kvstore_);
        LOG(INFO) << "Receive transfer leader for space "
                  << req.get_space_id() << ", part " << req.get_part_id()
                  << ", to [" << req.get_new_leader().get_ip()
                  << ", " << req.get_new_leader().get_port() << "]";
        auto spaceId = req.get_space_id();
        auto partId = req.get_part_id();
        auto ret = kvstore_->part(spaceId, partId);
        if (!ok(ret)) {
            onFinished(to(error(ret)));
            return;
        }
        auto part = nebula::value(ret);
        auto host = kvstore::NebulaStore::getRaftAddr(HostAddr(req.get_new_leader().get_ip(),
                                                               req.get_new_leader().get_port()));
        part->asyncTransferLeader(host,
                                  [this, spaceId, partId, host] (kvstore::ResultCode code) {
            auto leaderRet = kvstore_->partLeader(spaceId, partId);
            CHECK(ok(leaderRet));
            if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                auto leader = value(std::move(leaderRet));
                // Check target is randomPeer (0, 0) or the election has completed.
                if (host == HostAddr(0, 0) || leader == host) {
                    code = kvstore::ResultCode::SUCCEEDED;
                } else {
                    resp_.set_leader(toThriftHost(leader));
                }
            }
            onFinished(to(code));
        });
    }

private:
    explicit TransLeaderProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr) {}
};

class AddPartProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static AddPartProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddPartProcessor(kvstore);
    }

    void process(const cpp2::AddPartReq& req) {
        if (FLAGS_store_type != "nebula") {
            onFinished(cpp2::ErrorCode::E_INVALID_STORE);
            return;
        }

        LOG(INFO) << "Receive add part for space "
                  << req.get_space_id() << ", part " << req.get_part_id();
        auto* store = static_cast<kvstore::NebulaStore*>(kvstore_);
        store->addPart(req.get_space_id(), req.get_part_id(), req.get_as_learner());
        onFinished(cpp2::ErrorCode::SUCCEEDED);
    }

private:
    explicit AddPartProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr) {}
};

class RemovePartProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static RemovePartProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemovePartProcessor(kvstore);
    }

    void process(const cpp2::RemovePartReq& req) {
        if (FLAGS_store_type != "nebula") {
            onFinished(cpp2::ErrorCode::E_INVALID_STORE);
            return;
        }
        auto* store = static_cast<kvstore::NebulaStore*>(kvstore_);
        store->removePart(req.get_space_id(), req.get_part_id());
        onFinished(cpp2::ErrorCode::SUCCEEDED);
    }

private:
    explicit RemovePartProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr) {}
};

class MemberChangeProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static MemberChangeProcessor* instance(kvstore::KVStore* kvstore) {
        return new MemberChangeProcessor(kvstore);
    }

    void process(const cpp2::MemberChangeReq& req) {
        CHECK_NOTNULL(kvstore_);
        auto spaceId = req.get_space_id();
        auto partId = req.get_part_id();
        LOG(INFO) << "Receive member change for space "
                  << spaceId << ", part " << partId
                  << ", add/remove " << (req.get_add() ? "add" : "remove");
        auto ret = kvstore_->part(spaceId, partId);
        if (!ok(ret)) {
            onFinished(to(error(ret)));
            return;
        }
        auto part = nebula::value(ret);
        auto peer = kvstore::NebulaStore::getRaftAddr(HostAddr(req.get_peer().get_ip(),
                                                               req.get_peer().get_port()));
        auto cb = [this, spaceId, partId] (kvstore::ResultCode code) {
            auto leaderRet = kvstore_->partLeader(spaceId, partId);
            CHECK(ok(leaderRet));
            if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                auto leader = value(std::move(leaderRet));
                resp_.set_leader(toThriftHost(leader));
            }
            onFinished(to(code));
        };
        if (req.get_add()) {
            LOG(INFO) << "Add peer " << peer;
            part->asyncAddPeer(peer, cb);
        } else {
            LOG(INFO) << "Remove peer " << peer;
            part->asyncRemovePeer(peer, cb);
        }
    }

private:
    explicit MemberChangeProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr) {}
};

class AddLearnerProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static AddLearnerProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddLearnerProcessor(kvstore);
    }

    void process(const cpp2::AddLearnerReq& req) {
        auto spaceId = req.get_space_id();
        auto partId = req.get_part_id();
        LOG(INFO) << "Receive add learner for space "
                  << spaceId << ", part " << partId;
        auto ret = kvstore_->part(spaceId, partId);
        if (!ok(ret)) {
            onFinished(to(error(ret)));
            return;
        }
        auto part = nebula::value(ret);
        auto learner = kvstore::NebulaStore::getRaftAddr(HostAddr(req.get_learner().get_ip(),
                                                                  req.get_learner().get_port()));
        part->asyncAddLearner(learner, [this, spaceId, partId] (kvstore::ResultCode code) {
            auto leaderRet = kvstore_->partLeader(spaceId, partId);
            CHECK(ok(leaderRet));
            if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                auto leader = value(std::move(leaderRet));
                resp_.set_leader(toThriftHost(leader));
            }
            onFinished(to(code));
        });
    }

private:
    explicit AddLearnerProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr) {}
};

class WaitingForCatchUpDataProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static WaitingForCatchUpDataProcessor* instance(kvstore::KVStore* kvstore) {
        return new WaitingForCatchUpDataProcessor(kvstore);
    }

    ~WaitingForCatchUpDataProcessor() {
        if (bgThread_) {
            bgThread_->join();
        }
    }

    void process(const cpp2::CatchUpDataReq& req) {
        auto spaceId = req.get_space_id();
        auto partId = req.get_part_id();
        LOG(INFO) << "Received waiting for catching up data for space "
                  << spaceId << ", part " << partId;
        auto ret = kvstore_->part(spaceId, partId);
        if (!ok(ret)) {
            onFinished(to(error(ret)));
            return;
        }
        auto part = nebula::value(ret);
        auto peer = kvstore::NebulaStore::getRaftAddr(HostAddr(req.get_target().get_ip(),
                                                               req.get_target().get_port()));
        bgThread_ = std::make_unique<std::thread>([this, part, peer, spaceId, partId] {
            int retry = FLAGS_waiting_catch_up_retry_times;
            while (retry-- > 0) {
                LOG(INFO) << "Waiting for catching up data, peer " << peer
                          << ", try " << retry << " times";
                auto res = part->isCatchedUp(peer);
                switch (res) {
                    case raftex::AppendLogResult::SUCCEEDED:
                        onFinished(cpp2::ErrorCode::SUCCEEDED);
                        return;
                    case raftex::AppendLogResult::E_INVALID_PEER:
                        onFinished(cpp2::ErrorCode::E_INVALID_PEER);
                        return;
                    case raftex::AppendLogResult::E_NOT_A_LEADER: {
                        auto leaderRet = kvstore_->partLeader(spaceId, partId);
                        CHECK(ok(leaderRet));
                        auto leader = value(std::move(leaderRet));
                        resp_.set_leader(toThriftHost(leader));
                        onFinished(cpp2::ErrorCode::E_LEADER_CHANGED);
                        return;
                    }
                    default:
                        onFinished(cpp2::ErrorCode::E_UNKNOWN);
                        return;
                }
                sleep(FLAGS_waiting_catch_up_interval_in_secs);
            }
        });
    }

private:
    explicit WaitingForCatchUpDataProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr) {}

private:
    std::unique_ptr<std::thread> bgThread_;
};

class GetLeaderProcessor : public BaseProcessor<cpp2::GetLeaderResp> {
public:
    static GetLeaderProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetLeaderProcessor(kvstore);
    }

    void process(const cpp2::GetLeaderReq& req) {
        UNUSED(req);
        CHECK_NOTNULL(kvstore_);
        std::unordered_map<GraphSpaceID, std::vector<PartitionID>> leaderIds;
        kvstore_->allLeader(leaderIds);
        resp_.set_code(to(kvstore::ResultCode::SUCCEEDED));
        resp_.set_leader_parts(std::move(leaderIds));
        promise_.setValue(std::move(resp_));
        delete this;
    }

private:
    explicit GetLeaderProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetLeaderResp>(kvstore, nullptr) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSLEADERPROCESSOR_H_
