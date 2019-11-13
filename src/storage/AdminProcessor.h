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
#include <folly/executors/Async.h>

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
            this->pushResultCode(to(error(ret)), partId);
            onFinished();
            return;
        }
        auto part = nebula::value(ret);
        auto host = kvstore::NebulaStore::getRaftAddr(HostAddr(req.get_new_leader().get_ip(),
                                                               req.get_new_leader().get_port()));
        part->asyncTransferLeader(host,
                                  [this, spaceId, partId] (kvstore::ResultCode code) {
            auto leaderRet = kvstore_->partLeader(spaceId, partId);
            CHECK(ok(leaderRet));
            if (code == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                auto leader = value(std::move(leaderRet));
                this->pushResultCode(to(code), partId, leader);
            }
            onFinished();
        });
    }

private:
    explicit TransLeaderProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr) {}
};

class AddPartProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static AddPartProcessor* instance(kvstore::KVStore* kvstore, meta::MetaClient* mClient) {
        return new AddPartProcessor(kvstore, mClient);
    }

    void process(const cpp2::AddPartReq& req) {
        auto spaceId = req.get_space_id();
        auto partId = req.get_part_id();
        if (FLAGS_store_type != "nebula") {
            this->pushResultCode(cpp2::ErrorCode::E_INVALID_STORE, partId);
            onFinished();
            return;
        }

        LOG(INFO) << "Receive add part for space "
                  << req.get_space_id() << ", part " << partId;
        auto* store = static_cast<kvstore::NebulaStore*>(kvstore_);
        auto ret = store->space(spaceId);
        if (!nebula::ok(ret) && nebula::error(ret) == kvstore::ResultCode::ERR_SPACE_NOT_FOUND) {
            LOG(INFO) << "Space " << spaceId << " not exist, create it!";
            store->addSpace(spaceId);
        }
        auto st = mClient_->refreshCache();
        if (!st.ok()) {
            this->pushResultCode(cpp2::ErrorCode::E_LOAD_META_FAILED, partId);
            onFinished();
            return;
        }
        store->addPart(spaceId, partId, req.get_as_learner());
        onFinished();
    }

private:
    explicit AddPartProcessor(kvstore::KVStore* kvstore, meta::MetaClient* mClient)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr)
            , mClient_(mClient) {}

private:
    meta::MetaClient* mClient_ = nullptr;
};

class RemovePartProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static RemovePartProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemovePartProcessor(kvstore);
    }

    void process(const cpp2::RemovePartReq& req) {
        auto spaceId = req.get_space_id();
        auto partId = req.get_part_id();
        if (FLAGS_store_type != "nebula") {
            this->pushResultCode(cpp2::ErrorCode::E_INVALID_STORE, partId);
            onFinished();
            return;
        }
        auto* store = static_cast<kvstore::NebulaStore*>(kvstore_);
        store->removePart(spaceId, partId);
        onFinished();
    }

private:
    explicit RemovePartProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr) {}
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
            this->pushResultCode(to(error(ret)), partId);
            onFinished();
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
                this->pushResultCode(to(code), partId, leader);
            }
            onFinished();
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
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr) {}
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
            this->pushResultCode(to(error(ret)), partId);
            onFinished();
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
                this->pushResultCode(to(code), partId, leader);
            }
            onFinished();
        });
    }

private:
    explicit AddLearnerProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr) {}
};

class WaitingForCatchUpDataProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static WaitingForCatchUpDataProcessor* instance(kvstore::KVStore* kvstore) {
        return new WaitingForCatchUpDataProcessor(kvstore);
    }

    ~WaitingForCatchUpDataProcessor() {
    }

    void process(const cpp2::CatchUpDataReq& req) {
        auto spaceId = req.get_space_id();
        auto partId = req.get_part_id();
        LOG(INFO) << "Received waiting for catching up data for space "
                  << spaceId << ", part " << partId;
        auto ret = kvstore_->part(spaceId, partId);
        if (!ok(ret)) {
            this->pushResultCode(to(error(ret)), partId);
            onFinished();
            return;
        }
        auto part = nebula::value(ret);
        auto peer = kvstore::NebulaStore::getRaftAddr(HostAddr(req.get_target().get_ip(),
                                                               req.get_target().get_port()));

        folly::async([this, part, peer, spaceId, partId] {
            int retry = FLAGS_waiting_catch_up_retry_times;
            while (retry-- > 0) {
                auto res = part->isCatchedUp(peer);
                LOG(INFO) << "Waiting for catching up data, peer " << peer
                          << ", remaining " << retry << " retry times"
                          << ", result " << static_cast<int32_t>(res);
                switch (res) {
                    case raftex::AppendLogResult::SUCCEEDED:
                        onFinished();
                        return;
                    case raftex::AppendLogResult::E_INVALID_PEER:
                        this->pushResultCode(cpp2::ErrorCode::E_INVALID_PEER, partId);
                        onFinished();
                        return;
                    case raftex::AppendLogResult::E_NOT_A_LEADER: {
                        auto leaderRet = kvstore_->partLeader(spaceId, partId);
                        CHECK(ok(leaderRet));
                        auto leader = value(std::move(leaderRet));
                        this->pushResultCode(cpp2::ErrorCode::E_LEADER_CHANGED, partId, leader);
                        onFinished();
                        return;
                    }
                    case raftex::AppendLogResult::E_SENDING_SNAPSHOT:
                        LOG(INFO) << "Still sending snapshot, please wait...";
                        break;
                    default:
                        LOG(INFO) << "Unknown error " << static_cast<int32_t>(res);
                        break;
                }
                sleep(FLAGS_waiting_catch_up_interval_in_secs);
            }
            this->pushResultCode(cpp2::ErrorCode::E_RETRY_EXHAUSTED, partId);
            onFinished();
        });
    }

private:
    explicit WaitingForCatchUpDataProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr, nullptr) {}
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
        resp_.set_leader_parts(std::move(leaderIds));
        this->onFinished();
    }

private:
    explicit GetLeaderProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetLeaderResp>(kvstore, nullptr, nullptr) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSLEADERPROCESSOR_H_
