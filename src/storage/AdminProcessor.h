/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSLEADERPROCESSOR_H_
#define STORAGE_TRANSLEADERPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/Part.h"

namespace nebula {
namespace storage {

class TransLeaderProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static TransLeaderProcessor* instance(kvstore::KVStore* kvstore) {
        return new TransLeaderProcessor(kvstore);
    }

    void process(const cpp2::TransLeaderReq& req) {
        CHECK_NOTNULL(kvstore_);
        auto spaceId = req.get_space_id();
        auto partId = req.get_part_id();
        auto ret = kvstore_->part(spaceId, partId);
        if (!ok(ret)) {
            resp_.set_code(to(error(ret)));
            promise_.setValue(std::move(resp_));
            delete this;
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
            resp_.set_code(to(code));
            promise_.setValue(std::move(resp_));
            delete this;
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
        UNUSED(req);
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
        UNUSED(req);
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
        UNUSED(req);
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
        UNUSED(req);
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

    void process(const cpp2::CatchUpDataReq& req) {
        UNUSED(req);
    }

private:
    explicit WaitingForCatchUpDataProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminExecResp>(kvstore, nullptr) {}
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
