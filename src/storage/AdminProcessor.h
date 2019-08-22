/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_TRANSLEADERPROCESSOR_H_
#define STORAGE_TRANSLEADERPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class TransLeaderProcessor : public BaseProcessor<cpp2::AdminExecResp> {
public:
    static TransLeaderProcessor* instance(kvstore::KVStore* kvstore) {
        return new TransLeaderProcessor(kvstore);
    }

    void process(const cpp2::TransLeaderReq& req) {
        UNUSED(req);
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

class StatisticsProcessor : public BaseProcessor<cpp2::StatisticsResp> {
public:
    static StatisticsProcessor* instance(kvstore::KVStore* kvstore) {
        return new StatisticsProcessor(kvstore);
    }

    void process(const nebula::cpp2::HostAddr& h) {
        auto status = kvstore_->statistics();
        std::vector<cpp2::StatisticsData> v;
        for (auto& s : status) {
            v.emplace_back(apache::thrift::FragileConstructor::FRAGILE, std::get<0>(s),
                           std::get<1>(s), std::get<2>(s));
        }
        resp_.set_data(std::move(v));
        resp_.set_host(h);
        this->onFinished();
    }

private:
    explicit StatisticsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::StatisticsResp>(kvstore, nullptr) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_TRANSLEADERPROCESSOR_H_
