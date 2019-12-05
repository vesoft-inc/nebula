/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_GETUUIDPROCESSOR_H_
#define STORAGE_GETUUIDPROCESSOR_H_

#include "base/Base.h"
#include "base/MurmurHash2.h"
#include "storage/BaseProcessor.h"
#include "kvstore/NebulaStore.h"
#include "time/WallClock.h"

namespace nebula {
namespace storage {

class GetUUIDProcessor : public BaseProcessor<cpp2::GetUUIDResp> {
public:
    static GetUUIDProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetUUIDProcessor(kvstore);
    }

    void process(const cpp2::GetUUIDReq& req) {
        constexpr size_t hashMask = 0xFFFFFFFF00000000;
        constexpr size_t timeMask = 0x00000000FFFFFFFF;
        CHECK_NOTNULL(kvstore_);
        auto spaceId = req.get_space_id();
        auto partId = req.get_part_id();
        auto name = req.get_name();
        auto key = NebulaKeyUtils::uuidKey(partId, name.c_str());
        std::string val;
        VertexID vId;
        auto ret = kvstore_->get(spaceId, partId, key, &val);
        // try to get the corresponding vertex id
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            // need to generate new vertex id of this uuid
            MurmurHash2 hashFunc;
            auto hashValue = hashFunc(name);
            auto now = time::WallClock::fastNowInSec();
            vId = (hashValue & hashMask) | (now & timeMask);
            val.append(reinterpret_cast<char*>(&vId), sizeof(VertexID));
            std::vector<kvstore::KV> data;
            data.emplace_back(std::move(key), std::move(val));

            folly::Baton<true, std::atomic> baton;
            kvstore_->asyncMultiPut(spaceId, partId, std::move(data),
                                    [&] (kvstore::ResultCode code) {
                ret = code;
                baton.post();
            });
            baton.wait();
            if (ret == kvstore::ResultCode::ERR_LEADER_CHANGED) {
                this->handleLeaderChanged(spaceId, partId);
                this->onFinished();
                return;
            } else if (ret != kvstore::ResultCode::SUCCEEDED) {
                this->pushResultCode(to(ret), partId);
                this->onFinished();
                return;
            }
        } else {
            CHECK_EQ(val.size(), sizeof(VertexID));
            vId = *reinterpret_cast<const VertexID*>(val.c_str());
        }

        resp_.set_id(vId);
        this->onFinished();
    }

private:
    explicit GetUUIDProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetUUIDResp>(kvstore, nullptr) {}
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_GETUUIDPROCESSOR_H_
