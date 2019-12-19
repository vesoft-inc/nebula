/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/GetUUIDProcessor.h"
#include "base/NebulaKeyUtils.h"
#include "base/MurmurHash2.h"
#include "kvstore/LogEncoder.h"
#include "time/WallClock.h"

DEFINE_bool(enable_uuid_cache, true, "Enable uuid cache");

namespace nebula {
namespace storage {

void GetUUIDProcessor::process(const cpp2::GetUUIDReq& req) {
    constexpr size_t hashMask = 0xFFFFFFFF00000000;
    constexpr size_t timeMask = 0x00000000FFFFFFFF;
    CHECK_NOTNULL(kvstore_);
    auto spaceId = req.get_space_id();
    auto partId = req.get_part_id();
    auto name = req.get_name();
    auto key = NebulaKeyUtils::uuidKey(partId, name.c_str());

    if (FLAGS_enable_uuid_cache && uuidCache_ != nullptr) {
        auto result = uuidCache_->get(key, partId);
        if (result.ok()) {
            auto vId = std::move(result).value();
            VLOG(3) << "Hit cache for uuid(" << name << "), vId = " << vId;
            resp_.set_id(vId);
            this->onFinished();
            return;
        } else {
            VLOG(3) << "Miss cache for uuid(" << name << ")";
        }
    }

    callingNum_ = 1;
    auto atomicOp = [spaceId, partId, key, name = std::move(name), this]() -> std::string {
        std::string value;
        auto code = this->kvstore_->get(spaceId, partId, key, &value);
        if (code == kvstore::ResultCode::SUCCEEDED) {
            vId_ = *reinterpret_cast<const VertexID*>(value.c_str());
            succeeded_ = true;
            LOG(INFO) << "!!!";
            return std::string("");
        } else if (code == kvstore::ResultCode::ERR_KEY_NOT_FOUND) {
            // need to generate new vertex id of this uuid
            MurmurHash2 hashFunc;
            auto hashValue = hashFunc(name);
            auto now = time::WallClock::fastNowInSec();
            vId_ = (hashValue & hashMask) | (now & timeMask);
            value.append(reinterpret_cast<char*>(&vId_), sizeof(VertexID));
            LOG(INFO) << "@@@";
            return kvstore::encodeMultiValues(kvstore::LogType::OP_PUT,
                                              std::move(key), std::move(value));
        }
        return std::string("");
    };
    auto callback = [spaceId, partId, key, this](kvstore::ResultCode code) {
        if (succeeded_ || code == kvstore::ResultCode::SUCCEEDED) {
            LOG(INFO) << "###";
            resp_.set_id(vId_);
            if (FLAGS_enable_uuid_cache && uuidCache_ != nullptr) {
                uuidCache_->putIfAbsent(std::move(key), vId_, partId);
                VLOG(3) << "Insert cache of " << key;
            }
            this->onFinished();
            return;
        }
        LOG(INFO) << "$$$";
        handleAsync(spaceId, partId, code);
    };
    this->kvstore_->asyncAtomicOp(spaceId, partId, atomicOp, callback);
}

}  // namespace storage
}  // namespace nebula
