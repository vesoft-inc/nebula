/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/SendBlockSignProcessor.h"

namespace nebula {
namespace storage {

void SendBlockSignProcessor::process(const cpp2::BlockingSignRequest& req) {
    auto spaceId = req.get_space_id();
    auto sign = req.get_sign() == cpp2::EngineSignType::BLOCK_ON;
    LOG(INFO) << "Receive block sign for space " << req.get_space_id() << ", block: " <<  sign;

    auto code = env_->kvstore_->setWriteBlocking(spaceId, sign);
    if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
        cpp2::PartitionResult thriftRet;
        thriftRet.set_code(code);
        codes_.emplace_back(std::move(thriftRet));
        LOG(INFO) << "set block sign failed, error: "
                  << apache::thrift::util::enumNameSafe(code);
    }
    onFinished();
}

}  // namespace storage
}  // namespace nebula

