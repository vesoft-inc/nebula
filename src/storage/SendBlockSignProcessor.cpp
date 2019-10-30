/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/SendBlockSignProcessor.h"


namespace nebula {
namespace storage {

void SendBlockSignProcessor::process(const cpp2::EngineSignRequest& req) {
    auto sign = req.get_sign();
    switch (sign) {
        case cpp2::EngineSignType::BLOCK_ON :
            this->kvstore_->setBlocking(true);
            break;
        case cpp2::EngineSignType::BLOCK_OFF :
            this->kvstore_->setBlocking(false);
            break;
    }
    onFinished();
}

}  // namespace storage
}  // namespace nebula

