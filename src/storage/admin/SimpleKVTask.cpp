/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/SimpleKVTask.h"
#include "common/base/Logging.h"

namespace nebula {
namespace storage {

nebula::kvstore::ResultCode SimpleKVTask::run() {
    LOG(ERROR) << "messi " << __PRETTY_FUNCTION__;
    auto rc = nebula::kvstore::ResultCode::ERR_INVALID_ARGUMENT;
    switch (cmd_) {
    case nebula::cpp2::AdminCmd::COMPACT:
        rc = store_->compact(spaceId_);
        break;
    case nebula::cpp2::AdminCmd::FLUSH:
        rc = store_->flush(spaceId_);
        break;
    default:
        LOG(ERROR) << __PRETTY_FUNCTION__ << " invalid command ";
        return nebula::kvstore::ResultCode::ERR_INVALID_ARGUMENT;
    }
    return rc;
}

nebula::kvstore::ResultCode SimpleKVTask::stop() {
    LOG(ERROR) << "not implement function " << __PRETTY_FUNCTION__;
    return nebula::kvstore::ResultCode::SUCCEEDED;
}

}  // namespace storage
}  // namespace nebula
