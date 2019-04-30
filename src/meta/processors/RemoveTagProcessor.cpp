/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#include "meta/processors/RemoveTagProcessor.h"

namespace nebula {
namespace meta {

void RemoveTagProcessor::process(const cpp2::RemoveTagReq& req) {
    if (spaceExist(req.get_space_id()) == Status::NotFound()) {
        LOG(ERROR) << "RemoveTag Space " << req.get_space_id() << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    folly::SharedMutex::WriteHolder wHolder(LockUtils::tagLock());
    auto indexKey = MetaServiceUtils::tagIndexKey(req.get_space_id(), req.get_tag_name());
    auto indexResult = doGet(indexKey);
    if (!indexResult.ok()) {
        LOG(ERROR) << "RemoveTag Space " << req.get_space_id() << " TagName "
                   << req.get_tag_name() << " Not Found";
        resp_.set_code(cpp2::ErrorCode::E_NOT_FOUND);
        onFinished();
        return;
    }

    auto tagID = *reinterpret_cast<const TagID *>(indexResult.value().data());
    kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, indexKey,
                          [] (kvstore::ResultCode code, HostAddr leader) {
        UNUSED(code);
        UNUSED(leader);
    });
    doRemoveRange(MetaServiceUtils::schemaTagKey(req.get_space_id(), tagID, MAX_VERSION),
                  MetaServiceUtils::schemaTagKey(req.get_space_id(), tagID, MIN_VERSION));

    /**
    kvstore_->asyncRemoveRange(kDefaultSpaceId_, kDefaultPartId_,
                               MetaServiceUtils::schemaTagKey(req.get_space_id(), tagID, std::numeric_limits<int64_t>::max()),
                               MetaServiceUtils::schemaTagKey(req.get_space_id(), tagID, std::numeric_limits<int64_t>::min()),
                               [this] (kvstore::ResultCode code, HostAddr leader) {
       UNUSED(code);
       UNUSED(leader);
       });
    kvstore_->asyncRemove(kDefaultSpaceId_, kDefaultPartId_, indexKey,
                          [this] (kvstore::ResultCode code, HostAddr leader) {
       UNUSED(leader);
       this->resp_.set_code(to(code));
       this->onFinished();
       });
       **/
}


}  // namespace meta
}  // namespace nebula

