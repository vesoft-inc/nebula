/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_CREATEUSERPROCESSOR_H
#define NEBULA_GRAPH_CREATEUSERPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

enum RoleTypeE : RoleType {
    T_DEFAULT = 0x00,
    T_ADMIN = 0x01,
    T_USER =  0x02,
    T_GUEST = 0x03,
};

#define LOCK_CHECK(lock) \
auto& lock##Lock = LockUtils::lock##Lock(); \
if (!lock##Lock.try_lock_shared_for(std::chrono::microseconds(50))) { \
resp_.set_code(cpp2::ErrorCode::E_TABLE_LOCKED); \
onFinished(); \
return; \
} \

#define USER_CHECK(key, val) \
auto ret = kvstore_->get(kDefaultSpaceId_, kDefaultPartId_, key, &val); \
if (ret != kvstore::ResultCode::SUCCEEDED) { \
resp_.set_code(to(ret)); \
userLock.unlock_shared(); \
onFinished(); \
return; \
} \

class CreateUserProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static CreateUserProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateUserProcessor(kvstore);
    }

    void process(const cpp2::CreateUserReq& req);

private:
    explicit CreateUserProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class DropUserProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropUserProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropUserProcessor(kvstore);
    }

    void process(const cpp2::DropUserReq& req);

private:
    explicit DropUserProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class AlterUserProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AlterUserProcessor* instance(kvstore::KVStore* kvstore) {
        return new AlterUserProcessor(kvstore);
    }

    void process(const cpp2::AlterUserReq& req);

private:
    explicit AlterUserProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class GrantToUserProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static GrantToUserProcessor* instance(kvstore::KVStore* kvstore) {
        return new GrantToUserProcessor(kvstore);
    }

    void process(const cpp2::GrantToUserReq& req);

private:
    explicit GrantToUserProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class RevokeFromUserProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RevokeFromUserProcessor* instance(kvstore::KVStore* kvstore) {
        return new RevokeFromUserProcessor(kvstore);
    }

    void process(const cpp2::RevokeFromUserReq& req);

private:
    explicit RevokeFromUserProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class ListUsersProcessor : public BaseProcessor<cpp2::ListUsersResp> {
public:
    static ListUsersProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListUsersProcessor(kvstore);
    }

    void process(const cpp2::ListUsersReq& req);

private:
    explicit ListUsersProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ListUsersResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // NEBULA_GRAPH_CREATEUSERPROCESSOR_H
