/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SESSIONMANAGERPROCESSOR_H
#define META_SESSIONMANAGERPROCESSOR_H

#include "meta/processors/BaseProcessor.h"
#include "meta/common/MetaCommon.h"

namespace nebula {
namespace meta {

class CreateSessionProcessor : public BaseProcessor<cpp2::CreateSessionResp> {
public:
    static CreateSessionProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateSessionProcessor(kvstore);
    }

    void process(const cpp2::CreateSessionReq& req);

private:
    explicit CreateSessionProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::CreateSessionResp>(kvstore) {}
};


class UpdateSessionsProcessor : public BaseProcessor<cpp2::UpdateSessionsResp> {
public:
    static UpdateSessionsProcessor* instance(kvstore::KVStore* kvstore) {
        return new UpdateSessionsProcessor(kvstore);
    }

    void process(const cpp2::UpdateSessionsReq& req);

private:
    explicit UpdateSessionsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::UpdateSessionsResp>(kvstore) {}
};


class ListSessionsProcessor : public BaseProcessor<cpp2::ListSessionsResp> {
public:
    static ListSessionsProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListSessionsProcessor(kvstore);
    }

    void process(const cpp2::ListSessionsReq& req);

private:
    explicit ListSessionsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ListSessionsResp>(kvstore) {}
};


class GetSessionProcessor : public BaseProcessor<cpp2::GetSessionResp> {
public:
    static GetSessionProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetSessionProcessor(kvstore);
    }

    void process(const cpp2::GetSessionReq& req);

private:
    explicit GetSessionProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::GetSessionResp>(kvstore) {}
};

class RemoveSessionProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RemoveSessionProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemoveSessionProcessor(kvstore);
    }

    void process(const cpp2::RemoveSessionReq& req);

private:
    explicit RemoveSessionProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class KillQueryProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static KillQueryProcessor* instance(kvstore::KVStore* kvstore) {
        return new KillQueryProcessor(kvstore);
    }

    void process(const cpp2::KillQueryReq& req);

private:
    explicit KillQueryProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula
#endif  // META_SESSIONMANAGERPROCESSOR_H
