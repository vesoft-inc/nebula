/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GLOBALSESSIONPROCESSOR_H_
#define META_GLOBALSESSIONPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddSessionProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AddSessionProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddSessionProcessor(kvstore);
    }

    void process(const cpp2::AddSessionReq& req);

private:
    explicit AddSessionProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
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


class UpdateSessionProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static UpdateSessionProcessor* instance(kvstore::KVStore* kvstore) {
        return new UpdateSessionProcessor(kvstore);
    }

    void process(const cpp2::UpdateSessionReq& req);

private:
    explicit UpdateSessionProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
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

}   // namespace meta
}   // namespace nebula
#endif  // META_GLOBALSESSIONPROCESSOR_H_
