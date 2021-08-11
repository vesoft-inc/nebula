/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
#ifndef META_FTISERVICEPROCESSOR_H_
#define META_FTISERVICEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class SignInFTServiceProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static SignInFTServiceProcessor* instance(kvstore::KVStore* kvstore) {
        return new SignInFTServiceProcessor(kvstore);
    }

    void process(const cpp2::SignInFTServiceReq& req);

private:
    explicit SignInFTServiceProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class SignOutFTServiceProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static SignOutFTServiceProcessor* instance(kvstore::KVStore* kvstore) {
        return new SignOutFTServiceProcessor(kvstore);
    }

    void process(const cpp2::SignOutFTServiceReq& req);

private:
    explicit SignOutFTServiceProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class ListFTClientsProcessor : public BaseProcessor<cpp2::ListFTClientsResp> {
public:
    static ListFTClientsProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListFTClientsProcessor(kvstore);
    }

    void process(const cpp2::ListFTClientsReq& req);

private:
    explicit ListFTClientsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ListFTClientsResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif   // META_FTISERVICEPROCESSOR_H_
