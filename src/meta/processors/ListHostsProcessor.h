/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_LISTHOSTSPROCESSOR_H_
#define META_LISTHOSTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListHostsProcessor : public BaseProcessor<cpp2::ListHostsResp> {
public:
    static ListHostsProcessor* instance(kvstore::KVStore* kvstore, folly::RWSpinLock* lock) {
        return new ListHostsProcessor(kvstore, lock);
    }

    void process(const cpp2::ListHostsReq& req) {
        UNUSED(req);
        rh_ = std::make_unique<folly::RWSpinLock::ReadHolder>(*lock_);
        auto ret = allHosts();
        if (!ret.ok()) {
            resp_.set_code(cpp2::ErrorCode::E_NO_HOSTS);
            onFinished();
            return;
        }
        resp_.set_code(cpp2::ErrorCode::SUCCEEDED);
        resp_.set_hosts(std::move(ret.value()));
        onFinished();
    }

private:
    explicit ListHostsProcessor(kvstore::KVStore* kvstore, folly::RWSpinLock* lock)
            : BaseProcessor<cpp2::ListHostsResp>(kvstore, lock) {}

private:
    std::unique_ptr<folly::RWSpinLock::ReadHolder> rh_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTHOSTSPROCESSOR_H_
