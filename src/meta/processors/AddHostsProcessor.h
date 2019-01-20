/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_ADDHOSTSPROCESSOR_H_
#define META_ADDHOSTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddHostsProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AddHostsProcessor* instance(kvstore::KVStore* kvstore, folly::RWSpinLock* lock) {
        return new AddHostsProcessor(kvstore, lock);
    }

    void process(const cpp2::AddHostsReq& req) {
        wh_ = std::make_unique<folly::RWSpinLock::WriteHolder>(*lock_);
        std::vector<kvstore::KV> data;
        for (auto& h : req.get_hosts()) {
            data.emplace_back(MetaUtils::hostKey(h.ip, h.port), MetaUtils::hostVal());
        }
        doPut(std::move(data));
    }

private:
    explicit AddHostsProcessor(kvstore::KVStore* kvstore, folly::RWSpinLock* lock)
            : BaseProcessor<cpp2::ExecResp>(kvstore, lock) {}

private:
    std::unique_ptr<folly::RWSpinLock::WriteHolder> wh_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADDHOSTSPROCESSOR_H_
