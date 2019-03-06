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
    static ListHostsProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListHostsProcessor(kvstore);
    }

    void process(const cpp2::ListHostsReq& req);

private:
    explicit ListHostsProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ListHostsResp>(kvstore) {}

private:
    std::unique_ptr<folly::RWSpinLock::ReadHolder> rHolder_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTHOSTSPROCESSOR_H_
