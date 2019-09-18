/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SCANPROCESSOR_H_
#define META_SCANPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ScanProcessor : public BaseProcessor<cpp2::ScanResp> {
public:
    static ScanProcessor* instance(kvstore::KVStore* kvstore) {
        return new ScanProcessor(kvstore);
    }

    void process(const cpp2::ScanReq& req);

private:
    explicit ScanProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ScanResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SCANPROCESSOR_H_

