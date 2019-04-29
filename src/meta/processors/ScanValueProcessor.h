/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SCANVALUEPROCESSOR_H_
#define META_SCANVALUEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ScanValueProcessor : public BaseProcessor<cpp2::KeyOrValueScanResp> {
public:
    static ScanValueProcessor* instance(kvstore::KVStore* kvstore) {
        return new ScanValueProcessor(kvstore);
    }

    void process(const cpp2::ScanReq& req);

private:
    explicit ScanValueProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::KeyOrValueScanResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SCANVALUEPROCESSOR_H_
