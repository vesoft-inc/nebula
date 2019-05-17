/* Copyright (c) 2019 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_SCANKEYPROCESSOR_H_
#define META_SCANKEYPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ScanKeyProcessor : public BaseProcessor<cpp2::KeyOrValueScanResp> {
public:
    static ScanKeyProcessor* instance(kvstore::KVStore* kvstore) {
        return new ScanKeyProcessor(kvstore);
    }

    void process(const cpp2::ScanReq& req);

private:
    explicit ScanKeyProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::KeyOrValueScanResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SCANKEYPROCESSOR_H_
