/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_PARTIALSCANPROCESSOR_H_
#define META_PARTIALSCANPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class PartialScanProcessor : public BaseProcessor<cpp2::PartialScanResp> {
public:
    static PartialScanProcessor* instance(kvstore::KVStore* kvstore) {
        return new PartialScanProcessor(kvstore);
    }

    void process(const cpp2::PartialScanReq& req);

private:
    explicit PartialScanProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::PartialScanResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_PARTIALSCANPROCESSOR_H_
