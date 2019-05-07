/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_HBPROCESSOR_H_
#define META_HBPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class HBProcessor : public BaseProcessor<cpp2::HBResp> {
public:
    static HBProcessor* instance(kvstore::KVStore* kvstore) {
        return new HBProcessor(kvstore);
    }

    void process(const cpp2::HBReq& req);

private:
    explicit HBProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_HBPROCESSOR_H_
