/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_GETPROCESSOR_H_
#define META_GETPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetProcessor : public BaseProcessor<cpp2::GetResp> {
public:
    static GetProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetProcessor(kvstore);
    }

    void process(const cpp2::GetReq& req);

private:
    explicit GetProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETPROCESSOR_H_
