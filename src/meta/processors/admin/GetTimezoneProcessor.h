/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_PROCESSORS_GETTIMEZONEPROCESSOR_H
#define META_PROCESSORS_GETTIMEZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetTimezoneProcessor : public BaseProcessor<cpp2::GetTimezoneResp> {
public:
    static GetTimezoneProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetTimezoneProcessor(kvstore);
    }

    void process(const cpp2::GetTimezoneReq&);

private:
    explicit GetTimezoneProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetTimezoneResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula
#endif  //  #define META_PROCESSORS_GETTIMEZONEPROCESSOR_H

