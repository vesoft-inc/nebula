/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GETZONEPROCESSOR_H
#define META_GETZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetZoneProcessor : public BaseProcessor<cpp2::GetZoneResp> {
public:
    static GetZoneProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetZoneProcessor(kvstore);
    }

    void process(const cpp2::GetZoneReq& req);

private:
    explicit GetZoneProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::GetZoneResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_GETZONEPROCESSOR_H
