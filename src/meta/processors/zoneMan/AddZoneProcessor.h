/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADDZONEPROCESSOR_H
#define META_ADDZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AddZoneProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddZoneProcessor(kvstore);
    }

    void process(const cpp2::AddZoneReq& req);

private:
    explicit AddZoneProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    nebula::cpp2::ErrorCode checkHostNotOverlap(const std::vector<HostAddr>& nodes);
};

}  // namespace meta
}  // namespace nebula
#endif  // META_ADDZONEPROCESSOR_H

