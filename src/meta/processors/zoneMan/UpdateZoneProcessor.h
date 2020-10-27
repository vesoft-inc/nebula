/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_UPDATEZONEPROCESSOR_H
#define META_UPDATEZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddHostIntoZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AddHostIntoZoneProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddHostIntoZoneProcessor(kvstore);
    }

    void process(const cpp2::AddHostIntoZoneReq& req);

private:
    explicit AddHostIntoZoneProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class DropHostFromZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropHostFromZoneProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropHostFromZoneProcessor(kvstore);
    }

    void process(const cpp2::DropHostFromZoneReq& req);

private:
    explicit DropHostFromZoneProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_UPDATEZONEPROCESSOR_H
