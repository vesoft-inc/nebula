/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_UPDATEGROUPPROCESSOR_H
#define META_UPDATEGROUPPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddZoneIntoGroupProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AddZoneIntoGroupProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddZoneIntoGroupProcessor(kvstore);
    }

    void process(const cpp2::AddZoneIntoGroupReq& req);

private:
    explicit AddZoneIntoGroupProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

class DropZoneFromGroupProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropZoneFromGroupProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropZoneFromGroupProcessor(kvstore);
    }

    void process(const cpp2::DropZoneFromGroupReq& req);

private:
    explicit DropZoneFromGroupProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_UPDATEGROUPPROCESSOR_H
