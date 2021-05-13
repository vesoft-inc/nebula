/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_DROPGROUPPROCESSOR_H
#define META_DROPGROUPPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class DropGroupProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropGroupProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropGroupProcessor(kvstore);
    }

    void process(const cpp2::DropGroupReq& req);

private:
    explicit DropGroupProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    nebula::cpp2::ErrorCode checkSpaceDependency(const std::string& groupName);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPGROUPPROCESSOR_H
