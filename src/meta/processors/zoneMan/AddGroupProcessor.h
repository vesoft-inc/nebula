/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADDGROUPPROCESSOR_H
#define META_ADDGROUPPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddGroupProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AddGroupProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddGroupProcessor(kvstore);
    }

    void process(const cpp2::AddGroupReq& req);

private:
    explicit AddGroupProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    nebula::cpp2::ErrorCode  checkGroupRedundancy(std::vector<std::string> zones);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADDGROUPPROCESSOR_H
