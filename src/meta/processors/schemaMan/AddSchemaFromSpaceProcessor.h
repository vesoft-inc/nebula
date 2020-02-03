/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADDSCHEMAFROMSPACEPROCESSOR_H
#define META_ADDSCHEMAFROMSPACEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddSchemaFromSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AddSchemaFromSpaceProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddSchemaFromSpaceProcessor(kvstore);
    }

    void process(const cpp2::AddSchemaFromSpaceReq& req);

private:
    explicit AddSchemaFromSpaceProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    bool isExist(const GraphSpaceID spaceId, const std::string &name);
};

}  // namespace meta
}  // namespace nebula
#endif  // META_ADDSCHEMAFROMSPACEPROCESSOR_H
