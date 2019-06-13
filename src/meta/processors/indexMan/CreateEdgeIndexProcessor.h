/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CREATEEDGEINDEXPROCESSOR_H
#define META_CREATEEDGEINDEXPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class CreateEdgeIndexProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static CreateEdgeIndexProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateEdgeIndexProcessor(kvstore);
    }

    void process(const cpp2::CreateEdgeIndexReq& req);

private:
    explicit CreateEdgeIndexProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATEEDGEINDEXPROCESSOR_H

