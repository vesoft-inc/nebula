/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CREATETAGPROCESSOR_H_
#define META_CREATETAGPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class CreateTagProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    /*
     *  xxxProcessor is self-management.
     *  The user should get instance when needed and don't care about the instance deleted.
     *  The instance should be destroyed inside when onFinished method invoked
     */
    static CreateTagProcessor* instance(kvstore::KVStore* kvstore) {
        return new CreateTagProcessor(kvstore);
    }

    void process(const cpp2::CreateTagReq& req);

private:
    explicit CreateTagProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATETAGPROCESSOR_H_
