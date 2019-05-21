/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADDHOSTSPROCESSOR_H_
#define META_ADDHOSTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddHostsProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    /*
     *  xxxProcessor is self-management.
     *  The user should get instance when needed and don't care about the instance deleted.
     *  The instance should be destroyed inside when onFinished method invoked
     */
    static AddHostsProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddHostsProcessor(kvstore);
    }

    void process(const cpp2::AddHostsReq& req);

private:
    explicit AddHostsProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADDHOSTSPROCESSOR_H_
