/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_REMOVEHOSTSPROCESSOR_H_
#define META_REMOVEHOSTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RemoveHostsProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    /*
     *  xxxProcessor is self-management.
     *  The user should get instance when needed and don't care about the instance deleted.
     *  The instance should be destoryed inside when onFinished method invoked
     */
    static RemoveHostsProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemoveHostsProcessor(kvstore);
    }

    void process(const cpp2::RemoveHostsReq& req);

private:
    explicit RemoveHostsProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADDHOSTSPROCESSOR_H_
