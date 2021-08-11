/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTEDGESSPROCESSOR_H_
#define META_LISTEDGESSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListEdgesProcessor : public BaseProcessor<cpp2::ListEdgesResp> {
public:
    /*
     *  xxxProcessor is self-management.
     *  The user should get instance when needed and don't care about the instance deleted.
     *  The instance should be destroyed inside when onFinished method invoked
     */
    static ListEdgesProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListEdgesProcessor(kvstore);
    }

    void process(const cpp2::ListEdgesReq& req);

private:
    explicit ListEdgesProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ListEdgesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTEDGESSPROCESSOR_H_
