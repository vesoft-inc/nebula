/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTTAGINDEXSTATUSPROCESSOR_H_
#define META_LISTTAGINDEXSTATUSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/JobDescription.h"

namespace nebula {
namespace meta {

class ListTagIndexStatusProcessor : public BaseProcessor<cpp2::ListIndexStatusResp> {
public:
    static ListTagIndexStatusProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListTagIndexStatusProcessor(kvstore);
    }

    void process(const cpp2::ListIndexStatusReq& req);

private:
    explicit ListTagIndexStatusProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ListIndexStatusResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTTAGINDEXSTATUSPROCESSOR_H_
