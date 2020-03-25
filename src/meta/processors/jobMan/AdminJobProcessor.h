/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADMINJOBPROCESSOR_H_
#define META_ADMINJOBPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AdminJobProcessor : public BaseProcessor<cpp2::AdminJobResp> {
public:
    static AdminJobProcessor* instance(kvstore::KVStore* kvstore) {
        return new AdminJobProcessor(kvstore);
    }

    void process(const cpp2::AdminJobReq& req);

private:
    explicit AdminJobProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::AdminJobResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMINJOBPROCESSOR_H_
