/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_ADMINJOBPROCESSOR_H_
#define META_ADMINJOBPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class AdminJobProcessor : public BaseProcessor<cpp2::AdminJobResp> {
public:
    static AdminJobProcessor* instance(kvstore::KVStore* kvstore,
                                       AdminClient* adminClient) {
        return new AdminJobProcessor(kvstore, adminClient);
    }

    void process(const cpp2::AdminJobReq& req);

protected:
    explicit AdminJobProcessor(kvstore::KVStore* kvstore,
                               AdminClient* adminClient)
            : BaseProcessor<cpp2::AdminJobResp>(kvstore)
            , adminClient_(adminClient) {}

protected:
    AdminClient* adminClient_{nullptr};
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADMINJOBPROCESSOR_H_
