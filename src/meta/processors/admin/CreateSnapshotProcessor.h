/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_CREATESNAPSHOTPROCESSOR_H_
#define META_CREATESNAPSHOTPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

class CreateSnapshotProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static CreateSnapshotProcessor* instance(kvstore::KVStore* kvstore,
                                             AdminClient* client) {
        return new CreateSnapshotProcessor(kvstore, client);
    }
    void process(const cpp2::CreateSnapshotReq& req);

    nebula::cpp2::ErrorCode cancelWriteBlocking();

private:
    explicit CreateSnapshotProcessor(kvstore::KVStore* kvstore,
                                     AdminClient* client)
            : BaseProcessor<cpp2::ExecResp>(kvstore), client_(client) {}

private:
    AdminClient* client_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_CREATESNAPSHOTPROCESSOR_H_
