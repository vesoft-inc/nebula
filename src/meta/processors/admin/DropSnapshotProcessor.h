/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_DROPSNAPSHOTPROCESSOR_H_
#define META_DROPSNAPSHOTPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class DropSnapshotProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropSnapshotProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropSnapshotProcessor(kvstore);
    }
    void process(const cpp2::DropSnapshotReq& req);

private:
    explicit DropSnapshotProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif  // META_DROPSNAPSHOTPROCESSOR_H_
