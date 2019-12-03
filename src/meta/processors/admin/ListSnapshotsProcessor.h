/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTSNAPSHOTSPROCESSOR_H_
#define META_LISTSNAPSHOTSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListSnapshotsProcessor : public BaseProcessor<cpp2::ListSnapshotsResp> {
public:
    static ListSnapshotsProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListSnapshotsProcessor(kvstore);
    }
    void process(const cpp2::ListSnapshotsReq& req);

private:
    explicit ListSnapshotsProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ListSnapshotsResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif  // META_LISTSNAPSHOTSPROCESSOR_H_
