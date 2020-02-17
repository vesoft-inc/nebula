/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_RENAMESPACEPROCESSOR_H
#define META_RENAMESPACEPROCESSOR_H
#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RenameSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RenameSpaceProcessor* instance(kvstore::KVStore* kvstore) {
        return new RenameSpaceProcessor(kvstore);
    }

    void process(const cpp2::RenameSpaceReq& req);

private:
    explicit RenameSpaceProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_RENAMESPACEPROCESSOR_H

