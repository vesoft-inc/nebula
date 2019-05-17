/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REMOVEPROCESSOR_H_
#define META_REMOVEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

/**
 * Remove some rows in custorm kv operations.
 * */
class RemoveProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RemoveProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemoveProcessor(kvstore);
    }

    void process(const cpp2::RemoveReq& req);

private:
    explicit RemoveProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DELETEPROCESSOR_H_
