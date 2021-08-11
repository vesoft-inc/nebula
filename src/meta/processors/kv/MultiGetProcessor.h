/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_MULTIGETPROCESSOR_H_
#define META_MULTIGETPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class MultiGetProcessor : public BaseProcessor<cpp2::MultiGetResp> {
public:
    static MultiGetProcessor* instance(kvstore::KVStore* kvstore) {
        return new MultiGetProcessor(kvstore);
    }

    void process(const cpp2::MultiGetReq& req);

private:
    explicit MultiGetProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::MultiGetResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_MULTIGETPROCESSOR_H_
