/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_MULTIPUTPROCESSOR_H_
#define META_MULTIPUTPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class MultiPutProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static MultiPutProcessor* instance(kvstore::KVStore* kvstore) {
        return new MultiPutProcessor(kvstore);
    }

    void process(const cpp2::MultiPutReq& req);

private:
    explicit MultiPutProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_MULTIPUTPROCESSOR_H_
