/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GETSTATISPROCESSOR_H_
#define META_GETSTATISPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetStatisProcessor : public BaseProcessor<cpp2::GetStatisResp> {
public:
    static GetStatisProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetStatisProcessor(kvstore);
    }

    void process(const cpp2::GetStatisReq& req);

private:
    explicit GetStatisProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetStatisResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETSTATISPROCESSOR_H_
