/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_GETPARTSALLOCPROCESSOR_H_
#define META_GETPARTSALLOCPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetPartsAllocProcessor : public BaseProcessor<cpp2::GetPartsAllocResp> {
public:
    static GetPartsAllocProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetPartsAllocProcessor(kvstore);
    }

    void process(const cpp2::GetPartsAllocReq& req);

private:
    explicit GetPartsAllocProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetPartsAllocResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_GETPARTSALLOCPROCESSOR_H_
