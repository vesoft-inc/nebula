/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_GETUUIDPROCESSOR_H_
#define STORAGE_QUERY_GETUUIDPROCESSOR_H_

#include "base/Base.h"
#include "storage/BaseProcessor.h"

namespace nebula {
namespace storage {

class GetUUIDProcessor : public BaseProcessor<cpp2::GetUUIDResp> {
public:
    static GetUUIDProcessor* instance(kvstore::KVStore* kvstore,
                                      UUIDCache* uuidCache = nullptr) {
        return new GetUUIDProcessor(kvstore, uuidCache);
    }

    void process(const cpp2::GetUUIDReq& req);

private:
    explicit GetUUIDProcessor(kvstore::KVStore* kvstore, UUIDCache* uuidCache)
            : BaseProcessor<cpp2::GetUUIDResp>(kvstore, nullptr)
            , uuidCache_(uuidCache) {}

    UUIDCache* uuidCache_ = nullptr;
    VertexID vId_;
    bool succeeded_{false};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_QUERY_GETUUIDPROCESSOR_H_
