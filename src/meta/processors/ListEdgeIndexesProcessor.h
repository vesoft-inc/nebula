/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTEDGEINDEXESPROCESSOR_H
#define META_LISTEDGEINDEXESPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListEdgeIndexesProcessor : public BaseProcessor<cpp2::ListEdgeIndexesResp> {
public:
    static ListEdgeIndexesProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListEdgeIndexesProcessor(kvstore);
    }

    void process(const cpp2::ListEdgeIndexesReq& req);

private:
    explicit ListEdgeIndexesProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ListEdgeIndexesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTEDGEINDEXESPROCESSOR_H

