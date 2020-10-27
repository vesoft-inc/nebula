/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTZONESPROCESSOR_H
#define META_LISTZONESPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListZonesProcessor : public BaseProcessor<cpp2::ListZonesResp> {
public:
    static ListZonesProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListZonesProcessor(kvstore);
    }

    void process(const cpp2::ListZonesReq& req);

private:
    explicit ListZonesProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ListZonesResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // META_LISTZONESPROCESSOR_H
