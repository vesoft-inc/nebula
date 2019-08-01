/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTCONFIGPROCESSOR_H
#define META_LISTCONFIGPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListConfigsProcessor : public BaseProcessor<cpp2::ListConfigsResp> {
public:
    static ListConfigsProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListConfigsProcessor(kvstore);
    }

    void process(const cpp2::ListConfigsReq& req);

private:
    explicit ListConfigsProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ListConfigsResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTCONFIGPROCESSOR_H
