/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LISTPLUGINSPROCESSOR_H_
#define META_LISTPLUGINSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListPluginsProcessor : public BaseProcessor<cpp2::ListPluginsResp> {
public:
    static ListPluginsProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListPluginsProcessor(kvstore);
    }
    void process(const cpp2::ListPluginsReq& req);

private:
    explicit ListPluginsProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ListPluginsResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif  // META_LISTPLUGINSPROCESSOR_H_
