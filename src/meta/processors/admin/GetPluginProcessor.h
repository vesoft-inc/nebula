/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_GETPLUGINPROCESSOR_H_
#define META_GETPLUGINPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetPluginProcessor : public BaseProcessor<cpp2::GetPluginResp> {
public:
    static GetPluginProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetPluginProcessor(kvstore);
    }
    void process(const cpp2::GetPluginReq& req);

private:
    explicit GetPluginProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetPluginResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif  // META_GETPLUGINPROCESSOR_H_
