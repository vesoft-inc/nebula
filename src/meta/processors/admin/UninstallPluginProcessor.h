/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_UNINSTALLPLUGINPROCESSOR_H_
#define META_UNINSTALLPLUGINPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class UninstallPluginProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static UninstallPluginProcessor* instance(kvstore::KVStore* kvstore) {
        return new UninstallPluginProcessor(kvstore);
    }
    void process(const cpp2::UninstallPluginReq& req);

private:
    explicit UninstallPluginProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif  // META_UNINSTALLPLUGINPROCESSOR_H_
