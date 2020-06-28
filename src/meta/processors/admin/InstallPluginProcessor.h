/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_INSTALLPLUGINPROCESSOR_H_
#define META_INSTALLPLUGINPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class InstallPluginProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static InstallPluginProcessor* instance(kvstore::KVStore* kvstore) {
        return new InstallPluginProcessor(kvstore);
    }
    void process(const cpp2::InstallPluginReq& req);

private:
    explicit InstallPluginProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};
}  // namespace meta
}  // namespace nebula

#endif  // META_INSTALLPLUGINPROCESSOR_H_
