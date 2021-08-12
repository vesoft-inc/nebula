/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_SETCONFIGPROCESSOR_H
#define META_SETCONFIGPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class SetConfigProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static SetConfigProcessor* instance(kvstore::KVStore* kvstore) {
        return new SetConfigProcessor(kvstore);
    }

    void process(const cpp2::SetConfigReq& req);

    nebula::cpp2::ErrorCode
    setConfig(const cpp2::ConfigModule& module,
              const std::string& name,
              const Value& value,
              std::vector<kvstore::KV>& data);

private:
    explicit SetConfigProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    static std::unordered_set<std::string> mutableFields_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_SETCONFIGPROCESSOR_H
