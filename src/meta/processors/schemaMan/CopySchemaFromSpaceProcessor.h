/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_COPYSCHEMAFROMSPACEPROCESSOR_H
#define META_COPYSCHEMAFROMSPACEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class CopySchemaFromSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static CopySchemaFromSpaceProcessor* instance(kvstore::KVStore* kvstore) {
        return new CopySchemaFromSpaceProcessor(kvstore);
    }

    void process(const cpp2::CopySchemaFromSpaceReq& req);

private:
    explicit CopySchemaFromSpaceProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    bool isExist(const std::string &name);

    bool copySchemas(std::vector<kvstore::KV> &data);
    bool copyIndexes(std::vector<kvstore::KV> &data);

private:
    GraphSpaceID           currentSpaceId_{-1};
    GraphSpaceID           fromSpaceId_{-1};
};

}  // namespace meta
}  // namespace nebula
#endif  // META_COPYSCHEMAFROMSPACEPROCESSOR_H
