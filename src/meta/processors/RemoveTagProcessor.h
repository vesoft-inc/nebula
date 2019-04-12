/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_REMOVETAGPROCESSOR_H
#define NEBULA_GRAPH_REMOVETAGPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RemoveTagProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RemoveTagProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemoveTagProcessor(kvstore);
    }

    void process(const cpp2::RemoveTagReq& req);

private:
    explicit RemoveTagProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    StatusOr<std::vector<std::string>> getTagKeys(GraphSpaceID id, const std::string& tagName);
};

}  // namespace meta
}  // namespace nebula
#endif  // NEBULA_GRAPH_REMOVETAGPROCESSOR_H
