/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef NEBULA_GRAPH_GETTAGPROCESSOR_H
#define NEBULA_GRAPH_GETTAGPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class GetTagProcessor : public BaseProcessor<cpp2::GetTagResp> {
public:
    static GetTagProcessor* instance(kvstore::KVStore* kvstore) {
        return new GetTagProcessor(kvstore);
    }

    void process(const cpp2::GetTagReq& req);

private:
    explicit GetTagProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::GetTagResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula
#endif  // NEBULA_GRAPH_GETTAGPROCESSOR_H
