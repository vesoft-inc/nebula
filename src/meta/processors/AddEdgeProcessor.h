/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_ADDEDGEPROCESSOR_H_
#define META_ADDEDGEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddEdgeProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AddEdgeProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddEdgeProcessor(kvstore);
    }

    void process(const cpp2::AddEdgeReq& req) {
        UNUSED(req);
    }

private:
    explicit AddEdgeProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADDEDGEPROCESSOR_H_
