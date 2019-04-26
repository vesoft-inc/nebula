/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_REMOVEEDGEPROCESSOR_H_
#define META_REMOVEEDGEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RemoveEdgeProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static RemoveEdgeProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemoveEdgeProcessor(kvstore);
    }

    void process(const cpp2::RemoveEdgeReq& req);

 private:
    explicit RemoveEdgeProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REMOVEEDGEPROCESSOR_H_
