/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_REMOVERANGEPROCESSOR_H_
#define META_REMOVERANGEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RemoveRangeProcessor : public BaseProcessor<cpp2::RemoveRangeResp> {
public:
    static RemoveRangeProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemoveRangeProcessor(kvstore);
    }

    void process(const cpp2::RemoveRangeReq& req);

private:
    explicit RemoveRangeProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::RemoveRangeResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REMOVERANGEPROCESSOR_H_
