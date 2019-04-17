/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_REMOVEPROCESSOR_H_
#define META_REMOVEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class RemoveProcessor : public BaseProcessor<cpp2::RemoveResp> {
public:
    static RemoveProcessor* instance(kvstore::KVStore* kvstore) {
        return new RemoveProcessor(kvstore);
    }

    void process(const cpp2::RemoveReq& req);

private:
    explicit RemoveProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::RemoveResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DELETEPROCESSOR_H_
