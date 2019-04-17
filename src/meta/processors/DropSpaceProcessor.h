/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_DROPSPACEPROCESSOR_H_
#define META_DROPSPACEPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class DropSpaceProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropSpaceProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropSpaceProcessor(kvstore);
    }

    void process(const cpp2::DropSpaceReq& req);

private:
    explicit DropSpaceProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPSPACEPROCESSOR_H_
