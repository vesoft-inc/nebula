/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_LISTSPACESPROCESSOR_H_
#define META_LISTSPACESPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListSpacesProcessor : public BaseProcessor<cpp2::ListSpacesResp> {
public:
    static ListSpacesProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListSpacesProcessor(kvstore);
    }

    void process(const cpp2::ListSpacesReq& req);

private:
    explicit ListSpacesProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ListSpacesResp>(kvstore) {}

private:
    std::unique_ptr<folly::RWSpinLock::ReadHolder> rHolder_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTSPACESPROCESSOR_H_
