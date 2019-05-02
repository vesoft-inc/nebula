/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_LISTTAGSPROCESSOR_H_
#define META_LISTTAGSPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class ListTagsProcessor : public BaseProcessor<cpp2::ListTagsResp> {
public:
    /*
     *  xxxProcessor is self-management.
     *  The user should get instance when needed and don't care about the instance deleted.
     *  The instance should be destroyed inside when onFinished method invoked
     */
    static ListTagsProcessor* instance(kvstore::KVStore* kvstore) {
        return new ListTagsProcessor(kvstore);
    }

    void process(const cpp2::ListTagsReq& req);

private:
    explicit ListTagsProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ListTagsResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTTAGSPROCESSOR_H_
