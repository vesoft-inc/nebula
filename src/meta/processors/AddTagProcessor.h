/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_ADDTAGPROCESSOR_H_
#define META_ADDTAGPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AddTagProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    /*
     *  xxxProcessor is self-management.
     *  The user should get instance when needed and don't care about the instance deleted.
     *  The instance should be destroyed inside when onFinished method invoked
     */
    static AddTagProcessor* instance(kvstore::KVStore* kvstore) {
        return new AddTagProcessor(kvstore);
    }

    void process(const cpp2::AddTagReq& req);

private:
    explicit AddTagProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    StatusOr<TagID> getTag(const std::string& tagName);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_ADDTAGPROCESSOR_H_
