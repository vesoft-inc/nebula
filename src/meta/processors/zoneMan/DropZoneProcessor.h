/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_DROPZONEPROCESSOR_H
#define META_DROPZONEPROCESSOR_H

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class DropZoneProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static DropZoneProcessor* instance(kvstore::KVStore* kvstore) {
        return new DropZoneProcessor(kvstore);
    }

    void process(const cpp2::DropZoneReq& req);

private:
    explicit DropZoneProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    nebula::cpp2::ErrorCode checkGroupDependency(const std::string& zoneName);
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DROPZONEPROCESSOR_H
