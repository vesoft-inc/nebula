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
    static ListSpacesProcessor* instance(kvstore::KVStore* kvstore, std::mutex* lock) {
        return new ListSpacesProcessor(kvstore, lock);
    }

    void process(const cpp2::ListSpacesReq& req) {
        UNUSED(req);
        guard_ = std::make_unique<std::lock_guard<std::mutex>>(*lock_);
        auto prefix = MetaUtils::spacePrefix();
        std::unique_ptr<kvstore::KVIterator> iter;
        auto ret = kvstore_->prefix(kDefaultSpaceId_, kDefaultPartId_, prefix, &iter);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            resp_.set_code(to(ret));
            onFinished();
            return;
        }
        std::vector<cpp2::IdName> spaces;
        while (iter->valid()) {
            auto spaceId = MetaUtils::spaceId(iter->key());
            auto spaceName = MetaUtils::spaceName(iter->val());
            spaces.emplace_back(apache::thrift::FragileConstructor::FRAGILE,
                                to(spaceId, IDType::SPACE),
                                spaceName.str());
            iter->next();
        }
        resp_.set_spaces(std::move(spaces));
        onFinished();
    }

private:
    explicit ListSpacesProcessor(kvstore::KVStore* kvstore, std::mutex* lock)
            : BaseProcessor<cpp2::ListSpacesResp>(kvstore, lock) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LISTSPACESPROCESSOR_H_
