/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_PUTPROCESSOR_H_
#define META_PUTPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class PutProcessor : public BaseProcessor<cpp2::PutResp> {
public:
    static PutProcessor* instance(kvstore::KVStore* kvstore) {
        return new PutProcessor(kvstore);
    }

    void process(const cpp2::PutReq& req);

private:
    explicit PutProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::PutResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_PUTPROCESSOR_H_
