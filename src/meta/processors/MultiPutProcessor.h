/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_MULTIPUTPROCESSOR_H_
#define META_MULTIPUTPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class MultiPutProcessor : public BaseProcessor<cpp2::MultiPutResp> {
public:
    static MultiPutProcessor* instance(kvstore::KVStore* kvstore) {
        return new MultiPutProcessor(kvstore);
    }

    void process(const cpp2::MultiPutReq& req);

private:
    explicit MultiPutProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::MultiPutResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_MULTIPUTPROCESSOR_H_
