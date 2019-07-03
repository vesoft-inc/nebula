/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_HBPROCESSOR_H_
#define META_HBPROCESSOR_H_

#include <gtest/gtest_prod.h>
#include "meta/processors/BaseProcessor.h"


namespace nebula {
namespace meta {

class HBProcessor : public BaseProcessor<cpp2::HBResp> {
    FRIEND_TEST(HBProcessorTest, HBTest);
    FRIEND_TEST(MetaClientTest, HeartbeatTest);

public:
    static HBProcessor* instance(kvstore::KVStore* kvstore) {
        return new HBProcessor(kvstore);
    }

    void process(const cpp2::HBReq& req);

private:
    explicit HBProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::HBResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_HBPROCESSOR_H_
