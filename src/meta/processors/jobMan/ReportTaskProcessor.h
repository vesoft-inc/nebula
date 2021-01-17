/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_REPORTTASKPROCESSOR_H_
#define META_REPORTTASKPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {
class ReportTaskProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static ReportTaskProcessor* instance(kvstore::KVStore* kvstore) {
        return new ReportTaskProcessor(kvstore);
    }

    void process(const cpp2::ReportTaskReq& req);

protected:
    explicit ReportTaskProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {}
};

}  // namespace meta
}  // namespace nebula

#endif  // META_REPORTTASKPROCESSOR_H_
