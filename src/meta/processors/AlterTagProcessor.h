/* Copyright (c) 2018 - present, VE Software Inc. All rights reserved
 *
 * This source code is licensed under Apache 2.0 License
 *  (found in the LICENSE.Apache file in the root directory)
 */

#ifndef META_ALTERTAGPROCESSOR_H_
#define META_ALTERTAGPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"

namespace nebula {
namespace meta {

class AlterTagProcessor : public BaseProcessor<cpp2::ExecResp> {
public:
    static AlterTagProcessor* instance(kvstore::KVStore* kvstore) {
        return new AlterTagProcessor(kvstore);
    }

    void process(const cpp2::AlterTagReq& req);

private:
    explicit AlterTagProcessor(kvstore::KVStore* kvstore)
            : BaseProcessor<cpp2::ExecResp>(kvstore) {}

    cpp2::ErrorCode alterColumnDefs(std::vector<nebula::cpp2::ColumnDef>& cols,
                                    const nebula::cpp2::ColumnDef col,
                                    const cpp2::AlterSchemaOp op);
};

}  // namespace meta
}  // namespace nebula
#endif  // META_ALTERTAGPROCESSOR_H_

