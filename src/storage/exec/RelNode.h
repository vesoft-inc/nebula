/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_RELNODE_H_
#define STORAGE_EXEC_RELNODE_H_

#include "base/Base.h"
#include "common/NebulaKeyUtils.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

class RelNode {
    friend class StorageDAG;
public:
    virtual folly::Future<kvstore::ResultCode> execute() {
        DVLOG(1) << name_;
        return kvstore::ResultCode::SUCCEEDED;
    }

    void addDependency(RelNode* dep) {
        dependencies_.emplace_back(dep);
        dep->hasDependents_ = true;
    }

    RelNode() = default;

    explicit RelNode(const std::string& name): name_(name) {}

protected:
    StatusOr<nebula::Value> readValue(RowReader* reader, const PropContext& ctx) {
        auto value = reader->getValueByName(ctx.name_);
        if (value.type() == Value::Type::NULLVALUE) {
            // read null value
            auto nullType = value.getNull();
            if (nullType == NullType::BAD_DATA ||
                nullType == NullType::BAD_TYPE ||
                nullType == NullType::UNKNOWN_PROP) {
                VLOG(1) << "Fail to read prop " << ctx.name_;
                if (ctx.field_ != nullptr) {
                    if (ctx.field_->hasDefault()) {
                        return ctx.field_->defaultValue();
                    } else if (ctx.field_->nullable()) {
                        return NullType::__NULL__;
                    }
                }
            } else if (nullType == NullType::__NULL__ || nullType == NullType::NaN) {
                return value;
            }
            return Status::Error(folly::stringPrintf("Fail to read prop %s ", ctx.name_.c_str()));
        }
        return value;
    }

    std::string name_;
    folly::SharedPromise<kvstore::ResultCode> promise_;
    std::vector<RelNode*> dependencies_;
    bool hasDependents_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_RELNODE_H_
