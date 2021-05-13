/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_RELNODE_H_
#define STORAGE_EXEC_RELNODE_H_

#include "common/base/Base.h"
#include "common/context/ExpressionContext.h"
#include "utils/NebulaKeyUtils.h"
#include "storage/CommonUtils.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/query/QueryBaseProcessor.h"
#include "storage/exec/QueryUtils.h"
#include "storage/exec/StorageIterator.h"

namespace nebula {
namespace storage {

using NullHandler = std::function<nebula::cpp2::ErrorCode(const std::vector<PropContext>*)>;

using PropHandler = std::function<nebula::cpp2::ErrorCode(folly::StringPiece,
                                                          RowReader*,
                                                          const std::vector<PropContext>* props)>;

template<typename T> class StoragePlan;

// RelNode is shortcut for relational algebra node, each RelNode has an execute method,
// which will be invoked in dag when all its dependencies have finished
template<typename T>
class RelNode {
    friend class StoragePlan<T>;

public:
    virtual nebula::cpp2::ErrorCode execute(PartitionID partId, const T& input) {
        for (auto* dependency : dependencies_) {
            auto ret = dependency->execute(partId, input);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    virtual nebula::cpp2::ErrorCode execute(PartitionID partId) {
        for (auto* dependency : dependencies_) {
            auto ret = dependency->execute(partId);
            if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return ret;
            }
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }

    void addDependency(RelNode<T>* dep) {
        dependencies_.emplace_back(dep);
        dep->hasDependents_ = true;
    }

    RelNode() = default;

    virtual ~RelNode() = default;

    explicit RelNode(const std::string& name): name_(name) {}

    std::string name_;
    std::vector<RelNode<T>*> dependencies_;
    bool hasDependents_ = false;
};

// QueryNode is the node which would read data from kvstore, it usually generate a row in response
// or a cell in a row.
template<typename T>
class QueryNode : public RelNode<T> {
public:
    const Value& result() {
        return result_;
    }

    Value& mutableResult() {
        return result_;
    }

protected:
    Value result_;
};

/*
IterateNode is a typical volcano node, it will have a upstream node. It keeps moving forward
the iterator by calling `next`. If you need to filter some data, implement the `check` just
like FilterNode.

The difference between QueryNode and IterateNode is that, the latter one derives from
StorageIterator, which makes IterateNode has a output of RowReader. If the reader is not null,
user can get property from the reader.
*/
template<typename T>
class IterateNode : public QueryNode<T>, public StorageIterator {
public:
    IterateNode() = default;

    explicit IterateNode(IterateNode* node) : upstream_(node) {}

    bool valid() const override {
        return upstream_->valid();
    }

    void next() override {
        do {
            upstream_->next();
        } while (upstream_->valid() && !check());
    }

    folly::StringPiece key() const override {
        return upstream_->key();
    }

    folly::StringPiece val() const override {
        return upstream_->val();
    }

    // return the edge row reader which could pass filter
    RowReader* reader() const override {
        return upstream_->reader();
    }

protected:
    // return true when the iterator points to a valid value
    virtual bool check() {
        return true;
    }

    IterateNode* upstream_;
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_RELNODE_H_
