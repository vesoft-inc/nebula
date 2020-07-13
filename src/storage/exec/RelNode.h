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

using NullHandler = std::function<kvstore::ResultCode(const std::vector<PropContext>*)>;

using TagPropHandler = std::function<kvstore::ResultCode(TagID,
                                                         RowReader*,
                                                         const std::vector<PropContext>* props)>;

using EdgePropHandler = std::function<kvstore::ResultCode(EdgeType,
                                                          folly::StringPiece,
                                                          RowReader*,
                                                          const std::vector<PropContext>* props)>;

template<typename T> class StoragePlan;

// RelNode is shortcut for relational algebra node, each RelNode has an execute method,
// which will be invoked in dag when all its dependencies have finished
template<typename T>
class RelNode {
    friend class StoragePlan<T>;
public:
    virtual kvstore::ResultCode execute(PartitionID partId, const T& input) {
        for (auto* dependency : dependencies_) {
            auto ret = dependency->execute(partId, input);
            if (ret != kvstore::ResultCode::SUCCEEDED) {
                return ret;
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
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
    // if yields is not empty, will eval the yield expression, otherwize, collect property
    kvstore::ResultCode collectEdgeProps(
            EdgeType edgeType,
            RowReader* reader,
            folly::StringPiece key,
            size_t vIdLen,
            const std::vector<PropContext>* props,
            nebula::List& list) {
        for (const auto& prop : *props) {
            if (prop.returned_) {
                auto srcId = NebulaKeyUtils::getSrcId(vIdLen, key);
                auto edgeRank = NebulaKeyUtils::getRank(vIdLen, key);
                auto dstId = NebulaKeyUtils::getDstId(vIdLen, key);
                VLOG(2) << "Collect prop " << prop.name_ << ", type " << edgeType;
                auto value = QueryUtils::readEdgeProp(srcId, edgeType, edgeRank, dstId,
                                                      reader, prop);
                if (!value.ok()) {
                    return kvstore::ResultCode::ERR_EDGE_PROP_NOT_FOUND;
                }
                list.values.emplace_back(std::move(value).value());
            }
        }

        return kvstore::ResultCode::SUCCEEDED;
    }

    // Always put filter property into expression context.
    // if yields is not empty, will eval the yield expression, otherwize, collect property.
    kvstore::ResultCode collectTagProps(
            TagID tagId,
            const std::string& tagName,
            RowReader* reader,
            const std::vector<PropContext>* props,
            nebula::List& list,
            StorageExpressionContext* ctx = nullptr) {
        for (auto& prop : *props) {
            VLOG(2) << "Collect prop " << prop.name_ << ", type " << tagId;
            auto status = QueryUtils::readValue(reader, prop);
            if (!status.ok()) {
                return kvstore::ResultCode::ERR_TAG_PROP_NOT_FOUND;
            }
            auto value = std::move(status).value();
            if (ctx != nullptr && prop.filtered_) {
                ctx->setTagProp(tagName, prop.name_, value);
            }
            if (prop.returned_) {
                list.values.emplace_back(std::move(value));
            }
        }
        return kvstore::ResultCode::SUCCEEDED;
    }

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
