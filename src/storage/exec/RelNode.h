/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_RELNODE_H_
#define STORAGE_EXEC_RELNODE_H_

#include "common/base/Base.h"
#include "common/context/ExpressionContext.h"
#include "common/time/Duration.h"
#include "common/utils/NebulaKeyUtils.h"
#include "storage/CommonUtils.h"
#include "storage/context/StorageExpressionContext.h"
#include "storage/exec/QueryUtils.h"
#include "storage/exec/StorageIterator.h"
#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

using NullHandler = std::function<nebula::cpp2::ErrorCode(const std::vector<PropContext>*)>;

using PropHandler = std::function<nebula::cpp2::ErrorCode(
    folly::StringPiece, RowReader*, const std::vector<PropContext>* props)>;

template <typename T>
class StoragePlan;

// RelNode is shortcut for relational algebra node, each RelNode has an execute
// method, which will be invoked in dag when all its dependencies have finished
template <typename T>
class RelNode {
  friend class StoragePlan<T>;

 public:
  virtual nebula::cpp2::ErrorCode execute(PartitionID partId, const T& input) {
    duration_.resume();
    auto ret = doExecute(partId, input);
    duration_.pause();
    return ret;
  }
  virtual nebula::cpp2::ErrorCode doExecute(PartitionID partId, const T& input) {
    for (auto* dependency : dependencies_) {
      auto ret = dependency->execute(partId, input);
      if (ret != nebula::cpp2::ErrorCode::SUCCEEDED) {
        return ret;
      }
    }
    return nebula::cpp2::ErrorCode::SUCCEEDED;
  }

  virtual nebula::cpp2::ErrorCode execute(PartitionID partId) {
    duration_.resume();
    auto ret = doExecute(partId);
    duration_.pause();
    return ret;
  }

  virtual nebula::cpp2::ErrorCode doExecute(PartitionID partId) {
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

  const std::string& name() const { return name_; }
  int32_t latencyInUs() const { return duration_.elapsedInUSec(); }

  GraphSpaceID space() const { return context_->spaceId(); }
  size_t vIdLen() const { return context_->vIdLen(); }
  bool isIntId() const { return context_->isIntId(); }
  bool isPlanKilled() const { return context_->isPlanKilled(); }
  bool isEdge() const { return context_->isEdge(); }

  RuntimeContext* context() const { return context_; }
  StorageEnv* env() const { return context_->env(); }

  kvstore::KVStore* kvstore() const { return env()->kvstore_; }
  meta::IndexManager* idxMgr() const { return env()->indexMan_; }
  meta::SchemaManager* schemaMgr() const { return env()->schemaMan_; }

  const meta::NebulaSchemaProvider* schema() const {
    return isEdge() ? context_->edgeSchema_ : context_->tagSchema_;
  }

  std::string vertexKey(PartitionID partId, VertexID vId) const {
    return NebulaKeyUtils::vertexKey(this->vIdLen(), partId, vId, context_->tagId_);
  }

  std::string edgeKey(PartitionID partId,
                      const std::string& src,
                      const std::string& dst,
                      EdgeRanking rank) const {
    return edgeKey(partId, src, dst, rank, context_->edgeType_);
  }

  std::string edgeKey(PartitionID partId,
                      const std::string& src,
                      const std::string& dst,
                      EdgeRanking rank,
                      EdgeType type) const {
    return NebulaKeyUtils::edgeKey(vIdLen(), partId, src, type, rank, dst);
  }

  auto get(PartitionID partId, const std::string& key, std::string* val) const {
    return kvstore()->get(space(), partId, key, val);
  }

  auto range(PartitionID partId,
             const std::string& begin,
             const std::string& end,
             std::unique_ptr<kvstore::KVIterator>* iter) const {
    return kvstore()->range(space(), partId, begin, end, iter);
  }

  auto prefix(PartitionID partId,
              const std::string& prefix,
              std::unique_ptr<kvstore::KVIterator>* iter) const {
    return kvstore()->prefix(space(), partId, prefix, iter);
  }

  virtual ~RelNode() = default;

  explicit RelNode(RuntimeContext* context = nullptr, const std::string& name = "RelNode")
      : context_(context), name_(name) {}

 protected:
  RuntimeContext* context_;
  std::string name_;
  std::vector<RelNode<T>*> dependencies_;
  bool hasDependents_ = false;
  time::Duration duration_{true};
};

// QueryNode is the node which would read data from kvstore, it usually generate
// a row in response or a cell in a row.
template <typename T>
class QueryNode : public RelNode<T> {
 public:
  const Value& result() { return result_; }
  Value& mutableResult() { return result_; }

 protected:
  QueryNode(RuntimeContext* context, const std::string& name) : RelNode<T>(context, name) {}

  Value result_;
};

/**
 * IterateNode is a typical volcano node, it will have a upstream node. It keeps
 * moving forward the iterator by calling `next`. If you need to filter some data,
 * implement the `check` just like FilterNode.
 *
 * The difference between QueryNode and IterateNode is that, the latter one derives
 * from StorageIterator, which makes IterateNode has a output of RowReader. If the
 * reader is not null, user can get property from the reader.
 */
template <typename T>
class IterateNode : public QueryNode<T>, public StorageIterator {
 public:
  explicit IterateNode(RuntimeContext* context,
                       const std::string& name = "IterateNode",
                       IterateNode* node = nullptr)
      : QueryNode<T>(context, name), StorageIterator(), upstream_(node) {}

  bool valid() const override { return upstream_->valid(); }

  void next() override {
    do {
      upstream_->next();
    } while (upstream_->valid() && !check());
  }

  folly::StringPiece key() const override { return upstream_->key(); }

  folly::StringPiece val() const override { return upstream_->val(); }

  // return the edge row reader which could pass filter
  RowReader* reader() const override { return upstream_->reader(); }

 protected:
  // return true when the iterator points to a valid value
  virtual bool check() { return true; }

  IterateNode* upstream_{nullptr};
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_RELNODE_H_
