/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#pragma once
#include "common/base/ErrorOr.h"
#include "common/datatypes/DataSet.h"
#include "common/time/Duration.h"
#include "folly/AtomicLinkedList.h"
#include "folly/container/F14Map.h"
#include "interface/gen-cpp2/common_types.h"
#include "storage/CommonUtils.h"
namespace nebula {
namespace storage {
/**
 * IndexNode
 *
 * Indexnode is the base class for each node that makes up the plan tree.
 *
 * Member:
 * `children_`      : all children of the node.
 * `context_`       : runtime context of plan.
 * `name_`          : node name which should be set in derive node.
 * `duration_`      : used to record execution time(exclude children node's time).
 * `profileDetail_` : whether record execution time or not.
 *
 * Function:
 * The functions is divided into three parts.
 *
 * First part is used to build node. This part include constructor/destructor, and
 * `IndexNode(const IndexNode& node)` is used to cooperate with `copy` to realize
 * the deep copy of node.`copy` function needs to be implemented by the derived
 * class itself.
 * In fact, the build process is divided into two stages. First, the user needs to
 * make various derived classes and nd organize them into a plan tree(by
 * `children_`).After that, the root node of plan tree needs to call the init
 * function and recursively call the init function of all child nodes, `Initcontext`
 * will pass parameters between nodes to determine the data format or other
 * information to be returned between nodes during execution.Note that `init` needs
 * to be executed before `copy`.
 *
 * Second part is used to access data.
 * `execute` is used to initialize some variables at the beginning of each part(e.g
 * dedup set, kv iterator, etc.)
 * `next` is used to iterate data. Row format has been determined during `init`.
 * Batch processing and loop unrolling can be used to optimize performance if
 * necessary, but there are no serious performance problems at present.
 * `end` and `finish` are used to release resources at the end of execute or plan
 * (e.g, external sort free disk,release schema lease if support Online DDL, commit
 * write, etc.).
 * However, there are no relevant requirements, so it will not be implemented for
 * the time being.
 * `xxx` is the interface function.It will recursive call child node's `xxx`. `doXxx`
 * is the actual execution logic, and the derived class needs to override this
 * function
 *
 * The third part is used to assist in obtaining some detailed information
 */

using ErrorCode = ::nebula::cpp2::ErrorCode;
template <typename K, typename V>
using Map = folly::F14FastMap<K, V>;
template <typename K>
using Set = folly::F14FastSet<K>;
struct InitContext {
  // Column required by parent node
  Set<std::string> requiredColumns;
  // The format of the row returned to the parent node
  std::vector<std::string> returnColumns;
  // The index of name in `returncolumns`
  Map<std::string, size_t> retColMap;
};

class IndexNode {
 public:
  /* Iterate result*/
  class Result {
   public:
    Result() : code_(ErrorCode::SUCCEEDED), empty_(true) {}
    Result(const Result& result) : code_(result.code_), row_(result.row_), empty_(result.empty_) {}
    Result(Result&& result)
        : code_(result.code_), row_(std::move(result.row_)), empty_(result.empty_) {}
    explicit Result(ErrorCode code) : code_(code), empty_(true) {}
    explicit Result(Row&& row) : row_(row), empty_(false) {}
    Result& operator=(Result&& result) {
      this->code_ = result.code_;
      this->row_ = std::move(result.row_);
      this->empty_ = result.empty_;
      return *this;
    }
    inline bool success() {
      return code_ == ErrorCode::SUCCEEDED;
    }
    inline bool hasData() {
      return success() && empty_ == false;
    }
    inline Row row() && {
      return std::move(row_);
    }
    inline Row& row() & {
      return row_;
    }
    ErrorCode code() {
      return code_;
    }

   private:
    ErrorCode code_{ErrorCode::SUCCEEDED};
    Row row_;
    bool empty_{true};
  };
  /* build */
  IndexNode(const IndexNode& node);
  explicit IndexNode(RuntimeContext* context, const std::string& name);
  virtual ~IndexNode() = default;
  virtual std::unique_ptr<IndexNode> copy() = 0;
  void addChild(std::unique_ptr<IndexNode> child) {
    children_.emplace_back(std::move(child));
  }
  const std::vector<std::unique_ptr<IndexNode>>& children() {
    return children_;
  }
  virtual ::nebula::cpp2::ErrorCode init(InitContext& initCtx) {
    DCHECK_EQ(children_.size(), 1);
    return children_[0]->init(initCtx);
  }
  /* execution */
  inline nebula::cpp2::ErrorCode execute(PartitionID partId);
  inline Result next();
  // inline nebula::cpp2::ErrorCode finish();

  /* assist */
  const std::string& name() {
    return name_;
  }
  void enableProfileDetail();
  virtual std::string identify() = 0;
  inline const time::Duration& duration();

 protected:
  virtual Result doNext() = 0;
  void beforeNext();
  void afterNext();
  virtual nebula::cpp2::ErrorCode doExecute(PartitionID partId);
  void beforeExecute();
  void afterExecute();

  RuntimeContext* context_;
  GraphSpaceID spaceId_;
  std::vector<std::unique_ptr<IndexNode>> children_;
  std::string name_;
  time::Duration duration_;
  bool profileDetail_{false};
};

/* Defination of inline function */
inline IndexNode::Result IndexNode::next() {
  beforeNext();
  if (context_->isPlanKilled()) {
    return Result(::nebula::cpp2::ErrorCode::E_PLAN_IS_KILLED);
  }
  Result ret = doNext();
  afterNext();
  return ret;
}

inline void IndexNode::beforeNext() {
  if (UNLIKELY(profileDetail_)) {
    duration_.resume();
  }
}

inline void IndexNode::afterNext() {
  if (UNLIKELY(profileDetail_)) {
    duration_.pause();
  }
}

inline nebula::cpp2::ErrorCode IndexNode::execute(PartitionID partId) {
  beforeExecute();
  auto ret = doExecute(partId);
  afterExecute();
  return ret;
}

inline void IndexNode::beforeExecute() {
  if (UNLIKELY(profileDetail_)) {
    duration_.resume();
  }
}

inline void IndexNode::afterExecute() {
  if (UNLIKELY(profileDetail_)) {
    duration_.pause();
  }
}

inline void IndexNode::enableProfileDetail() {
  profileDetail_ = true;
  for (auto& child : children_) {
    child->enableProfileDetail();
  }
}

inline const time::Duration& IndexNode::duration() {
  return duration_;
}

}  // namespace storage
}  // namespace nebula
