/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */
#ifndef STORAGE_EXEC_INDEXNODE_H
#define STORAGE_EXEC_INDEXNODE_H
#include "common/base/ErrorOr.h"
#include "common/datatypes/DataSet.h"
#include "common/time/Duration.h"
#include "folly/AtomicLinkedList.h"
#include "folly/container/F14Map.h"
#include "interface/gen-cpp2/common_types.h"
#include "storage/CommonUtils.h"
namespace nebula {
namespace storage {
using ErrorCode = ::nebula::cpp2::ErrorCode;
template <typename K, typename V>
using Map = folly::F14FastMap<K, V>;
template <typename K>
using Set = folly::F14FastSet<K>;

/**
 * @brief Context during IndexNode init
 *
 * For better extensibility, IndexNode::next() will return a row with fix format every time. For
 * example, a IndexNode will return a row with format "a,b,a+b" to its parent node. In this way,
 * each IndexNode in the execution plan can be completely decoupled. Each node only needs to know
 * two things: The first is the column of the row returned by its child nodes in the attribute or
 * expression calculation result it needs. The second is that it needs to know what content and the
 * corresponding location of the row it returns.
 *
 * But these contents are not known when IndexNode is constructed, so it needs an Init() process,
 * and in this process, the content required by the parent node is notified to the child node, and
 * the child node needs to inform the specific location of the parent node content.So we need
 * InitContext to pass these messages.
 *
 * @see IndexNode, IndexNode::init(), IndexNode::next()
 */
struct InitContext {
  /**
   * @brief column required by partent node
   * @todo The parent node may not only need the attributes of a certain column, but may also be the
   * calculation result of an expression. For example, ProjectionNode/SelectionNode may need the
   * calculation result of max(a)
   */
  Set<std::string> requiredColumns;
  /**
   * @brief The format of the row returned to the parent node
   * @todo the same as requiredColumns
   */
  std::vector<std::string> returnColumns;
  /**
   * @brief The index of name in `returncolumns`
   * @todo the same as requiredColumns
   */
  Map<std::string, size_t> retColMap;
  // The columns in statColumns
  // TODO(nivras) need refactor this, put statColumns in returnColumns
  Set<std::string> statColumns;
};

/**
 * @brief IndexNode is the base class for each node that makes up the plan tree.
 *
 * The functions of IndexNode is divided into three parts.
 *
 * First part is used to build node. This part contains two stages. First, the user needs to make
 * various derived classes and nd organize them into a plan tree(by `children_`).After that, the
 * root node of plan tree needs to call the init function and recursively call the init function of
 * all child nodes, `Initcontext` will pass parameters between nodes to determine the data format or
 * other information to be returned between nodes during execution.Note that `init` needs to be
 * executed before `copy`.
 *
 * Second part is used to access data. `execute()` initlializes some variables at the beginning of
 * each part.Then, `next()` iterates data. At the end, `end()` and `finish()` are called if
 * necessary.
 *
 * The third part contains some auxiliary functions for debugging.
 *
 * @see InitContext
 */

class IndexNode {
 public:
  /**
   * @class Result
   * @brief Iterate result of IndexNode::next()
   *
   * There are three options for Result:
   * - not succeeded. Some error occured during iterating.
   * - succeeded but there isn't any remain Row.
   * - succeeded and there is a Row.
   */
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
    /**
     * @return true if successful
     * @return false if not successful
     */
    inline bool success() {
      return code_ == ErrorCode::SUCCEEDED;
    }
    /**
     * @return true if successful and has data
     * @return false if not successful or doesn't have data
     */
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

  /**
   * @brief shallow copy a new IndexNode
   *
   * @attention children_ will be empty and must copy after init.Under normal circumstances, there
   * is no need to call this function.
   *
   * @param node
   */

  IndexNode(const IndexNode& node);

  /**
   * @brief Construct a new Index Node object
   *
   * @param context
   * @param name name of the node
   */
  IndexNode(RuntimeContext* context, const std::string& name);

  virtual ~IndexNode() = default;

  /**
   * @brief deep copy the node without children
   *
   * Every derived class should implement copy() by itself.
   *
   * @return std::unique_ptr<IndexNode>
   */
  virtual std::unique_ptr<IndexNode> copy() = 0;

  /**
   * @brief add a child node
   *
   * @param child
   */
  void addChild(std::unique_ptr<IndexNode> child) {
    children_.emplace_back(std::move(child));
  }

  /**
   * @brief return all children
   *
   * @return const std::vector<std::unique_ptr<IndexNode>>&
   */
  const std::vector<std::unique_ptr<IndexNode>>& children() {
    return children_;
  }

  /**
   * @brief Initialize execute plan tree.
   *
   * @param initCtx
   * @return ::nebula::cpp2::ErrorCode
   * @attention init() should be called before copy() and execute()
   * @see InitContext
   */
  virtual ::nebula::cpp2::ErrorCode init(InitContext& initCtx) {
    DCHECK_EQ(children_.size(), 1);
    return children_[0]->init(initCtx);
  }
  /**
   * @brief `execute()` is used to initialize some variables at the beginning of each part(e.g
   * dedup set, kv iterator, etc.
   *
   * To support add some hooks before execute() or after execute(), execute() contain three
   * functions: beforeExecute(),doExecute(),afterExecute(). And doExecute() is a pure virtual
   * function, derived class only need override doExecute() function.
   *
   * @param partId
   * @return nebula::cpp2::ErrorCode
   *
   * @note
   */
  inline nebula::cpp2::ErrorCode execute(PartitionID partId);

  /**
   * @brief `next()` is used to iterate data.
   *
   * Row format has been determined during `init()`. Batch processing and loop unrolling can be used
   * to optimize performance if necessary, but there are no serious performance problems at present.
   *
   * Like exeucte(), next() also contains beforeNext(), doNext(), afterNext()
   *
   * @note
   * @return Result
   */
  inline Result next();

  // inline nebula::cpp2::ErrorCode finish();
  // inline nebula::cpp2::ErrorCode end();

  /**
   * @brief return IndexNode name
   *
   * @return const std::string&
   */
  const std::string& name() {
    return name_;
  }
  void enableProfileDetail();

  /**
   * @brief return identify of node
   *
   * identity will contain node name and execute args. This is very useful for viewing the execution
   * plan.
   *
   * @return std::string
   */

  virtual std::string identify() = 0;
  /**
   * @brief  All the time spent by next()
   *
   * @return const time::Duration&
   */
  inline const time::Duration& duration();

 protected:
  virtual Result doNext() = 0;
  void beforeNext();
  void afterNext();
  virtual nebula::cpp2::ErrorCode doExecute(PartitionID partId);
  void beforeExecute();
  void afterExecute();

  /**
   * @brief runtime context of plan
   */
  RuntimeContext* context_;
  /**
   * @brief graph space id of plan
   */
  GraphSpaceID spaceId_;
  /**
   * @brief all children of the node
   */
  std::vector<std::unique_ptr<IndexNode>> children_;
  /**
   * @brief node name which should be set in derive node.
   */
  std::string name_;
  /**
   * @brief used to record execution time(exclude children node's time).
   */
  time::Duration duration_;
  /**
   * @brief whether record execution time or not.
   */
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
#endif
