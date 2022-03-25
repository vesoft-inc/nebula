/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef PARSER_MATCHSENTENCE_H_
#define PARSER_MATCHSENTENCE_H_

#include "common/expression/ContainerExpression.h"
#include "common/expression/SubscriptExpression.h"
#include "parser/MatchPath.h"
#include "parser/Sentence.h"
#include "parser/TraverseSentences.h"

namespace nebula {

class MatchPathList final {
 public:
  explicit MatchPathList(MatchPath* path);

  void add(MatchPath* path);

  size_t pathSize() const {
    return pathList_.size();
  }

  const MatchPath* path(size_t i) const {
    return pathList_[i].get();
  }

  std::string toString() const;

 private:
  std::vector<std::unique_ptr<MatchPath>> pathList_;
};

class MatchReturnItems final {
 public:
  explicit MatchReturnItems(bool allNamedAliases, YieldColumns* columns = nullptr)
      : allNamedAliases_(allNamedAliases), columns_(columns) {}

  bool allNamedAliases() const {
    return allNamedAliases_;
  }

  YieldColumns* columns() {
    return columns_.get();
  }

  const YieldColumns* columns() const {
    return columns_.get();
  }

  std::string toString() const;

 private:
  bool allNamedAliases_{false};  // `*` indicates include all existing variables
  std::unique_ptr<YieldColumns> columns_;
};

class MatchReturn final {
 public:
  MatchReturn(MatchReturnItems* returnItems = nullptr,
              OrderFactors* orderFactors = nullptr,
              Expression* skip = nullptr,
              Expression* limit = nullptr,
              bool distinct = false) {
    returnItems_.reset(returnItems);
    orderFactors_.reset(orderFactors);
    skip_ = skip;
    limit_ = limit;
    isDistinct_ = distinct;
  }

  MatchReturnItems* returnItems() {
    return returnItems_.get();
  }

  const MatchReturnItems* returnItems() const {
    return returnItems_.get();
  }

  bool isDistinct() const {
    return isDistinct_;
  }

  const Expression* skip() const {
    return skip_;
  }

  const Expression* limit() const {
    return limit_;
  }

  OrderFactors* orderFactors() {
    return orderFactors_.get();
  }

  const OrderFactors* orderFactors() const {
    return orderFactors_.get();
  }

  std::string toString() const;

 private:
  std::unique_ptr<MatchReturnItems> returnItems_;
  bool isDistinct_{false};
  std::unique_ptr<OrderFactors> orderFactors_;
  Expression* skip_{nullptr};
  Expression* limit_{nullptr};
};

class ReadingClause {
 public:
  enum class Kind : uint8_t {
    kMatch,
    kUnwind,
    kWith,
  };
  explicit ReadingClause(Kind kind) {
    kind_ = kind;
  }
  virtual ~ReadingClause() = default;

  auto kind() const {
    return kind_;
  }

  bool isMatch() const {
    return kind() == Kind::kMatch;
  }

  bool isUnwind() const {
    return kind() == Kind::kUnwind;
  }

  bool isWith() const {
    return kind() == Kind::kWith;
  }

  virtual std::string toString() const = 0;

 private:
  Kind kind_;
};

class MatchClause final : public ReadingClause {
 public:
  MatchClause(MatchPathList* pathList, WhereClause* where, bool optional)
      : ReadingClause(Kind::kMatch) {
    pathList_.reset(pathList);
    where_.reset(where);
    isOptional_ = optional;
  }

  MatchPathList* pathList() {
    return pathList_.get();
  }

  const MatchPathList* path() const {
    return pathList_.get();
  }

  WhereClause* where() {
    return where_.get();
  }

  const WhereClause* where() const {
    return where_.get();
  }

  bool isOptional() const {
    return isOptional_;
  }

  std::string toString() const override;

 private:
  bool isOptional_{false};
  std::unique_ptr<MatchPathList> pathList_;
  std::unique_ptr<WhereClause> where_;
};

class UnwindClause final : public ReadingClause {
 public:
  UnwindClause(Expression* expr, const std::string& alias) : ReadingClause(Kind::kUnwind) {
    expr_ = expr;
    alias_ = alias;
  }

  Expression* expr() {
    return expr_;
  }

  const Expression* expr() const {
    return expr_;
  }

  const std::string& alias() const {
    return alias_;
  }

  std::string toString() const override;

 private:
  Expression* expr_{nullptr};
  std::string alias_;
};

class WithClause final : public ReadingClause {
 public:
  explicit WithClause(MatchReturnItems* returnItems,
                      OrderFactors* orderFactors = nullptr,
                      Expression* skip = nullptr,
                      Expression* limit = nullptr,
                      WhereClause* where = nullptr,
                      bool distinct = false)
      : ReadingClause(Kind::kWith) {
    returnItems_.reset(returnItems);
    orderFactors_.reset(orderFactors);
    skip_ = skip;
    limit_ = limit;
    where_.reset(where);
    isDistinct_ = distinct;
  }

  MatchReturnItems* returnItems() {
    return returnItems_.get();
  }

  const MatchReturnItems* returnItems() const {
    return returnItems_.get();
  }

  OrderFactors* orderFactors() {
    return orderFactors_.get();
  }

  const OrderFactors* orderFactors() const {
    return orderFactors_.get();
  }

  Expression* skip() {
    return skip_;
  }

  const Expression* skip() const {
    return skip_;
  }

  Expression* limit() {
    return limit_;
  }

  const Expression* limit() const {
    return limit_;
  }

  WhereClause* where() {
    return where_.get();
  }

  const WhereClause* where() const {
    return where_.get();
  }

  bool isDistinct() const {
    return isDistinct_;
  }

  std::string toString() const override;

 private:
  std::unique_ptr<MatchReturnItems> returnItems_;
  std::unique_ptr<OrderFactors> orderFactors_;
  Expression* skip_{nullptr};
  Expression* limit_{nullptr};
  std::unique_ptr<WhereClause> where_;
  bool isDistinct_;
};

class MatchClauseList final {
 public:
  void add(ReadingClause* clause) {
    clauses_.emplace_back(clause);
  }

  void add(MatchClauseList* list) {
    DCHECK(list != nullptr);
    for (auto& clause : list->clauses_) {
      clauses_.emplace_back(std::move(clause));
    }
    delete list;
  }

  auto clauses() && {
    return std::move(clauses_);
  }

 private:
  std::vector<std::unique_ptr<ReadingClause>> clauses_;
};

class MatchSentence final : public Sentence {
 public:
  MatchSentence(MatchClauseList* clauses, MatchReturn* ret) : Sentence(Kind::kMatch) {
    clauses_ = std::move(*clauses).clauses();
    delete clauses;
    return_.reset(ret);
  }

  auto& clauses() {
    return clauses_;
  }

  const auto& clauses() const {
    return clauses_;
  }

  const MatchReturn* ret() const {
    return return_.get();
  }

  MatchReturn* ret() {
    return return_.get();
  }

  std::string toString() const override;

 private:
  std::vector<std::unique_ptr<ReadingClause>> clauses_;
  std::unique_ptr<MatchReturn> return_;
};

}  // namespace nebula

#endif  // PARSER_MATCHSENTENCE_H_
