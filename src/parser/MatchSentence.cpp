/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "parser/MatchSentence.h"

namespace nebula {

std::string MatchClause::toString() const {
  std::string buf;
  buf.reserve(256);

  if (isOptional()) {
    buf += "OPTIONAL ";
  }

  buf += "MATCH ";
  buf += pathList_->toString();
  if (where_ != nullptr) {
    buf += " ";
    buf += where_->toString();
  }
  return buf;
}

std::string UnwindClause::toString() const {
  std::string buf;
  buf.reserve(256);

  buf += "UNWIND ";
  buf += expr_->toString();
  buf += " AS ";
  buf += alias_;

  return buf;
}

std::string WithClause::toString() const {
  std::string buf;
  buf.reserve(256);

  buf += "WITH ";

  if (isDistinct()) {
    buf += "DISTINCT ";
  }

  buf += returnItems_->toString();

  if (orderFactors_ != nullptr) {
    buf += " ";
    buf += "ORDER BY ";
    buf += orderFactors_->toString();
  }

  if (skip_ != nullptr) {
    buf += " ";
    buf += "SKIP ";
    buf += skip_->toString();
  }

  if (limit_ != nullptr) {
    buf += " ";
    buf += "LIMIT ";
    buf += limit_->toString();
  }

  if (where_ != nullptr) {
    buf += " ";
    buf += where_->toString();
  }

  return buf;
}

std::string MatchReturnItems::toString() const {
  std::string buf;
  if (allNamedAliases_) {
    buf += '*';
  }
  if (columns_) {
    if (allNamedAliases_) {
      buf += ',';
    }
    buf += columns_->toString();
  }

  return buf;
}

std::string MatchReturn::toString() const {
  std::string buf;
  buf.reserve(64);

  buf += "RETURN ";

  if (isDistinct()) {
    buf += "DISTINCT ";
  }

  buf += returnItems_->toString();

  if (orderFactors_ != nullptr) {
    buf += " ";
    buf += "ORDER BY ";
    buf += orderFactors_->toString();
  }

  if (skip_ != nullptr) {
    buf += " ";
    buf += "SKIP ";
    buf += skip_->toString();
  }

  if (limit_ != nullptr) {
    buf += " ";
    buf += "LIMIT ";
    buf += limit_->toString();
  }

  return buf;
}

std::string MatchSentence::toString() const {
  std::string buf;
  buf.reserve(256);

  for (auto& clause : clauses_) {
    buf += clause->toString();
    buf += " ";
  }

  buf += return_->toString();

  return buf;
}

MatchPathList::MatchPathList(MatchPath* path) {
  pathList_.emplace_back(path);
}

void MatchPathList::add(MatchPath* path) {
  pathList_.emplace_back(path);
}

std::string MatchPathList::toString() const {
  std::string buf;
  buf.reserve(256);
  std::vector<std::string> pathList;
  std::transform(pathList_.begin(), pathList_.end(), std::back_inserter(pathList), [](auto& path) {
    return path->toString();
  });
  folly::join(",", pathList.begin(), pathList.end(), buf);
  return buf;
}

}  // namespace nebula
